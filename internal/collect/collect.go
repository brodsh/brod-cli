// Package collect turns read-only Kafka admin data into a rules.Snapshot.
//
// PILLAR (metadata only): every input here is topic/group/broker METADATA read
// via internal/kafka — never a message payload. PILLAR (euros, not metrics):
// throughput figures produced here are honestly labeled ("estimated" / "none")
// so the rules engine's euros carry the right basis downstream.
//
// This package does no I/O of its own: it calls read methods on the passed
// *kafka.Client (or any reader implementing the same surface) and maps the
// results. Keeping the mapping separate from the wire calls makes it unit
// testable against fixtures with no network (see collect_test.go).
package collect

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/brodsh/brod-cli/internal/kafka"
	"github.com/brodsh/brod-cli/internal/rules"
)

// reader is the read-only surface collect needs. *kafka.Client satisfies it;
// tests supply a fake. It is intentionally the metadata-only subset — no
// produce, no consume.
type reader interface {
	Brokers(ctx context.Context) ([]kafka.BrokerInfo, error)
	Topics(ctx context.Context) ([]kafka.TopicInfo, error)
	TopicConfigs(ctx context.Context, topics []string) (map[string]kafka.TopicConfig, error)
	LogDirSizes(ctx context.Context) (map[string]map[int32][]int64, error)
	StartOffsets(ctx context.Context) (map[string]map[int32]int64, error)
	EndOffsets(ctx context.Context) (map[string]map[int32]int64, error)
	Groups(ctx context.Context) ([]kafka.GroupInfo, error)
	CommittedOffsets(ctx context.Context, groups []string) (map[string]map[string]struct{}, error)
}

// Options control snapshot construction.
type Options struct {
	// ClusterName labels the snapshot; falls back to the first bootstrap host.
	ClusterName string
	// Provider is "self" | "confluent" | "msk"; drives cost presets in rules.
	Provider string
	// Now is injected (no clock reads inside the mapping) for deterministic
	// snapshots and to match the rules engine's purity contract.
	Now time.Time
	// SampleWindow > 0 enables two-probe throughput estimation (A-3); 0 disables
	// it (single-shot, ThroughputSource="none").
	SampleWindow time.Duration
	// IncludeInternal keeps Kafka's internal topics (__consumer_offsets, etc.)
	// in the snapshot. Off by default — they're noise for cost attribution.
	IncludeInternal bool
}

// sleepFunc lets tests drive the sample-window wait without real time.
type sleepFunc func(ctx context.Context, d time.Duration) error

// Build assembles a rules.Snapshot from the cluster's metadata. With
// opts.SampleWindow > 0 it sleeps that long between two END-offset probes to
// estimate throughput; otherwise it returns immediately with no throughput.
func Build(ctx context.Context, c *kafka.Client, opts Options) (rules.Snapshot, error) {
	return build(ctx, c, opts, ctxSleep)
}

// build is the testable core (reader interface + injectable sleep).
func build(ctx context.Context, r reader, opts Options, sleep sleepFunc) (rules.Snapshot, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	brokers, err := r.Brokers(ctx)
	if err != nil {
		return rules.Snapshot{}, err
	}
	topics, err := r.Topics(ctx)
	if err != nil {
		return rules.Snapshot{}, err
	}
	if !opts.IncludeInternal {
		topics = dropInternal(topics)
	}

	names := topicNames(topics)
	configs, err := r.TopicConfigs(ctx, names)
	if err != nil {
		return rules.Snapshot{}, err
	}
	logDirs, err := r.LogDirSizes(ctx)
	if err != nil {
		return rules.Snapshot{}, err
	}
	startOff, err := r.StartOffsets(ctx)
	if err != nil {
		return rules.Snapshot{}, err
	}
	endOff, err := r.EndOffsets(ctx)
	if err != nil {
		return rules.Snapshot{}, err
	}
	groups, err := r.Groups(ctx)
	if err != nil {
		return rules.Snapshot{}, err
	}

	groupIDs := make([]string, 0, len(groups))
	for _, g := range groups {
		groupIDs = append(groupIDs, g.GroupID)
	}
	committed, err := r.CommittedOffsets(ctx, groupIDs)
	if err != nil {
		return rules.Snapshot{}, err
	}

	// ActiveConsumers per topic = count of distinct non-empty/dead groups that
	// have committed offsets on the topic. (A-2)
	activeByTopic := activeConsumers(groups, committed)

	// Throughput estimation (A-3): take a second END-offset probe after the
	// window, then estimate bytes/sec per topic from the offset delta.
	var endOff2 map[string]map[int32]int64
	var windowSecs float64
	if opts.SampleWindow > 0 {
		if err := sleep(ctx, opts.SampleWindow); err != nil {
			return rules.Snapshot{}, err
		}
		endOff2, err = r.EndOffsets(ctx)
		if err != nil {
			return rules.Snapshot{}, err
		}
		windowSecs = opts.SampleWindow.Seconds()
	}

	snap := rules.Snapshot{
		TakenAt: now,
		Cluster: rules.ClusterInfo{
			Name:     clusterName(opts),
			Provider: providerOrSelf(opts.Provider),
		},
		Brokers: mapBrokers(brokers, logDirs),
		Groups:  mapGroups(groups),
	}

	for _, t := range topics {
		cfg := configs[t.Name]
		retained := sumReplicaBytes(logDirs[t.Name])
		earliest, latest := offsetSpan(t, startOff[t.Name], endOff[t.Name])

		topic := rules.Topic{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			RetentionMs:       retentionMs(cfg.RetentionMs),
			CompressionType:   strOr(cfg.CompressionType, "producer"),
			CleanupPolicy:     strOr(cfg.CleanupPolicy, "delete"),
			RetainedBytes:     retained,
			PartitionBytes:    perPartitionBytes(logDirs[t.Name]),
			EarliestOffset:    earliest,
			LatestOffset:      latest,
			// lag-TIME needs offset->timestamp history a single snapshot can't
			// provide; -1 makes the history-dependent rule (R3) skip correctly.
			MaxConsumerLagMs: -1,
			ActiveConsumers:  activeByTopic[t.Name],
			ThroughputSource: "none",
		}

		if opts.SampleWindow > 0 {
			in := estimateBytesInPerSec(retained, startOff[t.Name], endOff[t.Name], endOff2[t.Name], windowSecs)
			topic.BytesInPerSec = in
			// BytesOutPerSec needs broker/CloudWatch metrics (out of CLI
			// scope); leave 0 and label the topic's source honestly.
			topic.BytesOutPerSec = 0
			topic.ThroughputSource = "estimated"
		}

		snap.Topics = append(snap.Topics, topic)
	}
	sort.Slice(snap.Topics, func(i, j int) bool { return snap.Topics[i].Name < snap.Topics[j].Name })

	return snap, nil
}

// ctxSleep waits d or returns ctx.Err() if cancelled first.
func ctxSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// --- mapping helpers (pure) ---

func dropInternal(ts []kafka.TopicInfo) []kafka.TopicInfo {
	out := ts[:0:0]
	for _, t := range ts {
		// Internal topics either report IsInternal or use the __ prefix
		// convention (Redpanda/older brokers don't always set the flag).
		if t.IsInternal || strings.HasPrefix(t.Name, "__") {
			continue
		}
		out = append(out, t)
	}
	return out
}

func topicNames(ts []kafka.TopicInfo) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.Name
	}
	return out
}

// sumReplicaBytes sums on-disk sizes across ALL replicas of ALL partitions of a
// topic. This is the true on-disk footprint (replication included) — what
// storage actually costs and what R1/R3/R6 savings act on. (A-2)
func sumReplicaBytes(parts map[int32][]int64) int64 {
	var total int64
	for _, replicas := range parts {
		for _, b := range replicas {
			total += b
		}
	}
	return total
}

// perPartitionBytes returns each partition's on-disk size (summed across its
// replicas), ordered by partition id. Summing replicas scales every partition by
// the same replication factor, so the distribution — which is all R7 (skew)
// cares about — is preserved. Returns nil when no log-dir data is available.
func perPartitionBytes(parts map[int32][]int64) []int64 {
	if len(parts) == 0 {
		return nil
	}
	pids := make([]int32, 0, len(parts))
	for pid := range parts {
		pids = append(pids, pid)
	}
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
	out := make([]int64, 0, len(pids))
	for _, pid := range pids {
		var sum int64
		for _, b := range parts[pid] {
			sum += b
		}
		out = append(out, sum)
	}
	return out
}

// offsetSpan returns min start across partitions and max end across partitions.
// Missing offset data falls back to 0.
func offsetSpan(t kafka.TopicInfo, start, end map[int32]int64) (earliest, latest int64) {
	earliest, latest = int64(0), int64(0)
	first := true
	for pid := range t.Replicas {
		if s, ok := start[pid]; ok {
			if first || s < earliest {
				earliest = s
			}
		}
		if e, ok := end[pid]; ok {
			if e > latest {
				latest = e
			}
		}
		first = false
	}
	return earliest, latest
}

// activeConsumers builds topic -> count of distinct live groups (non
// Empty/Dead) that have committed offsets on the topic.
func activeConsumers(groups []kafka.GroupInfo, committed map[string]map[string]struct{}) map[string]int {
	out := map[string]int{}
	for _, g := range groups {
		if isInactiveState(g.State) {
			continue
		}
		for topic := range committed[g.GroupID] {
			out[topic]++
		}
	}
	return out
}

// isInactiveState reports whether a group state means "not actively consuming".
func isInactiveState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "empty", "dead", "":
		return true
	default:
		return false
	}
}

func mapBrokers(brokers []kafka.BrokerInfo, logDirs map[string]map[int32][]int64) []rules.Broker {
	// Total on-disk bytes per broker isn't exposed per-broker in our log-dir
	// map (it's keyed by topic/partition with replica lists, not broker id), so
	// LogDirBytes is left 0 here — broker-level disk attribution is a SaaS
	// concern. We still record the broker inventory.
	out := make([]rules.Broker, 0, len(brokers))
	for _, b := range brokers {
		out = append(out, rules.Broker{ID: int(b.ID)})
	}
	return out
}

func mapGroups(groups []kafka.GroupInfo) []rules.ConsumerGrp {
	out := make([]rules.ConsumerGrp, 0, len(groups))
	for _, g := range groups {
		out = append(out, rules.ConsumerGrp{
			GroupID: g.GroupID,
			State:   g.State,
			// LastCommitAgeDays needs commit-timestamp history; unknown from a
			// single snapshot. -1 documents "unknown" for the rules.
			LastCommitAgeDays: -1,
		})
	}
	return out
}

// estimateBytesInPerSec approximates ingress as
//
//	(Σ Δend_offset / window_secs) × avg_record_bytes
//
// where avg_record_bytes = RetainedBytes / Σ(end-start). This is the
// ARCHITECTURE "offset-delta × avg record size" approximation; the caller labels
// the result ThroughputSource="estimated". Returns 0 when it can't be computed
// (no records, no delta) rather than guessing. (A-3)
func estimateBytesInPerSec(retained int64, start, end0, end1 map[int32]int64, windowSecs float64) float64 {
	if windowSecs <= 0 {
		return 0
	}
	var totalRecords int64 // Σ(end-start) across partitions, for avg record size
	var deltaRecords int64 // Σ(end1-end0) across partitions, produced in window
	for pid, e0 := range end0 {
		s := start[pid]
		if e0 > s {
			totalRecords += e0 - s
		}
		if e1, ok := end1[pid]; ok && e1 > e0 {
			deltaRecords += e1 - e0
		}
	}
	if totalRecords <= 0 || deltaRecords <= 0 || retained <= 0 {
		return 0
	}
	avgRecordBytes := float64(retained) / float64(totalRecords)
	return (float64(deltaRecords) / windowSecs) * avgRecordBytes
}

func retentionMs(v *string) int64 {
	if v == nil {
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(*v), 10, 64)
	if err != nil {
		return -1
	}
	return n // -1 here means infinite, matching the snapshot schema.
}

func strOr(v *string, def string) string {
	if v == nil || *v == "" {
		return def
	}
	return *v
}

func clusterName(opts Options) string {
	if opts.ClusterName != "" {
		return opts.ClusterName
	}
	return "kafka-cluster"
}

func providerOrSelf(p string) string {
	if p == "" {
		return "self"
	}
	return p
}
