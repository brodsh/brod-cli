package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
)

// This file holds the ENTIRE read surface of the kafka package. Every method
// here issues only Admin / Describe / Metadata / ListOffsets / OffsetFetch
// requests via kadm. There is deliberately no Produce and no Consume method —
// brod cannot, by construction, read message payloads. (guard_test.go enforces
// that no record-fetch/poll/consumer entrypoint exists anywhere.)

// BrokerInfo is one broker's identity.
type BrokerInfo struct {
	ID   int32
	Host string
	Port int32
	Rack string
}

// TopicInfo is per-topic metadata (no payloads): partition count, replication
// factor, and the per-partition replica layout used to attribute on-disk bytes.
type TopicInfo struct {
	Name              string
	IsInternal        bool
	Partitions        int
	ReplicationFactor int
	// Replicas maps partition -> replica broker IDs (for log-dir attribution).
	Replicas map[int32][]int32
}

// TopicConfig is the subset of topic configs Brod's rules care about. A nil
// pointer means the config key was absent.
type TopicConfig struct {
	RetentionMs     *string
	CompressionType *string
	CleanupPolicy   *string
	MinInsyncReplicas *string
	SegmentBytes    *string
	SegmentMs       *string
}

// GroupInfo is consumer-group metadata: id + state. (No member payloads needed.)
type GroupInfo struct {
	GroupID string
	State   string
}

// Brokers returns the cluster's brokers via a metadata read.
func (c *Client) Brokers(ctx context.Context) ([]BrokerInfo, error) {
	md, err := c.adm.BrokerMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: broker metadata: %w", err)
	}
	out := make([]BrokerInfo, 0, len(md.Brokers))
	for _, b := range md.Brokers {
		bi := BrokerInfo{ID: b.NodeID, Host: b.Host, Port: b.Port}
		if b.Rack != nil {
			bi.Rack = *b.Rack
		}
		out = append(out, bi)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Topics returns topic metadata. Internal topics are included (callers decide
// whether to keep them); replication factor is taken from partition 0's replica
// count, which is the conventional definition.
func (c *Client) Topics(ctx context.Context) ([]TopicInfo, error) {
	td, err := c.adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list topics: %w", err)
	}
	out := make([]TopicInfo, 0, len(td))
	for name, d := range td {
		if d.Err != nil {
			return nil, fmt.Errorf("kafka: topic %q metadata: %w", name, d.Err)
		}
		ti := TopicInfo{
			Name:       name,
			IsInternal: d.IsInternal,
			Partitions: len(d.Partitions),
			Replicas:   make(map[int32][]int32, len(d.Partitions)),
		}
		maxRF := 0
		for pid, p := range d.Partitions {
			rep := append([]int32(nil), p.Replicas...)
			ti.Replicas[pid] = rep
			if len(rep) > maxRF {
				maxRF = len(rep)
			}
		}
		ti.ReplicationFactor = maxRF
		out = append(out, ti)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// TopicConfigs describes the configs for the given topics, projecting the keys
// Brod's rules use. Topics with no name are ignored.
func (c *Client) TopicConfigs(ctx context.Context, topics []string) (map[string]TopicConfig, error) {
	if len(topics) == 0 {
		return map[string]TopicConfig{}, nil
	}
	rcs, err := c.adm.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe topic configs: %w", err)
	}
	out := make(map[string]TopicConfig, len(rcs))
	for _, rc := range rcs {
		if rc.Err != nil {
			return nil, fmt.Errorf("kafka: topic %q configs: %w", rc.Name, rc.Err)
		}
		tc := TopicConfig{}
		for _, cfg := range rc.Configs {
			switch cfg.Key {
			case "retention.ms":
				tc.RetentionMs = cfg.Value
			case "compression.type":
				tc.CompressionType = cfg.Value
			case "cleanup.policy":
				tc.CleanupPolicy = cfg.Value
			case "min.insync.replicas":
				tc.MinInsyncReplicas = cfg.Value
			case "segment.bytes":
				tc.SegmentBytes = cfg.Value
			case "segment.ms":
				tc.SegmentMs = cfg.Value
			}
		}
		out[rc.Name] = tc
	}
	return out, nil
}

// LogDirSizes returns the on-disk size of every (topic, partition) replica
// across ALL brokers, as a nested map topic -> partition -> []bytes (one entry
// per replica that reported a size). collect sums these to get the true on-disk
// footprint (replication included). Errors on individual brokers/dirs are
// skipped — a partial picture is still useful and brod never fails closed on a
// read.
func (c *Client) LogDirSizes(ctx context.Context) (map[string]map[int32][]int64, error) {
	// Empty TopicsSet => describe all log dirs on all brokers.
	dirs, err := c.adm.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("kafka: describe log dirs: %w", err)
	}
	out := map[string]map[int32][]int64{}
	// DescribedAllLogDirs is broker -> dirs; EachPartition flattens a broker's
	// dirs into per-partition entries. Iterating all brokers therefore yields
	// one entry per replica, which is exactly what we sum downstream.
	for _, brokerDirs := range dirs {
		brokerDirs.EachPartition(func(p kadm.DescribedLogDirPartition) {
			// Skip "future" replicas (mid-reassignment shadows) — they would
			// double-count bytes against the active replica.
			if p.IsFuture {
				return
			}
			tp := out[p.Topic]
			if tp == nil {
				tp = map[int32][]int64{}
				out[p.Topic] = tp
			}
			tp[p.Partition] = append(tp[p.Partition], p.Size)
		})
	}
	return out, nil
}

// StartOffsets returns the earliest (oldest) offset per topic-partition.
func (c *Client) StartOffsets(ctx context.Context) (map[string]map[int32]int64, error) {
	lo, err := c.adm.ListStartOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list start offsets: %w", err)
	}
	return flattenOffsets(lo), nil
}

// EndOffsets returns the latest (newest) offset per topic-partition. The
// two-probe throughput estimator (A-3) calls this twice.
func (c *Client) EndOffsets(ctx context.Context) (map[string]map[int32]int64, error) {
	lo, err := c.adm.ListEndOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list end offsets: %w", err)
	}
	return flattenOffsets(lo), nil
}

func flattenOffsets(lo kadm.ListedOffsets) map[string]map[int32]int64 {
	out := make(map[string]map[int32]int64, len(lo))
	for topic, parts := range lo {
		tp := make(map[int32]int64, len(parts))
		for pid, o := range parts {
			if o.Err != nil {
				continue
			}
			tp[pid] = o.Offset
		}
		out[topic] = tp
	}
	return out
}

// Groups returns all consumer groups with their state. (DescribeGroups would
// add members/assignments; ListGroups is enough for our metadata needs and is
// cheaper.)
func (c *Client) Groups(ctx context.Context) ([]GroupInfo, error) {
	lg, err := c.adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: list groups: %w", err)
	}
	out := make([]GroupInfo, 0, len(lg))
	for _, g := range lg {
		out = append(out, GroupInfo{GroupID: g.Group, State: g.State})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out, nil
}

// CommittedOffsets fetches, for each group, the topics it has committed offsets
// on. The returned map is group -> set of topic names. This is committed-offset
// METADATA (which partitions a group tracks) — not message data — and is what
// distinguishes a topic with live consumers from a dead one.
func (c *Client) CommittedOffsets(ctx context.Context, groups []string) (map[string]map[string]struct{}, error) {
	out := make(map[string]map[string]struct{}, len(groups))
	for _, g := range groups {
		resp, err := c.adm.FetchOffsets(ctx, g)
		if err != nil {
			// A group can vanish between listing and fetching; tolerate it.
			continue
		}
		topics := map[string]struct{}{}
		resp.Each(func(o kadm.OffsetResponse) {
			if o.Err != nil {
				return
			}
			topics[o.Topic] = struct{}{}
		})
		out[g] = topics
	}
	return out, nil
}
