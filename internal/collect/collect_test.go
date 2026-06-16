package collect

import (
	"context"
	"testing"
	"time"

	"github.com/brodsh/brod-cli/internal/kafka"
)

// fakeReader is an in-memory implementation of the reader interface so the
// snapshot builder can be tested with no network. endOffsetCalls lets tests
// return a different second probe for throughput estimation.
type fakeReader struct {
	brokers   []kafka.BrokerInfo
	topics    []kafka.TopicInfo
	configs   map[string]kafka.TopicConfig
	logDirs   map[string]map[int32][]int64
	startOff  map[string]map[int32]int64
	endOff    []map[string]map[int32]int64 // successive END-offset probes
	groups    []kafka.GroupInfo
	committed map[string]map[string]struct{}

	endCalls int
}

func (f *fakeReader) Brokers(context.Context) ([]kafka.BrokerInfo, error) { return f.brokers, nil }
func (f *fakeReader) Topics(context.Context) ([]kafka.TopicInfo, error)   { return f.topics, nil }
func (f *fakeReader) TopicConfigs(_ context.Context, _ []string) (map[string]kafka.TopicConfig, error) {
	return f.configs, nil
}
func (f *fakeReader) LogDirSizes(context.Context) (map[string]map[int32][]int64, error) {
	return f.logDirs, nil
}
func (f *fakeReader) StartOffsets(context.Context) (map[string]map[int32]int64, error) {
	return f.startOff, nil
}
func (f *fakeReader) EndOffsets(context.Context) (map[string]map[int32]int64, error) {
	i := f.endCalls
	if i >= len(f.endOff) {
		i = len(f.endOff) - 1
	}
	f.endCalls++
	return f.endOff[i], nil
}
func (f *fakeReader) Groups(context.Context) ([]kafka.GroupInfo, error) { return f.groups, nil }
func (f *fakeReader) CommittedOffsets(_ context.Context, _ []string) (map[string]map[string]struct{}, error) {
	return f.committed, nil
}

func strp(s string) *string { return &s }

// noSleep skips the sample-window wait in tests.
func noSleep(context.Context, time.Duration) error { return nil }

// baseReader returns a small two-topic cluster fixture.
func baseReader() *fakeReader {
	return &fakeReader{
		brokers: []kafka.BrokerInfo{{ID: 1}, {ID: 2}, {ID: 3}},
		topics: []kafka.TopicInfo{
			{
				Name:              "events.raw",
				Partitions:        2,
				ReplicationFactor: 3,
				Replicas:          map[int32][]int32{0: {1, 2, 3}, 1: {2, 3, 1}},
			},
			{
				Name:              "__consumer_offsets",
				Partitions:        1,
				ReplicationFactor: 3,
				IsInternal:        true,
				Replicas:          map[int32][]int32{0: {1, 2, 3}},
			},
		},
		configs: map[string]kafka.TopicConfig{
			"events.raw": {
				RetentionMs:     strp("604800000"),
				CompressionType: strp("zstd"),
				CleanupPolicy:   strp("delete"),
			},
		},
		// Two partitions, three replicas each, 10 bytes apiece => 60 total.
		logDirs: map[string]map[int32][]int64{
			"events.raw": {0: {10, 10, 10}, 1: {10, 10, 10}},
		},
		startOff: map[string]map[int32]int64{
			"events.raw": {0: 0, 1: 0},
		},
		endOff: []map[string]map[int32]int64{
			{"events.raw": {0: 100, 1: 100}}, // probe 1
		},
		groups: []kafka.GroupInfo{
			{GroupID: "live-consumer", State: "Stable"},
			{GroupID: "dead-consumer", State: "Empty"},
		},
		committed: map[string]map[string]struct{}{
			"live-consumer": {"events.raw": {}},
			"dead-consumer": {"events.raw": {}},
		},
	}
}

func TestBuild_MapsMetadata(t *testing.T) {
	r := baseReader()
	snap, err := build(context.Background(), r, Options{
		ClusterName: "test-cluster",
		Provider:    "msk",
		Now:         time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
	}, noSleep)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if snap.Cluster.Name != "test-cluster" || snap.Cluster.Provider != "msk" {
		t.Errorf("cluster = %+v, want test-cluster/msk", snap.Cluster)
	}
	if got := snap.TakenAt.Year(); got != 2026 {
		t.Errorf("TakenAt year = %d", got)
	}
	// Internal topic dropped by default.
	if len(snap.Topics) != 1 {
		t.Fatalf("topics = %d, want 1 (internal dropped)", len(snap.Topics))
	}
	top := snap.Topics[0]
	if top.Name != "events.raw" {
		t.Fatalf("topic name = %q", top.Name)
	}
	if top.Partitions != 2 || top.ReplicationFactor != 3 {
		t.Errorf("partitions/rf = %d/%d, want 2/3", top.Partitions, top.ReplicationFactor)
	}
	// RetainedBytes = sum across ALL replicas = 6 replicas * 10 = 60.
	if top.RetainedBytes != 60 {
		t.Errorf("RetainedBytes = %d, want 60 (sum across all replicas)", top.RetainedBytes)
	}
	if top.RetentionMs != 604800000 {
		t.Errorf("RetentionMs = %d", top.RetentionMs)
	}
	if top.CompressionType != "zstd" || top.CleanupPolicy != "delete" {
		t.Errorf("compression/cleanup = %q/%q", top.CompressionType, top.CleanupPolicy)
	}
	if top.EarliestOffset != 0 || top.LatestOffset != 100 {
		t.Errorf("offset span = %d..%d, want 0..100", top.EarliestOffset, top.LatestOffset)
	}
	// -1 documents "unknown" — a single snapshot can't compute lag-time.
	if top.MaxConsumerLagMs != -1 {
		t.Errorf("MaxConsumerLagMs = %d, want -1", top.MaxConsumerLagMs)
	}
	// Only the live (Stable) group counts; the Empty one is excluded.
	if top.ActiveConsumers != 1 {
		t.Errorf("ActiveConsumers = %d, want 1 (Empty group excluded)", top.ActiveConsumers)
	}
	// Single-shot: no throughput, honestly labeled.
	if top.ThroughputSource != "none" || top.BytesInPerSec != 0 {
		t.Errorf("throughput = %v/%q, want 0/none", top.BytesInPerSec, top.ThroughputSource)
	}
	if len(snap.Brokers) != 3 {
		t.Errorf("brokers = %d, want 3", len(snap.Brokers))
	}
	for _, g := range snap.Groups {
		if g.LastCommitAgeDays != -1 {
			t.Errorf("group %s LastCommitAgeDays = %d, want -1", g.GroupID, g.LastCommitAgeDays)
		}
	}
}

func TestBuild_IncludeInternal(t *testing.T) {
	r := baseReader()
	snap, err := build(context.Background(), r, Options{IncludeInternal: true}, noSleep)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Topics) != 2 {
		t.Errorf("topics = %d, want 2 with IncludeInternal", len(snap.Topics))
	}
}

func TestBuild_ThroughputEstimated(t *testing.T) {
	r := baseReader()
	// Second probe: +50 records on each of 2 partitions over a 10s window.
	r.endOff = append(r.endOff, map[string]map[int32]int64{
		"events.raw": {0: 150, 1: 150},
	})
	snap, err := build(context.Background(), r, Options{SampleWindow: 10 * time.Second}, noSleep)
	if err != nil {
		t.Fatal(err)
	}
	top := snap.Topics[0]
	if top.ThroughputSource != "estimated" {
		t.Fatalf("ThroughputSource = %q, want estimated", top.ThroughputSource)
	}
	// avg_record_bytes = retained(60) / totalRecords(end-start = 200) = 0.3
	// deltaRecords = 100 over 10s => 10 rec/s * 0.3 = 3.0 bytes/s
	if got := top.BytesInPerSec; got < 2.99 || got > 3.01 {
		t.Errorf("BytesInPerSec = %v, want ~3.0", got)
	}
	// Egress needs broker metrics; stays 0.
	if top.BytesOutPerSec != 0 {
		t.Errorf("BytesOutPerSec = %v, want 0 (out of CLI scope)", top.BytesOutPerSec)
	}
}

func TestBuild_NoThroughputWhenNoDelta(t *testing.T) {
	r := baseReader()
	// Second probe identical to first => no produced records => 0, not a guess.
	r.endOff = append(r.endOff, map[string]map[int32]int64{
		"events.raw": {0: 100, 1: 100},
	})
	snap, err := build(context.Background(), r, Options{SampleWindow: 10 * time.Second}, noSleep)
	if err != nil {
		t.Fatal(err)
	}
	if got := snap.Topics[0].BytesInPerSec; got != 0 {
		t.Errorf("BytesInPerSec = %v, want 0 (no delta)", got)
	}
}

func TestBuild_InfiniteRetentionAndDefaults(t *testing.T) {
	r := baseReader()
	r.configs = map[string]kafka.TopicConfig{
		"events.raw": {RetentionMs: strp("-1")}, // infinite; compression/cleanup absent
	}
	snap, err := build(context.Background(), r, Options{}, noSleep)
	if err != nil {
		t.Fatal(err)
	}
	top := snap.Topics[0]
	if top.RetentionMs != -1 {
		t.Errorf("RetentionMs = %d, want -1 (infinite)", top.RetentionMs)
	}
	// Defaults applied when configs absent.
	if top.CompressionType != "producer" || top.CleanupPolicy != "delete" {
		t.Errorf("defaults = %q/%q, want producer/delete", top.CompressionType, top.CleanupPolicy)
	}
	if snap.Cluster.Provider != "self" {
		t.Errorf("provider default = %q, want self", snap.Cluster.Provider)
	}
}

func TestEstimateBytesInPerSec_Guards(t *testing.T) {
	// Zero window, zero retained, no delta => all return 0 (never NaN/guess).
	if v := estimateBytesInPerSec(0, nil, nil, nil, 0); v != 0 {
		t.Errorf("zero inputs = %v, want 0", v)
	}
	if v := estimateBytesInPerSec(100, map[int32]int64{0: 0}, map[int32]int64{0: 10}, map[int32]int64{0: 10}, 5); v != 0 {
		t.Errorf("no delta = %v, want 0", v)
	}
}
