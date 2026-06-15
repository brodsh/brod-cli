package rules

import (
	"testing"
	"time"

	"github.com/brodsh/brod-cli/internal/cost"
)

// fixedNow is the injected evaluation time for all tests (purity: no clock).
var fixedNow = time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

// demoFixture builds a single-snapshot history that triggers R1, R3, R4, R5, R6
// at least once each, with an explicit storage price so euros are exact.
func demoFixture() (SnapshotHistory, cost.CostModel, RuleConfig) {
	snap := Snapshot{
		TakenAt: fixedNow,
		Cluster: ClusterInfo{Name: "prod", Provider: "self"},
		Topics: []Topic{
			{ // R5: 2 MB/s uncompressed
				Name: "events.raw", Partitions: 6, ReplicationFactor: 3,
				RetentionMs: 7 * dayMs, CompressionType: "none",
				RetainedBytes: 40 << 30, BytesInPerSec: 2 << 20,
				ThroughputSource: "metrics", MaxConsumerLagMs: -1, ActiveConsumers: 2,
			},
			{ // R1: dead — no traffic, no consumers, 20 GB
				Name: "legacy.dump", Partitions: 3, ReplicationFactor: 3,
				RetentionMs: -1, CompressionType: "zstd",
				RetainedBytes: 20 << 30, BytesInPerSec: 0,
				ThroughputSource: "metrics", MaxConsumerLagMs: -1, ActiveConsumers: 0,
			},
			{ // R6: non-prod RF=3
				Name: "dev.orders", Partitions: 3, ReplicationFactor: 3,
				RetentionMs: dayMs, CompressionType: "zstd",
				RetainedBytes: 10 << 30, BytesInPerSec: 1000,
				ThroughputSource: "metrics", MaxConsumerLagMs: -1, ActiveConsumers: 1,
			},
			{ // R4: over-partitioned, low per-partition throughput
				Name: "metrics.fine", Partitions: 48, ReplicationFactor: 3,
				RetentionMs: 3 * dayMs, CompressionType: "zstd",
				RetainedBytes: 8 << 30, BytesInPerSec: 10 << 10, // ~213 B/s per partition
				ThroughputSource: "metrics", MaxConsumerLagMs: -1, ActiveConsumers: 1,
			},
			{ // R3: huge retention vs tiny lag horizon, known lag; low ingest so
				// projected-at-proposed is far below current retained → real saving
				Name: "clicks", Partitions: 6, ReplicationFactor: 3,
				RetentionMs: 30 * dayMs, CompressionType: "zstd",
				RetainedBytes: 30 << 30, BytesInPerSec: 2 << 10, // 2 KB/s
				ThroughputSource: "metrics", MaxConsumerLagMs: int64(60 * 60 * 1000), // 1h
				ActiveConsumers: 3,
			},
		},
	}
	cm := cost.CostModel{
		Provider:          "self",
		StorageEURPerGBMo: 0.10, // explicit → exact euros
		NetworkEURPerGB:   0.01,
		Weights:           cost.DefaultWeights(),
	}
	return SnapshotHistory{snap}, cm, DefaultConfig(fixedNow)
}

func findByRule(fs []Finding, id string) []Finding {
	var out []Finding
	for _, f := range fs {
		if f.RuleID == id {
			out = append(out, f)
		}
	}
	return out
}

func TestAllDemoRulesFire(t *testing.T) {
	h, cm, cfg := demoFixture()
	fs := Evaluate(h, cm, cfg)
	for _, id := range []string{"R1", "R3", "R4", "R5", "R6"} {
		if len(findByRule(fs, id)) == 0 {
			t.Errorf("expected rule %s to fire on the demo fixture", id)
		}
	}
}

func TestFindingsSortedBySavingDesc(t *testing.T) {
	h, cm, cfg := demoFixture()
	fs := Evaluate(h, cm, cfg)
	for i := 1; i < len(fs); i++ {
		if fs[i-1].EstSavingEUR < fs[i].EstSavingEUR {
			t.Fatalf("findings not sorted by saving desc at %d: %.2f then %.2f", i, fs[i-1].EstSavingEUR, fs[i].EstSavingEUR)
		}
	}
}

func TestR5Math(t *testing.T) {
	h, cm, cfg := demoFixture()
	f := findByRule(Evaluate(h, cm, cfg), "R5")[0]
	// 2 MB/s over a 30d month, × 0.65, × (0.10 + 0.01) €/GB.
	inGB := cost.MonthlyGB(2 << 20)
	want := round2(inGB * 0.65 * 0.11)
	if f.EstSavingEUR != want {
		t.Fatalf("R5 saving = %.2f, want %.2f", f.EstSavingEUR, want)
	}
	if f.Confidence != ConfidenceLow {
		t.Fatalf("R5 confidence = %s, want low (assumption)", f.Confidence)
	}
}

func TestR6Math(t *testing.T) {
	h, cm, cfg := demoFixture()
	f := findByRule(Evaluate(h, cm, cfg), "R6")[0]
	// retained × (3-2)/3 × 0.10 (retained in decimal GB).
	want := round2(gb(10<<30) * (1.0 / 3.0) * 0.10)
	if f.EstSavingEUR != want {
		t.Fatalf("R6 saving = %.2f, want %.2f", f.EstSavingEUR, want)
	}
}

func TestR4IsPlanNeverOneClick(t *testing.T) {
	h, cm, cfg := demoFixture()
	f := findByRule(Evaluate(h, cm, cfg), "R4")[0]
	if f.Severity != SeverityPlan {
		t.Fatalf("R4 severity = %s, want plan", f.Severity)
	}
	if f.Remediation.OneClick {
		t.Fatal("R4 must never be a one-click PR (partition shrink needs migration)")
	}
}

func TestR3SkippedWhenLagUnknown(t *testing.T) {
	h, cm, cfg := demoFixture()
	snap := h[0]
	for i := range snap.Topics {
		snap.Topics[i].MaxConsumerLagMs = -1 // unknown everywhere
	}
	fs := Evaluate(SnapshotHistory{snap}, cm, cfg)
	if len(findByRule(fs, "R3")) != 0 {
		t.Fatal("R3 must skip topics with unknown lag-time (history-dependent)")
	}
}

func TestDisabledRuleIsSkipped(t *testing.T) {
	h, cm, cfg := demoFixture()
	cfg.Disabled["R5"] = true
	if len(findByRule(Evaluate(h, cm, cfg), "R5")) != 0 {
		t.Fatal("disabled R5 still produced findings")
	}
}

func TestEmptyHistoryYieldsNoFindings(t *testing.T) {
	_, cm, cfg := demoFixture()
	if fs := Evaluate(SnapshotHistory{}, cm, cfg); fs != nil {
		t.Fatalf("empty history should yield nil, got %d findings", len(fs))
	}
}
