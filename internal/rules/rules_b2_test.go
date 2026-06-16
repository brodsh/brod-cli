package rules

import (
	"testing"

	"github.com/brodsh/brod-cli/internal/cost"
)

func priceCfg() (cost.CostModel, RuleConfig) {
	cm := cost.CostModel{Provider: "self", StorageEURPerGBMo: 0.10, NetworkEURPerGB: 0.01, Weights: cost.DefaultWeights()}
	return cm, DefaultConfig(fixedNow)
}

// R8: an orphaned changelog topic (no group is a prefix of its name) fires; one
// whose owning Streams app is still present (group is a prefix) does not.
func TestR8OrphanedInternalTopic(t *testing.T) {
	cm, cfg := priceCfg()
	snap := Snapshot{
		Cluster: ClusterInfo{Provider: "self"},
		Topics: []Topic{
			{Name: "orders-app-store-changelog", RetainedBytes: 4 << 30, CompressionType: "producer"},
			{Name: "payments-app-store-changelog", RetainedBytes: 4 << 30, CompressionType: "producer"},
		},
		Groups: []ConsumerGrp{
			{GroupID: "payments-app", State: "Stable"}, // owns payments-app-* → not orphaned
		},
	}
	fs := findByRule(Evaluate(SnapshotHistory{snap}, cm, cfg), "R8")
	if len(fs) != 1 {
		t.Fatalf("expected exactly 1 R8 finding, got %d", len(fs))
	}
	if fs[0].ResourceKey != "R8:orders-app-store-changelog" {
		t.Fatalf("R8 fired on the wrong topic: %s", fs[0].ResourceKey)
	}
	want := round2(gb(4<<30) * 0.10)
	if fs[0].EstSavingEUR != want {
		t.Fatalf("R8 saving = %.2f, want %.2f", fs[0].EstSavingEUR, want)
	}
}

// R1 must NOT also fire on a Streams internal topic (R8 owns it) — no double-bill.
func TestR1SkipsStreamsInternal(t *testing.T) {
	cm, cfg := priceCfg()
	snap := Snapshot{
		Cluster: ClusterInfo{Provider: "self"},
		Topics: []Topic{
			{Name: "orders-app-store-changelog", RetainedBytes: 4 << 30, BytesInPerSec: 0, ActiveConsumers: 0, MaxConsumerLagMs: -1},
		},
	}
	if fs := findByRule(Evaluate(SnapshotHistory{snap}, cm, cfg), "R1"); len(fs) != 0 {
		t.Fatalf("R1 should skip Streams internal topics, but fired %d times", len(fs))
	}
}

// R2: long-empty group fires; recently-empty, unknown-age, and active groups don't.
func TestR2ZombieGroup(t *testing.T) {
	cm, cfg := priceCfg()
	snap := Snapshot{
		Cluster: ClusterInfo{Provider: "self"},
		Groups: []ConsumerGrp{
			{GroupID: "old-etl", State: "Empty", LastCommitAgeDays: 95},  // fires
			{GroupID: "recent", State: "Empty", LastCommitAgeDays: 5},    // too recent
			{GroupID: "unknown", State: "Empty", LastCommitAgeDays: -1},  // age unknown
			{GroupID: "live", State: "Stable", LastCommitAgeDays: 200},   // active
		},
	}
	fs := findByRule(Evaluate(SnapshotHistory{snap}, cm, cfg), "R2")
	if len(fs) != 1 || fs[0].ResourceKey != "R2:old-etl" {
		t.Fatalf("expected 1 R2 finding for old-etl, got %+v", fs)
	}
	if fs[0].EstSavingEUR != 0 {
		t.Fatalf("R2 is hygiene (€0), got %.2f", fs[0].EstSavingEUR)
	}
}

// R7: a skewed topic fires; a balanced one does not.
func TestR7PartitionSkew(t *testing.T) {
	cm, cfg := priceCfg()
	gbv := int64(1) << 30
	snap := Snapshot{
		Cluster: ClusterInfo{Provider: "self"},
		Topics: []Topic{
			{Name: "skewed", RetainedBytes: 13 * gbv, PartitionBytes: []int64{gbv, gbv, gbv, 10 * gbv}},
			{Name: "balanced", RetainedBytes: 8 * gbv, PartitionBytes: []int64{2 * gbv, 2 * gbv, 2 * gbv, 2 * gbv}},
		},
	}
	fs := findByRule(Evaluate(SnapshotHistory{snap}, cm, cfg), "R7")
	if len(fs) != 1 || fs[0].ResourceKey != "R7:skewed" {
		t.Fatalf("expected 1 R7 finding for skewed, got %+v", fs)
	}
	if fs[0].EstSavingEUR != 0 {
		t.Fatalf("R7 is reliability (€0), got %.2f", fs[0].EstSavingEUR)
	}
}

func TestMedianInt64(t *testing.T) {
	cases := []struct {
		in   []int64
		want int64
	}{
		{[]int64{1, 2, 3}, 2},
		{[]int64{1, 2, 3, 4}, 2}, // (2+3)/2 = 2 (int)
		{[]int64{5}, 5},
		{nil, 0},
	}
	for _, c := range cases {
		if got := medianInt64(c.in); got != c.want {
			t.Errorf("median(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}
