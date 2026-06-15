package cost

import "testing"

func TestResolveDerivesStorageRate(t *testing.T) {
	m := CostModel{Provider: "self", MonthlyCostEUR: 1000, Weights: DefaultWeights()}
	// 1000 GB retained, 40% storage weight → 400€ / 1000 GB = 0.40 €/GB-mo.
	up := m.Resolve(1000 * 1e9)
	if got := round(up.StorageEURPerGBMo); got != 0.40 {
		t.Fatalf("derived storage = %.4f, want 0.40", up.StorageEURPerGBMo)
	}
	if up.StorageBasis == "" {
		t.Fatal("storage basis label must never be empty")
	}
}

func TestResolveFallsBackToPresetWithLabel(t *testing.T) {
	up := CostModel{Provider: "confluent"}.Resolve(0)
	if up.StorageEURPerGBMo != 0.12 {
		t.Fatalf("confluent preset = %.4f, want 0.12", up.StorageEURPerGBMo)
	}
	if up.StorageBasis == "" || up.StorageBasis[:7] != "assumed" {
		t.Fatalf("preset must be labeled assumed, got %q", up.StorageBasis)
	}
}

func TestAllocateSharesSumToClusterCost(t *testing.T) {
	m := CostModel{Provider: "self", MonthlyCostEUR: 900, Weights: DefaultWeights()}
	topics := []TopicCostInput{
		{Name: "a", RetainedBytes: 100 * 1e9, BytesInPerSec: 10, BytesOutPerSec: 5, Partitions: 4},
		{Name: "b", RetainedBytes: 50 * 1e9, BytesInPerSec: 5, BytesOutPerSec: 1, Partitions: 2},
		{Name: "c", RetainedBytes: 10 * 1e9, BytesInPerSec: 0, BytesOutPerSec: 0, Partitions: 1},
	}
	var sum float64
	for _, tc := range Allocate(m, topics) {
		sum += tc.EUR
	}
	if d := sum - 900; d < -0.01 || d > 0.01 {
		t.Fatalf("allocated sum = %.4f, want 900", sum)
	}
}

func TestAllocateNoCostNeverFabricatesEuros(t *testing.T) {
	m := CostModel{Provider: "self"} // no MonthlyCostEUR
	for _, tc := range Allocate(m, []TopicCostInput{{Name: "a", RetainedBytes: 1e9}}) {
		if tc.EUR != 0 {
			t.Fatalf("fabricated euro %.4f with no cluster cost", tc.EUR)
		}
		if tc.Basis == "" {
			t.Fatal("zero-euro allocation must carry a basis label")
		}
	}
}

func round(f float64) float64 {
	return float64(int(f*10000+0.5)) / 10000
}
