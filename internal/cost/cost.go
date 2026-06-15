// Package cost holds the cost model, unit-price derivation, and the per-topic
// allocation used by both the CLI and the SaaS. It is PURE (no I/O, no clock).
//
// It deliberately does NOT import internal/rules: the rules package imports this
// one for pricing, so allocation works over a minimal TopicCostInput rather than
// rules.Topic to avoid an import cycle.
//
// PILLAR: euros, not metrics. Every derived figure carries a Basis label and is
// honest about whether it is configured, derived, or assumed.
package cost

import "fmt"

const bytesPerGB = 1e9
const secondsPerMonth = 30 * 24 * 3600 // 30-day month, matches 30d averages

// Weights split a cluster's cost across storage / throughput / partition drivers.
type Weights struct {
	Storage    float64 `json:"storage"`
	Throughput float64 `json:"throughput"`
	Partition  float64 `json:"partition"`
}

// DefaultWeights is the 40/40/20 split (storage/throughput/partitions).
func DefaultWeights() Weights { return Weights{0.40, 0.40, 0.20} }

// CostModel describes how a cluster's euros are priced. MonthlyCostEUR is the
// manual € input (0 if unknown). The *EURPer* overrides win when > 0; otherwise
// they are derived from MonthlyCostEUR, or fall back to a provider preset.
type CostModel struct {
	Provider          string  `json:"provider"`
	MonthlyCostEUR    float64 `json:"monthly_cost_eur"`
	StorageEURPerGBMo float64 `json:"storage_eur_per_gb_mo"`
	NetworkEURPerGB   float64 `json:"network_eur_per_gb"`
	Weights           Weights `json:"weights"`
}

// UnitPrices are the resolved €/GB figures the rules engine multiplies against,
// each with an honest Basis label.
type UnitPrices struct {
	StorageEURPerGBMo float64
	StorageBasis      string
	NetworkEURPerGB   float64
	NetworkBasis      string
}

// providerPreset returns assumed (storage €/GB-mo, network €/GB) for a provider.
// These are labeled as assumptions everywhere they surface.
func providerPreset(provider string) (storage, network float64) {
	switch provider {
	case "confluent":
		return 0.12, 0.04
	case "msk":
		return 0.11, 0.02
	default: // self-hosted / unknown
		return 0.08, 0.01
	}
}

// Resolve derives the unit prices given the cluster's total retained bytes.
// totalRetainedBytes is used to turn a whole-cluster monthly € into a €/GB rate.
func (m CostModel) Resolve(totalRetainedBytes int64) UnitPrices {
	w := m.Weights
	if w == (Weights{}) {
		w = DefaultWeights()
	}
	presetStorage, presetNetwork := providerPreset(m.Provider)
	up := UnitPrices{}

	switch {
	case m.StorageEURPerGBMo > 0:
		up.StorageEURPerGBMo = m.StorageEURPerGBMo
		up.StorageBasis = fmt.Sprintf("configured (%.4f €/GB-mo)", m.StorageEURPerGBMo)
	case m.MonthlyCostEUR > 0 && totalRetainedBytes > 0:
		totalGB := float64(totalRetainedBytes) / bytesPerGB
		storagePortion := m.MonthlyCostEUR * w.Storage
		up.StorageEURPerGBMo = storagePortion / totalGB
		up.StorageBasis = fmt.Sprintf("derived: %.0f€/mo × %.0f%% storage weight ÷ %.1f GB retained",
			m.MonthlyCostEUR, w.Storage*100, totalGB)
	default:
		up.StorageEURPerGBMo = presetStorage
		up.StorageBasis = fmt.Sprintf("assumed: %s preset (%.4f €/GB-mo) — give --cost for a derived rate", providerLabel(m.Provider), presetStorage)
	}

	if m.NetworkEURPerGB > 0 {
		up.NetworkEURPerGB = m.NetworkEURPerGB
		up.NetworkBasis = fmt.Sprintf("configured (%.4f €/GB)", m.NetworkEURPerGB)
	} else {
		up.NetworkEURPerGB = presetNetwork
		up.NetworkBasis = fmt.Sprintf("assumed: %s preset (%.4f €/GB)", providerLabel(m.Provider), presetNetwork)
	}
	return up
}

func providerLabel(p string) string {
	if p == "" {
		return "self-hosted"
	}
	return p
}

// MonthlyGB converts a bytes/sec rate into GB ingested per 30-day month.
func MonthlyGB(bytesPerSec float64) float64 {
	return bytesPerSec * secondsPerMonth / bytesPerGB
}

// TopicCostInput is the minimal per-topic data needed for allocation.
type TopicCostInput struct {
	Name          string
	RetainedBytes int64
	BytesInPerSec float64
	BytesOutPerSec float64
	Partitions    int
}

// TopicCost is an allocated monthly euro figure for one topic.
type TopicCost struct {
	Name     string
	EUR      float64
	Basis    string
}

// Allocate splits the cluster's MonthlyCostEUR across topics using the weighted
// storage/throughput/partition model (default 40/40/20). Returns zero-euro,
// basis-labeled entries when no cluster cost is known — never a fabricated euro.
func Allocate(m CostModel, topics []TopicCostInput) []TopicCost {
	w := m.Weights
	if w == (Weights{}) {
		w = DefaultWeights()
	}
	var totBytes, totThru float64
	var totParts int
	for _, t := range topics {
		totBytes += float64(t.RetainedBytes)
		totThru += t.BytesInPerSec + t.BytesOutPerSec
		totParts += t.Partitions
	}
	out := make([]TopicCost, 0, len(topics))
	for _, t := range topics {
		if m.MonthlyCostEUR <= 0 {
			out = append(out, TopicCost{Name: t.Name, EUR: 0,
				Basis: "no cluster cost given — set --cost to allocate"})
			continue
		}
		share := weightedShare(w, t, totBytes, totThru, totParts)
		out = append(out, TopicCost{
			Name:  t.Name,
			EUR:   m.MonthlyCostEUR * share,
			Basis: fmt.Sprintf("allocated from %.0f€/mo using %.0f/%.0f/%.0f weights",
				m.MonthlyCostEUR, w.Storage*100, w.Throughput*100, w.Partition*100),
		})
	}
	return out
}

func weightedShare(w Weights, t TopicCostInput, totBytes, totThru float64, totParts int) float64 {
	var s float64
	if totBytes > 0 {
		s += w.Storage * (float64(t.RetainedBytes) / totBytes)
	}
	if totThru > 0 {
		s += w.Throughput * ((t.BytesInPerSec + t.BytesOutPerSec) / totThru)
	}
	if totParts > 0 {
		s += w.Partition * (float64(t.Partitions) / float64(totParts))
	}
	return s
}
