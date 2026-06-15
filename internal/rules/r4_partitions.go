package rules

import (
	"fmt"
	"math"
)

// R4 — Over-partitioning. Static approximation: a single snapshot gives current
// partition count and an avg throughput/partition (bytes-in ÷ partitions); the
// full rule wants 14d-sustained data, so confidence is medium and severity is
// `plan` (Kafka can't shrink partitions in place — NEVER a one-click PR).
const r4Version = 1

// R4Config holds R4 thresholds.
type R4Config struct {
	MinPartitions      int     // only consider topics above this
	MaxBytesPerPartSec float64 // "under-used" partition throughput ceiling
	TargetBytesPerPart float64 // sizing target for the proposal
	// PerPartitionEURMo prices a Confluent-style per-partition dimension. When 0
	// (MSK/self-hosted), broker cost is amortized from the cost model instead.
	ConfluentPerPartitionEURMo float64
}

func defaultR4() R4Config {
	return R4Config{
		MinPartitions:              12,
		MaxBytesPerPartSec:         1 << 10, // 1 KB/s
		TargetBytesPerPart:         1 << 20, // 1 MB/s
		ConfluentPerPartitionEURMo: 0.5,     // assumed Confluent per-partition price
	}
}

func evalR4(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R4
	provider := ctx.snap.Cluster.Provider

	totalParts := 0
	for _, t := range ctx.snap.Topics {
		totalParts += t.Partitions
	}

	var out []Finding
	for _, t := range ctx.snap.Topics {
		if t.Partitions <= cfg.MinPartitions {
			continue
		}
		perPart := 0.0
		if t.Partitions > 0 {
			perPart = t.BytesInPerSec / float64(t.Partitions)
		}
		if perPart >= cfg.MaxBytesPerPartSec {
			continue
		}
		proposed := int(math.Max(float64(cfg.MinPartitions),
			math.Ceil(t.BytesInPerSec/cfg.TargetBytesPerPart)))
		if proposed >= t.Partitions {
			continue
		}
		removed := t.Partitions - proposed

		var eur float64
		var basis string
		if provider == "confluent" && cfg.ConfluentPerPartitionEURMo > 0 {
			eur = float64(removed) * cfg.ConfluentPerPartitionEURMo
			basis = fmt.Sprintf("ASSUMPTION: %d partitions removed × %.2f €/partition-mo (Confluent dimension)",
				removed, cfg.ConfluentPerPartitionEURMo)
		} else {
			// Amortized broker cost: the partition weight share of cluster cost,
			// spread over all partitions, times partitions removed.
			perPartCost := 0.0
			if totalParts > 0 {
				// Reconstruct cluster monthly € from the derived storage rate is
				// lossy; instead price the partition slice off retained storage as
				// a conservative proxy and label it clearly.
				clusterGB := 0.0
				for _, tt := range ctx.snap.Topics {
					clusterGB += gb(tt.RetainedBytes)
				}
				clusterStorageEUR := clusterGB * ctx.prices.StorageEURPerGBMo
				// partition weight share (20%) of a storage-priced cluster proxy.
				perPartCost = (0.20 * clusterStorageEUR) / float64(totalParts)
			}
			eur = perPartCost * float64(removed)
			basis = fmt.Sprintf("ASSUMPTION: amortized broker overhead — 20%% partition weight × storage-priced cluster ÷ %d partitions × %d removed. Storage basis: %s",
				totalParts, removed, ctx.prices.StorageBasis)
		}

		out = append(out, Finding{
			RuleID:      "R4",
			RuleVersion: r4Version,
			Title:       fmt.Sprintf("Topic %q over-partitioned: %d partitions at %.0f B/s each", t.Name, t.Partitions, perPart),
			Severity:    SeverityPlan, // migration required — never one-click
			ResourceKey: "R4:" + t.Name,
			Evidence: map[string]any{
				"partitions":             t.Partitions,
				"proposed_partitions":    proposed,
				"bytes_per_partition_sec": round2(perPart),
				"throughput_source":      t.ThroughputSource,
			},
			EstSavingEUR: round2(eur),
			Basis:        basis,
			Confidence:   ConfidenceMedium,
			Remediation: Remediation{
				Summary: fmt.Sprintf("PLAN: migrate %q from %d to %d partitions. Kafka cannot shrink partitions in place — create a new topic and reproduce/replay; coordinate with producers and key-ordering guarantees.",
					t.Name, t.Partitions, proposed),
				OneClick: false,
			},
		})
	}
	return out
}
