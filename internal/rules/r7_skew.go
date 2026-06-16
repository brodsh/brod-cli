package rules

import (
	"fmt"
	"sort"
)

// R7 — Partition skew. When one partition holds far more than the median, the
// topic has a hot key / bad partitioner — a reliability problem (uneven broker
// load, lag concentration), not a cost one. €0 savings. Needs per-partition
// sizes (Topic.PartitionBytes); skips topics without them.
const r7Version = 1

// R7Config holds R7 thresholds.
type R7Config struct {
	SkewFactor    float64 // flag when max partition > factor × median
	MinTopicBytes int64   // ignore small topics
	MinPartitions int     // need enough partitions for a meaningful median
}

func defaultR7() R7Config {
	return R7Config{SkewFactor: 3, MinTopicBytes: 1 << 30, MinPartitions: 3} // 1 GB
}

func evalR7(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R7
	var out []Finding
	for _, t := range ctx.snap.Topics {
		if len(t.PartitionBytes) < cfg.MinPartitions || t.RetainedBytes < cfg.MinTopicBytes {
			continue
		}
		sizes := append([]int64(nil), t.PartitionBytes...)
		sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
		med := medianInt64(sizes)
		maxP := sizes[len(sizes)-1]
		if med <= 0 || float64(maxP) <= cfg.SkewFactor*float64(med) {
			continue
		}
		skew := float64(maxP) / float64(med)

		out = append(out, Finding{
			RuleID:      "R7",
			RuleVersion: r7Version,
			Title:       fmt.Sprintf("Partition skew on %q: largest %.1f GB vs %.1f GB median", t.Name, gb(maxP), gb(med)),
			Severity:    severityForBytes(t.RetainedBytes),
			ResourceKey: "R7:" + t.Name,
			Evidence: map[string]any{
				"max_partition_gb":    round2(gb(maxP)),
				"median_partition_gb": round2(gb(med)),
				"skew_x":              round2(skew),
				"partitions":          len(sizes),
			},
			EstSavingEUR: 0,
			Basis:        "reliability finding — €0 savings; flags an uneven keying/partitioning problem (hot partition)",
			Confidence:   ConfidenceHigh,
			Remediation: Remediation{
				Summary: fmt.Sprintf("Investigate the partitioning key for %q — one partition holds %.1f× the median size. Rebalancing requires a key/partitioner change (topic migration).",
					t.Name, skew),
				OneClick: false,
			},
		})
	}
	return out
}

func medianInt64(sorted []int64) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
