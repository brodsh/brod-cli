package rules

import "fmt"

// R3 — Retention overkill ⭐ demo. PARTIAL in the CLI: it needs a lag-time
// horizon, which the SaaS computes from snapshot history. If the snapshot
// carries MaxConsumerLagMs (a collector can supply a single-snapshot estimate)
// we evaluate it and label the result; otherwise the topic is skipped.
const r3Version = 1

const dayMs = int64(24 * 60 * 60 * 1000)

// R3Config holds R3 thresholds.
type R3Config struct {
	LagMultiplier    float64 // retention must exceed this × lag-time to trigger
	MinRetainedBytes int64   // ignore small topics
	MinFloorMs       int64   // proposed retention never goes below this
}

func defaultR3() R3Config {
	return R3Config{LagMultiplier: 4, MinRetainedBytes: 5 << 30, MinFloorMs: dayMs} // 5 GB, 24h floor
}

func evalR3(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R3
	var out []Finding
	for _, t := range ctx.snap.Topics {
		if t.RetainedBytes < cfg.MinRetainedBytes {
			continue
		}
		if t.MaxConsumerLagMs < 0 {
			continue // lag-time unknown — history-dependent, SaaS-only
		}
		if t.RetentionMs >= 0 && float64(t.RetentionMs) <= cfg.LagMultiplier*float64(t.MaxConsumerLagMs) {
			continue // retention already proportionate to consumption horizon
		}
		proposed := maxI64(cfg.MinFloorMs, 2*t.MaxConsumerLagMs)
		if t.RetentionMs >= 0 && proposed >= t.RetentionMs {
			continue
		}

		// Projected retained bytes at the proposed retention, guarded by the
		// current observed size (never project larger than what's on disk).
		projected := minF(
			ctx.snap.lookupBytesInPerSec(t.Name)*float64(proposed)/1000.0,
			float64(t.RetainedBytes),
		)
		savedGB := (float64(t.RetainedBytes) - projected) / 1e9
		if savedGB <= 0 {
			continue
		}
		eur := savedGB * ctx.prices.StorageEURPerGBMo

		out = append(out, Finding{
			RuleID:      "R3",
			RuleVersion: r3Version,
			Title:       fmt.Sprintf("Topic %q keeps %s retention vs %s consumption horizon", t.Name, durMs(t.RetentionMs), durMs(t.MaxConsumerLagMs)),
			Severity:    SeverityWarning,
			ResourceKey: "R3:" + t.Name,
			Evidence: map[string]any{
				"retention_ms":         t.RetentionMs,
				"proposed_retention_ms": proposed,
				"max_consumer_lag_ms":  t.MaxConsumerLagMs,
				"retained_gb":          round2(gb(t.RetainedBytes)),
				"projected_gb":         round2(projected / 1e9),
			},
			EstSavingEUR: round2(eur),
			Basis: fmt.Sprintf(
				"(%.1f GB current − %.1f GB projected at %s) × %.4f €/GB-mo; projected = bytes-in × proposed retention. Storage basis: %s",
				gb(t.RetainedBytes), projected/1e9, durMs(proposed), ctx.prices.StorageEURPerGBMo, ctx.prices.StorageBasis),
			Confidence: ConfidenceMedium,
			Remediation: Remediation{
				Summary:         fmt.Sprintf("Lower retention.ms on %q to %d (%s).", t.Name, proposed, durMs(proposed)),
				KafkaConfigsCmd: fmt.Sprintf("kafka-configs --alter --topic %s --add-config retention.ms=%d", t.Name, proposed),
				TerraformDiff:   fmt.Sprintf("  config = {\n-   \"retention.ms\" = \"%d\"\n+   \"retention.ms\" = \"%d\"\n  }", t.RetentionMs, proposed),
				StrimziYAML:     fmt.Sprintf("    config:\n      retention.ms: %d", proposed),
				OneClick:        true,
			},
		})
	}
	return out
}

// lookupBytesInPerSec finds a topic's ingest rate within the snapshot.
func (s Snapshot) lookupBytesInPerSec(name string) float64 {
	for _, t := range s.Topics {
		if t.Name == name {
			return t.BytesInPerSec
		}
	}
	return 0
}

func durMs(ms int64) string {
	if ms < 0 {
		return "∞"
	}
	d := float64(ms) / float64(dayMs)
	if d >= 1 {
		return fmt.Sprintf("%.1fd", d)
	}
	return fmt.Sprintf("%.1fh", float64(ms)/(60*60*1000))
}
