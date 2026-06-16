package rules

import "fmt"

// R1 — Dead topic. PARTIAL in the CLI: the full rule needs 30d of history to be
// sure a topic is truly dead. From a single snapshot we approximate: ~zero
// bytes-in AND no active consumers AND retained > threshold. Labeled as an
// approximation, confidence low — the SaaS confirms it over 30d before booking.
//
// v2: skips Kafka Streams internal topics (-changelog/-repartition) — those are
// R8's domain, so the two rules never bill the same orphaned topic.
const r1Version = 2

// R1Config holds R1 thresholds.
type R1Config struct {
	MaxBytesInPerSec float64 // "≈ 0" ceiling
	MinRetainedBytes int64   // ignore tiny topics
}

func defaultR1() R1Config {
	return R1Config{MaxBytesInPerSec: 1, MinRetainedBytes: 1 << 30} // 1 GB
}

func evalR1(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R1
	var out []Finding
	for _, t := range ctx.snap.Topics {
		if isStreamsInternal(t.Name) {
			continue // R8 owns orphaned -changelog/-repartition topics
		}
		if t.BytesInPerSec > cfg.MaxBytesInPerSec {
			continue
		}
		if t.ActiveConsumers > 0 {
			continue
		}
		if t.RetainedBytes < cfg.MinRetainedBytes {
			continue
		}
		retainedGB := gb(t.RetainedBytes)
		eur := retainedGB * ctx.prices.StorageEURPerGBMo

		out = append(out, Finding{
			RuleID:      "R1",
			RuleVersion: r1Version,
			Title:       fmt.Sprintf("Topic %q looks dead: %.1f GB retained, no traffic, no consumers", t.Name, retainedGB),
			Severity:    severityForBytes(t.RetainedBytes),
			ResourceKey: "R1:" + t.Name,
			Evidence: map[string]any{
				"bytes_in_per_sec": t.BytesInPerSec,
				"active_consumers": t.ActiveConsumers,
				"retained_gb":      round2(retainedGB),
				"approximation":    "single snapshot — SaaS confirms over 30d before booking",
			},
			EstSavingEUR: round2(eur),
			Basis: fmt.Sprintf(
				"APPROXIMATION (single snapshot): %.1f GB × %.4f €/GB-mo. Storage basis: %s",
				retainedGB, ctx.prices.StorageEURPerGBMo, ctx.prices.StorageBasis),
			Confidence: ConfidenceLow,
			Remediation: Remediation{
				Summary:         fmt.Sprintf("Delete topic %q after confirming no producers/consumers depend on it.", t.Name),
				KafkaConfigsCmd: fmt.Sprintf("kafka-topics --delete --topic %s", t.Name),
				TerraformDiff:   fmt.Sprintf("- resource \"kafka_topic\" %q { ... }", t.Name),
				OneClick:        false, // deletion is destructive — always reviewed
			},
		})
	}
	return out
}

func severityForBytes(b int64) Severity {
	switch {
	case b >= 100<<30: // >= 100 GB
		return SeverityCritical
	case b >= 10<<30: // >= 10 GB
		return SeverityWarning
	default:
		return SeverityInfo
	}
}
