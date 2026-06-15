package rules

import "fmt"

// R5 — Missing compression  ⭐ demo. Static-capable: a single snapshot carries
// bytes-in and compression.type. Savings assume a 0.65 zstd ratio for text
// payloads — LABELED AS ASSUMPTION, confidence low until measured post-fix.
const r5Version = 1

// R5Config holds R5 thresholds.
type R5Config struct {
	MinBytesInPerSec float64 // trigger floor
	AssumedRatio     float64 // fraction of bytes saved by zstd (0.65 = 65% smaller)
}

func defaultR5() R5Config {
	return R5Config{MinBytesInPerSec: 1 << 20, AssumedRatio: 0.65} // 1 MB/s
}

// uncompressedTypes are compression settings that leave data effectively
// uncompressed on the wire/disk.
var uncompressedTypes = map[string]bool{
	"": true, "none": true, "uncompressed": true, "producer": true,
}

func evalR5(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R5
	var out []Finding
	for _, t := range ctx.snap.Topics {
		if t.BytesInPerSec < cfg.MinBytesInPerSec {
			continue
		}
		if !uncompressedTypes[t.CompressionType] {
			continue
		}
		// Saved bytes/month × (storage €/GB-mo + one network €/GB pass).
		inGB := monthlyGB(t.BytesInPerSec)
		savedGB := inGB * cfg.AssumedRatio
		eur := savedGB * (ctx.prices.StorageEURPerGBMo + ctx.prices.NetworkEURPerGB)

		out = append(out, Finding{
			RuleID:      "R5",
			RuleVersion: r5Version,
			Title:       fmt.Sprintf("Topic %q ingests %.1f MB/s uncompressed", t.Name, t.BytesInPerSec/(1<<20)),
			Severity:    SeverityWarning,
			ResourceKey: "R5:" + t.Name,
			Evidence: map[string]any{
				"bytes_in_per_sec":   t.BytesInPerSec,
				"compression_type":   labelEmpty(t.CompressionType),
				"monthly_in_gb":      round2(inGB),
				"assumed_saved_gb":   round2(savedGB),
				"throughput_source":  t.ThroughputSource,
			},
			EstSavingEUR: round2(eur),
			Basis: fmt.Sprintf(
				"ASSUMPTION: %.1f GB/mo in × %.0f%% zstd ratio × (%.4f storage + %.4f network €/GB). Storage basis: %s",
				inGB, cfg.AssumedRatio*100, ctx.prices.StorageEURPerGBMo, ctx.prices.NetworkEURPerGB, ctx.prices.StorageBasis),
			Confidence: ConfidenceLow,
			Remediation: Remediation{
				Summary:         "Set compression.type=zstd on the topic (or enable producer-side zstd).",
				KafkaConfigsCmd: fmt.Sprintf("kafka-configs --alter --topic %s --add-config compression.type=zstd", t.Name),
				TerraformDiff:   fmt.Sprintf("  config = {\n-   \"compression.type\" = \"%s\"\n+   \"compression.type\" = \"zstd\"\n  }", labelEmpty(t.CompressionType)),
				StrimziYAML:     "    config:\n      compression.type: zstd",
				OneClick:        true,
			},
		})
	}
	return out
}

func labelEmpty(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
