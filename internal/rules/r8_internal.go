package rules

import (
	"fmt"
	"strings"
)

// R8 — Idle-but-retained internal topics. Kafka Streams creates internal topics
// named `<application.id>-<store>-changelog` and `<application.id>-...-repartition`.
// When the owning app is gone, these linger and keep costing storage. We detect
// "owner gone" statically: the app's consumer group id (== application.id) would
// appear as a PREFIX of the topic name, so if NO consumer group is a prefix of a
// changelog/repartition topic, the owning app is absent → orphaned. Savings as R1.
const r8Version = 1

// streamsInternalSuffixes are the Kafka Streams internal-topic suffixes. Shared
// with R1 so the two rules don't both bill the same orphaned topic.
var streamsInternalSuffixes = []string{"-changelog", "-repartition"}

// R8Config holds R8 thresholds.
type R8Config struct {
	MinRetainedBytes int64
	Suffixes         []string
}

func defaultR8() R8Config {
	return R8Config{MinRetainedBytes: 1 << 28, Suffixes: streamsInternalSuffixes} // 256 MB
}

func evalR8(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R8
	var out []Finding
	for _, t := range ctx.snap.Topics {
		suffix := matchSuffix(t.Name, cfg.Suffixes)
		if suffix == "" {
			continue
		}
		if t.RetainedBytes < cfg.MinRetainedBytes {
			continue
		}
		if hasOwningApp(t.Name, ctx.snap.Groups) {
			continue // a consumer group is a prefix → the owning app is still around
		}
		retainedGB := gb(t.RetainedBytes)
		eur := retainedGB * ctx.prices.StorageEURPerGBMo

		out = append(out, Finding{
			RuleID:      "R8",
			RuleVersion: r8Version,
			Title:       fmt.Sprintf("Orphaned internal topic %q: %.1f GB, owning app gone", t.Name, retainedGB),
			Severity:    severityForBytes(t.RetainedBytes),
			ResourceKey: "R8:" + t.Name,
			Evidence: map[string]any{
				"suffix":               suffix,
				"retained_gb":          round2(retainedGB),
				"owning_group_present": false,
				"approximation":        "single snapshot — confirm the Streams/Connect app is truly gone before deleting",
			},
			EstSavingEUR: round2(eur),
			Basis: fmt.Sprintf(
				"APPROXIMATION: no consumer group is a prefix of %q (Streams application.id absent) → orphaned. %.1f GB × %.4f €/GB-mo. Storage basis: %s",
				t.Name, retainedGB, ctx.prices.StorageEURPerGBMo, ctx.prices.StorageBasis),
			Confidence: ConfidenceMedium,
			Remediation: Remediation{
				Summary:         fmt.Sprintf("Delete orphaned internal topic %q after confirming no Kafka Streams/Connect app owns it.", t.Name),
				KafkaConfigsCmd: fmt.Sprintf("kafka-topics --delete --topic %s", t.Name),
				OneClick:        false, // deletion is destructive — always reviewed
			},
		})
	}
	return out
}

func matchSuffix(name string, suffixes []string) string {
	for _, s := range suffixes {
		if strings.HasSuffix(name, s) {
			return s
		}
	}
	return ""
}

// isStreamsInternal reports whether a topic is a Kafka Streams internal topic
// (R8's domain) so R1 can leave it alone and avoid double-billing.
func isStreamsInternal(name string) bool {
	return matchSuffix(name, streamsInternalSuffixes) != ""
}

// hasOwningApp reports whether any consumer group id is a prefix of the topic
// name — the signature of a live Streams app owning its internal topic.
func hasOwningApp(topic string, groups []ConsumerGrp) bool {
	for _, g := range groups {
		if g.GroupID != "" && strings.HasPrefix(topic, g.GroupID) {
			return true
		}
	}
	return false
}
