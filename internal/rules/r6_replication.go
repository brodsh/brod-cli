package rules

import (
	"fmt"
	"regexp"
)

// R6 — Replication mismatch. Static-capable: RF and name are in one snapshot.
// Non-prod topics (dev/test/tmp/staging) carrying RF=3 waste storage on copies
// they don't need.
const r6Version = 1

// R6Config holds R6 thresholds.
type R6Config struct {
	NamePattern *regexp.Regexp
	TriggerRF   int // only flag topics at or above this RF
	ProposedRF  int // recommended RF for non-prod topics
}

func defaultR6() R6Config {
	return R6Config{
		NamePattern: regexp.MustCompile(`(?i)(^|[._-])(dev|test|tmp|temp|staging|sandbox)([._-]|$)`),
		TriggerRF:   3,
		ProposedRF:  2,
	}
}

func evalR6(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R6
	var out []Finding
	for _, t := range ctx.snap.Topics {
		if t.ReplicationFactor < cfg.TriggerRF || cfg.ProposedRF >= t.ReplicationFactor {
			continue
		}
		if cfg.NamePattern == nil || !cfg.NamePattern.MatchString(t.Name) {
			continue
		}
		retainedGB := gb(t.RetainedBytes)
		frac := float64(t.ReplicationFactor-cfg.ProposedRF) / float64(t.ReplicationFactor)
		eur := retainedGB * frac * ctx.prices.StorageEURPerGBMo

		out = append(out, Finding{
			RuleID:      "R6",
			RuleVersion: r6Version,
			Title:       fmt.Sprintf("Non-prod topic %q runs RF=%d", t.Name, t.ReplicationFactor),
			Severity:    SeverityWarning,
			ResourceKey: "R6:" + t.Name,
			Evidence: map[string]any{
				"replication_factor": t.ReplicationFactor,
				"proposed_rf":        cfg.ProposedRF,
				"retained_gb":        round2(retainedGB),
				"matched_pattern":    cfg.NamePattern.String(),
			},
			EstSavingEUR: round2(eur),
			Basis: fmt.Sprintf(
				"%.1f GB × (%d−%d)/%d replicas × %.4f €/GB-mo. Storage basis: %s",
				retainedGB, t.ReplicationFactor, cfg.ProposedRF, t.ReplicationFactor,
				ctx.prices.StorageEURPerGBMo, ctx.prices.StorageBasis),
			Confidence: ConfidenceMedium,
			Remediation: Remediation{
				Summary:       fmt.Sprintf("Reduce replication factor to %d via a partition-reassignment PR (non-prod data).", cfg.ProposedRF),
				TerraformDiff: fmt.Sprintf("  topic %q {\n-   replication_factor = %d\n+   replication_factor = %d\n  }", t.Name, t.ReplicationFactor, cfg.ProposedRF),
				StrimziYAML:   fmt.Sprintf("  spec:\n    replicas: %d", cfg.ProposedRF),
				OneClick:      false, // RF change needs a reassignment plan
			},
		})
	}
	return out
}
