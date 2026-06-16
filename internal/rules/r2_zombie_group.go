package rules

import (
	"fmt"
	"strings"
)

// R2 — Zombie consumer group. A group that's been EMPTY (no members) for a long
// time with stale committed offsets is dead weight. €0 hygiene — bundle into a
// cleanup PR. Needs a last-commit age; when that's unknown (-1, e.g. the CLI's
// single-snapshot collector) the rule skips, consistent with R3.
const r2Version = 1

// R2Config holds R2 thresholds.
type R2Config struct {
	MinEmptyDays int
}

func defaultR2() R2Config { return R2Config{MinEmptyDays: 30} }

func evalR2(ctx evalCtx) []Finding {
	cfg := ctx.cfg.R2
	var out []Finding
	for _, g := range ctx.snap.Groups {
		if !strings.EqualFold(strings.TrimSpace(g.State), "Empty") {
			continue
		}
		if g.LastCommitAgeDays < 0 {
			continue // age unknown — needs history; SaaS confirms it
		}
		if g.LastCommitAgeDays < cfg.MinEmptyDays {
			continue
		}
		out = append(out, Finding{
			RuleID:      "R2",
			RuleVersion: r2Version,
			Title:       fmt.Sprintf("Zombie consumer group %q: empty for %dd", g.GroupID, g.LastCommitAgeDays),
			Severity:    SeverityInfo,
			ResourceKey: "R2:" + g.GroupID,
			Evidence: map[string]any{
				"state":                "Empty",
				"last_commit_age_days": g.LastCommitAgeDays,
			},
			EstSavingEUR: 0,
			Basis:        "hygiene finding — €0 savings; clears stale committed-offset state for a long-empty group",
			Confidence:   ConfidenceHigh,
			Remediation: Remediation{
				Summary:         fmt.Sprintf("Delete the long-empty consumer group %q (bundle into a cleanup PR).", g.GroupID),
				KafkaConfigsCmd: fmt.Sprintf("kafka-consumer-groups --delete --group %s", g.GroupID),
				OneClick:        false,
			},
		})
	}
	return out
}
