package rules

import (
	"sort"
	"time"

	"github.com/brodsh/brod-cli/internal/cost"
)

// RuleConfig carries the injected evaluation time, per-rule thresholds, and the
// enable/disable toggles. PURITY: Now is injected — rules must never call the
// clock themselves.
type RuleConfig struct {
	Now      time.Time
	Disabled map[string]bool
	R1       R1Config
	R3       R3Config
	R4       R4Config
	R5       R5Config
	R6       R6Config
}

// DefaultConfig returns the default thresholds with the evaluation time set.
func DefaultConfig(now time.Time) RuleConfig {
	return RuleConfig{
		Now:      now,
		Disabled: map[string]bool{},
		R1:       defaultR1(),
		R3:       defaultR3(),
		R4:       defaultR4(),
		R5:       defaultR5(),
		R6:       defaultR6(),
	}
}

// evalCtx is the per-evaluation bundle handed to each rule.
type evalCtx struct {
	snap   Snapshot
	prices cost.UnitPrices
	cfg    RuleConfig
}

// rule is one waste detector. StaticCapable=true means it produces useful output
// from a single snapshot (so the CLI runs it). History-only rules are skipped by
// the CLI path.
type rule struct {
	id            string
	staticCapable bool
	eval          func(evalCtx) []Finding
}

func registry() []rule {
	return []rule{
		{"R1", true, evalR1},
		{"R3", true, evalR3},
		{"R4", true, evalR4},
		{"R5", true, evalR5},
		{"R6", true, evalR6},
	}
}

// Evaluate is the pure entrypoint shared by the CLI and SaaS:
//
//	(snapshot history, cost model, config) -> []Finding
//
// The CLI passes a single-element history and gets the static-capable subset.
// Findings come back sorted by est. saving (desc), then rule id for stability.
func Evaluate(history SnapshotHistory, cm cost.CostModel, cfg RuleConfig) []Finding {
	snap, ok := history.Latest()
	if !ok {
		return nil
	}
	var totalRetained int64
	for _, t := range snap.Topics {
		totalRetained += t.RetainedBytes
	}
	ctx := evalCtx{
		snap:   snap,
		prices: cm.Resolve(totalRetained),
		cfg:    cfg,
	}

	var out []Finding
	staticOnly := len(history) <= 1
	for _, r := range registry() {
		if cfg.Disabled[r.id] {
			continue
		}
		if staticOnly && !r.staticCapable {
			continue
		}
		out = append(out, r.eval(ctx)...)
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EstSavingEUR != out[j].EstSavingEUR {
			return out[i].EstSavingEUR > out[j].EstSavingEUR
		}
		return out[i].RuleID < out[j].RuleID
	})
	return out
}

// TotalSavingsEUR sums the est. monthly saving across findings.
func TotalSavingsEUR(fs []Finding) float64 {
	var t float64
	for _, f := range fs {
		t += f.EstSavingEUR
	}
	return t
}

func gb(bytes int64) float64 { return float64(bytes) / 1e9 }
