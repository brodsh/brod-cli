package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/brodsh/brod-cli/internal/rules"
)

// renderReport prints the euro-ranked waste report. PILLAR: every € figure is
// shown with its basis label; nothing is presented as measured.
func renderReport(w io.Writer, snap rules.Snapshot, fs []rules.Finding, usedDemo, verbose bool) {
	total := rules.TotalSavingsEUR(fs)

	fmt.Fprintln(w)
	fmt.Fprintf(w, "  brod scan — cluster %q (%s)\n", clusterName(snap), providerOf(snap))
	if usedDemo {
		fmt.Fprintln(w, "  [demo snapshot — run `brod scan --snapshot your.json` on your own metadata]")
	}
	fmt.Fprintf(w, "  %d findings · est. up to %s/mo recoverable\n", len(fs), eur(total))
	fmt.Fprintln(w, "  "+strings.Repeat("─", 64))

	if len(fs) == 0 {
		fmt.Fprintln(w, "  No waste detected in this snapshot. 🎉")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "  "+saasPointer)
		return
	}

	// Ranked table.
	fmt.Fprintf(w, "  %-4s  %-9s  %-12s  %-6s  %s\n", "RULE", "€/MO", "SEVERITY", "CONF", "FINDING")
	for _, f := range fs {
		fmt.Fprintf(w, "  %-4s  %-9s  %-12s  %-6s  %s\n",
			f.RuleID, eur(f.EstSavingEUR), f.Severity, f.Confidence, truncate(f.Title, 60))
	}
	fmt.Fprintln(w)

	// Basis (honesty) lines — always shown, never optional.
	fmt.Fprintln(w, "  Basis (how each € was computed):")
	for _, f := range fs {
		fmt.Fprintf(w, "   • [%s %s] %s\n", f.RuleID, eur(f.EstSavingEUR), f.Basis)
	}

	if verbose {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "  Remediation (review before applying — brod never touches your cluster):")
		for _, f := range fs {
			fmt.Fprintf(w, "\n   ▸ %s — %s\n", f.RuleID, f.Title)
			fmt.Fprintf(w, "     %s\n", f.Remediation.Summary)
			if f.Remediation.OneClick {
				fmt.Fprintln(w, "     delivery: one-click reviewable PR")
			} else {
				fmt.Fprintln(w, "     delivery: PLAN — needs a migration/reassignment you drive (never auto-applied)")
			}
			if c := f.Remediation.KafkaConfigsCmd; c != "" {
				fmt.Fprintf(w, "     $ %s\n", c)
			}
		}
	} else {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "  Run with --verbose for the exact config diff per finding.")
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "  "+saasPointer)
}

func clusterName(s rules.Snapshot) string {
	if s.Cluster.Name == "" {
		return "(unnamed)"
	}
	return s.Cluster.Name
}

func providerOf(s rules.Snapshot) string {
	if s.Cluster.Provider == "" {
		return "self-hosted"
	}
	return s.Cluster.Provider
}

func eur(v float64) string { return fmt.Sprintf("€%.2f", v) }

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
