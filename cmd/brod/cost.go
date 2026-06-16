package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/brodsh/brod-cli/internal/cost"
	"github.com/brodsh/brod-cli/internal/rules"
)

// teamRule maps topics matching a regex to a team name (ordered; first match wins).
type teamRule struct {
	re   *regexp.Regexp
	name string
}

// teamFlags collects repeated `--team 'regex=Team'` flags in order.
type teamFlags struct{ rules []teamRule }

func (tf *teamFlags) String() string { return fmt.Sprintf("%d rule(s)", len(tf.rules)) }

func (tf *teamFlags) Set(v string) error {
	i := strings.Index(v, "=")
	if i <= 0 {
		return fmt.Errorf("--team must be 'regex=Team', got %q", v)
	}
	re, err := regexp.Compile(v[:i])
	if err != nil {
		return fmt.Errorf("--team regex %q: %w", v[:i], err)
	}
	tf.rules = append(tf.rules, teamRule{re: re, name: v[i+1:]})
	return nil
}

// teamFor returns the first matching team, or "unassigned".
func (tf teamFlags) teamFor(topic string) string {
	for _, r := range tf.rules {
		if r.re.MatchString(topic) {
			return r.name
		}
	}
	return "unassigned"
}

// cmdCost implements `brod cost`: allocate a cluster's monthly € across topics
// (and optionally teams) using the weighted 40/40/20 model. Every figure is
// basis-labeled; with no --cost it shows the drivers and nudges for the number.
func cmdCost(argv []string) error {
	fs := flag.NewFlagSet("cost", flag.ContinueOnError)
	snapPath := fs.String("snapshot", "", "snapshot JSON file ('-' for stdin)")
	clusterCost := fs.Float64("cost", 0, "cluster monthly cost in €")
	provider := fs.String("provider", "", "confluent | msk | self")
	asJSON := fs.Bool("json", false, "emit allocation as JSON")
	var teams teamFlags
	fs.Var(&teams, "team", "ordered 'regex=Team' mapping (repeatable)")
	cf := registerConnectFlags(fs)
	if err := fs.Parse(argv); err != nil {
		return err
	}

	snap, err := loadSnapshot(snapPath, cf)
	if err != nil {
		return err
	}

	prov := *provider
	if prov == "" {
		prov = snap.Cluster.Provider
	}
	cm := cost.CostModel{Provider: prov, MonthlyCostEUR: *clusterCost, Weights: cost.DefaultWeights()}

	inputs := make([]cost.TopicCostInput, 0, len(snap.Topics))
	for _, t := range snap.Topics {
		inputs = append(inputs, cost.TopicCostInput{
			Name:           t.Name,
			RetainedBytes:  t.RetainedBytes,
			BytesInPerSec:  t.BytesInPerSec,
			BytesOutPerSec: t.BytesOutPerSec,
			Partitions:     t.Partitions,
		})
	}
	alloc := cost.Allocate(cm, inputs)

	if *asJSON {
		return emitCostJSON(os.Stdout, snap, cm, alloc, teams)
	}
	renderCost(os.Stdout, snap, cm, alloc, teams)
	return nil
}

// loadSnapshot resolves a snapshot from --bootstrap (live, read-only) or
// --snapshot / stdin / demo, shared by scan and cost.
func loadSnapshot(snapPath *string, cf connectFlags) (rules.Snapshot, error) {
	if *cf.bootstrap != "" {
		ctx, cancel := context.WithTimeout(context.Background(), collectTimeout(*cf.sampleWindow))
		defer cancel()
		return connectAndCollect(ctx, cf)
	}
	raw, _, err := readSnapshot(*snapPath)
	if err != nil {
		return rules.Snapshot{}, err
	}
	var snap rules.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return rules.Snapshot{}, fmt.Errorf("parsing snapshot: %w", err)
	}
	return snap, nil
}

func renderCost(w io.Writer, snap rules.Snapshot, cm cost.CostModel, alloc []cost.TopicCost, teams teamFlags) {
	hasCost := cm.MonthlyCostEUR > 0
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  brod cost — cluster %q (%s)\n", clusterName(snap), providerOf(snap))
	if !hasCost {
		fmt.Fprintln(w, "  No --cost given: showing the storage driver per topic. Pass --cost <€/mo> to allocate euros.")
	} else {
		fmt.Fprintf(w, "  Allocating €%.2f/mo across %d topics using 40/40/20 (storage/throughput/partitions).\n", cm.MonthlyCostEUR, len(snap.Topics))
	}
	fmt.Fprintln(w, "  "+strings.Repeat("─", 64))

	if len(teams.rules) > 0 {
		renderTeams(w, snap, alloc, teams, hasCost)
		return
	}
	renderTopics(w, snap, alloc, hasCost)
}

func renderTopics(w io.Writer, snap rules.Snapshot, alloc []cost.TopicCost, hasCost bool) {
	type row struct {
		name string
		eur  float64
		gb   float64
	}
	gbByTopic := map[string]float64{}
	for _, t := range snap.Topics {
		gbByTopic[t.Name] = float64(t.RetainedBytes) / 1e9
	}
	rows := make([]row, 0, len(alloc))
	var total float64
	for _, a := range alloc {
		rows = append(rows, row{a.Name, a.EUR, gbByTopic[a.Name]})
		total += a.EUR
	}
	sort.Slice(rows, func(i, j int) bool {
		if hasCost {
			return rows[i].eur > rows[j].eur
		}
		return rows[i].gb > rows[j].gb
	})

	if hasCost {
		fmt.Fprintf(w, "  %-40s  %-10s  %s\n", "TOPIC", "€/MO", "SHARE")
		for _, r := range rows {
			share := 0.0
			if total > 0 {
				share = 100 * r.eur / total
			}
			fmt.Fprintf(w, "  %-40s  %-10s  %4.1f%%\n", truncate(r.name, 40), eur(r.eur), share)
		}
		fmt.Fprintln(w, "  "+strings.Repeat("─", 64))
		fmt.Fprintf(w, "  %-40s  %-10s\n", "TOTAL", eur(total))
	} else {
		fmt.Fprintf(w, "  %-40s  %s\n", "TOPIC", "RETAINED GB")
		for _, r := range rows {
			fmt.Fprintf(w, "  %-40s  %8.1f\n", truncate(r.name, 40), r.gb)
		}
	}
	if len(alloc) > 0 {
		fmt.Fprintf(w, "\n  basis: %s\n", alloc[0].Basis)
	}
	fmt.Fprintln(w, "\n  → Per-team attribution and showback CSV are on the roadmap at https://brod.sh")
}

func renderTeams(w io.Writer, snap rules.Snapshot, alloc []cost.TopicCost, teams teamFlags, hasCost bool) {
	eurByTeam := map[string]float64{}
	gbByTeam := map[string]float64{}
	gbByTopic := map[string]float64{}
	for _, t := range snap.Topics {
		gbByTopic[t.Name] = float64(t.RetainedBytes) / 1e9
	}
	for _, a := range alloc {
		team := teams.teamFor(a.Name)
		eurByTeam[team] += a.EUR
		gbByTeam[team] += gbByTopic[a.Name]
	}
	type row struct {
		team string
		eur  float64
		gb   float64
	}
	rows := make([]row, 0, len(eurByTeam))
	var total float64
	for team, e := range eurByTeam {
		rows = append(rows, row{team, e, gbByTeam[team]})
		total += e
	}
	sort.Slice(rows, func(i, j int) bool {
		if hasCost {
			return rows[i].eur > rows[j].eur
		}
		return rows[i].gb > rows[j].gb
	})

	if hasCost {
		fmt.Fprintf(w, "  %-24s  %-10s  %s\n", "TEAM", "€/MO", "SHARE")
		for _, r := range rows {
			share := 0.0
			if total > 0 {
				share = 100 * r.eur / total
			}
			fmt.Fprintf(w, "  %-24s  %-10s  %4.1f%%\n", r.team, eur(r.eur), share)
		}
		fmt.Fprintln(w, "  "+strings.Repeat("─", 64))
		fmt.Fprintf(w, "  %-24s  %-10s\n", "TOTAL", eur(total))
	} else {
		fmt.Fprintf(w, "  %-24s  %s\n", "TEAM", "RETAINED GB")
		for _, r := range rows {
			fmt.Fprintf(w, "  %-24s  %8.1f\n", r.team, r.gb)
		}
	}
	if u := eurByTeam["unassigned"]; u > 0 || gbByTeam["unassigned"] > 0 {
		fmt.Fprintln(w, "\n  note: 'unassigned' topics matched no --team rule — add rules to finish the mapping.")
	}
}

func emitCostJSON(w io.Writer, snap rules.Snapshot, cm cost.CostModel, alloc []cost.TopicCost, teams teamFlags) error {
	type topicOut struct {
		Topic string  `json:"topic"`
		EUR   float64 `json:"eur_per_month"`
		Team  string  `json:"team,omitempty"`
		Basis string  `json:"basis"`
	}
	out := make([]topicOut, 0, len(alloc))
	for _, a := range alloc {
		t := topicOut{Topic: a.Name, EUR: a.EUR, Basis: a.Basis}
		if len(teams.rules) > 0 {
			t.Team = teams.teamFor(a.Name)
		}
		out = append(out, t)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"cluster":          snap.Cluster.Name,
		"monthly_cost_eur": cm.MonthlyCostEUR,
		"weights":          cm.Weights,
		"topics":           out,
	})
}
