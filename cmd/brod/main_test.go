package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/brodsh/brod-cli/internal/cost"
	"github.com/brodsh/brod-cli/internal/rules"
)

// TestEmbeddedSampleParsesAndScans is the CLI smoke test: the shipped demo
// snapshot must parse and produce findings through the shared engine.
func TestEmbeddedSampleParsesAndScans(t *testing.T) {
	var snap rules.Snapshot
	if err := json.Unmarshal(sampleSnapshot, &snap); err != nil {
		t.Fatalf("embedded sample does not parse: %v", err)
	}
	cm := cost.CostModel{Provider: snap.Cluster.Provider, Weights: cost.DefaultWeights()}
	fixed := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	fs := rules.Evaluate(rules.SnapshotHistory{snap}, cm, rules.DefaultConfig(fixed))
	if len(fs) == 0 {
		t.Fatal("expected findings from the demo snapshot")
	}
	for _, f := range fs {
		if strings.TrimSpace(f.Basis) == "" {
			t.Errorf("finding %s has no basis label — violates euros-not-metrics pillar", f.RuleID)
		}
	}
}

// TestCollectSnapshotRoundTrips proves a snapshot encoded the way `brod collect`
// writes it deserializes back through the same rules.Snapshot scan reads — so
// `brod collect --out x.json` then `brod scan --snapshot x.json` is coherent.
func TestCollectSnapshotRoundTrips(t *testing.T) {
	var snap rules.Snapshot
	if err := json.Unmarshal(sampleSnapshot, &snap); err != nil {
		t.Fatal(err)
	}
	// Encode exactly as cmdCollect does (MarshalIndent), then re-decode.
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var back rules.Snapshot
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("collect output does not round-trip through scan: %v", err)
	}
	if back.Cluster != snap.Cluster || len(back.Topics) != len(snap.Topics) {
		t.Errorf("round-trip mismatch: cluster %v vs %v, topics %d vs %d",
			back.Cluster, snap.Cluster, len(back.Topics), len(snap.Topics))
	}
}

// TestReportShowsBasisAndPointer guards the two non-negotiables in the rendered
// output: a basis section and the SaaS funnel pointer.
func TestReportShowsBasisAndPointer(t *testing.T) {
	var snap rules.Snapshot
	if err := json.Unmarshal(sampleSnapshot, &snap); err != nil {
		t.Fatal(err)
	}
	cm := cost.CostModel{Provider: snap.Cluster.Provider, Weights: cost.DefaultWeights()}
	fixed := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	fs := rules.Evaluate(rules.SnapshotHistory{snap}, cm, rules.DefaultConfig(fixed))

	var buf bytes.Buffer
	renderReport(&buf, snap, fs, true, false)
	out := buf.String()
	if !strings.Contains(out, "Basis (how each € was computed)") {
		t.Error("report missing basis section")
	}
	if !strings.Contains(out, "brod.sh") {
		t.Error("report missing SaaS funnel pointer")
	}
}
