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
