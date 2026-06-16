// Command brod is the open-source, read-only Kafka FinOps CLI — the funnel for
// brod.sh. It runs the static-capable subset of the SHARED rules engine over a
// metadata snapshot and prints a euro-ranked waste report.
//
// THREE PILLARS, enforced here:
//   - Read-only by design: brod only ever reads a snapshot file. It never
//     connects to, let alone writes to, a cluster.
//   - Metadata only: the snapshot carries topic/group/broker metadata — never
//     message payloads.
//   - Euros, not metrics: every line is a € figure with a labeled basis.
//
// Install: curl -fsSL brod.sh | sh
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	_ "embed"

	"github.com/brodsh/brod-cli/internal/cost"
	"github.com/brodsh/brod-cli/internal/rules"
)

//go:embed sample_snapshot.json
var sampleSnapshot []byte

const version = "0.2.0"

const saasPointer = "→ See this continuously, attributed per team, at https://brod.sh — read-only, metadata-only, fixes ship as PRs."

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "scan":
		err = cmdScan(os.Args[2:])
	case "collect":
		err = cmdCollect(os.Args[2:])
	case "sample":
		err = cmdSample(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("brod %s\n", version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "brod: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `brod — read-only Kafka FinOps CLI (brod.sh)

USAGE
  brod scan [--snapshot FILE | --bootstrap HOSTS] [--cost EUR] [--provider P] [--json] [--verbose]
  brod collect --bootstrap HOSTS [--out FILE] [connection flags]
  brod sample        print a sample snapshot JSON (the input format)
  brod version

scan reads a metadata snapshot and prints a euro-ranked waste report. Source it
from a JSON file (--snapshot), a live cluster (--bootstrap, read-only), or — with
neither and nothing on stdin — the built-in demo snapshot.

collect connects read-only to a live cluster and writes a metadata snapshot JSON
(round-trips with 'brod scan --snapshot'). brod NEVER produces or consumes: it
issues only Admin/Describe/Metadata/ListOffsets/OffsetFetch requests.

SCAN/REPORT FLAGS
  --snapshot FILE   snapshot JSON file ('-' for stdin)
  --cost EUR        cluster monthly cost in € (enables derived, not assumed, rates)
  --provider P      confluent | msk | self  (default: from snapshot, else self)
  --json            emit findings as JSON
  --verbose         show remediation (config diff) per finding

LIVE CONNECTION FLAGS (scan --bootstrap / collect)
  --bootstrap H     comma-separated bootstrap servers (host:port)
  --tls             use TLS
  --sasl-mechanism  plain | scram-sha-256 | scram-sha-512
  --user / --pass   SASL creds (or env BROD_KAFKA_USER / BROD_KAFKA_PASS)
  --cluster-name N  label for the snapshot
  --sample-window D throughput sampling window (default 30s; 0 = no estimate)
  --require-readonly  fail closed on detectable write/topic-READ ACLs
  --include-internal  include internal topics (e.g. __consumer_offsets)
`)
}

func cmdSample(_ []string) error {
	_, err := os.Stdout.Write(sampleSnapshot)
	return err
}

func cmdScan(argv []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	snapPath := fs.String("snapshot", "", "snapshot JSON file ('-' for stdin)")
	clusterCost := fs.Float64("cost", 0, "cluster monthly cost in €")
	provider := fs.String("provider", "", "confluent | msk | self")
	asJSON := fs.Bool("json", false, "emit findings as JSON")
	verbose := fs.Bool("verbose", false, "show remediation per finding")
	cf := registerConnectFlags(fs)
	if err := fs.Parse(argv); err != nil {
		return err
	}

	var (
		snap     rules.Snapshot
		usedDemo bool
	)
	if *cf.bootstrap != "" {
		// Live mode: collect from the cluster in-memory, then run rules.
		ctx, cancel := context.WithTimeout(context.Background(), collectTimeout(*cf.sampleWindow))
		defer cancel()
		s, err := connectAndCollect(ctx, cf)
		if err != nil {
			return err
		}
		snap = s
	} else {
		raw, demo, err := readSnapshot(*snapPath)
		if err != nil {
			return err
		}
		usedDemo = demo
		if err := json.Unmarshal(raw, &snap); err != nil {
			return fmt.Errorf("parsing snapshot: %w", err)
		}
	}

	prov := *provider
	if prov == "" {
		prov = snap.Cluster.Provider
	}
	cm := cost.CostModel{
		Provider:       prov,
		MonthlyCostEUR: *clusterCost,
		Weights:        cost.DefaultWeights(),
	}

	// The CLI is the boundary: it reads the wall clock and INJECTS it into the
	// pure engine, which never reads a clock itself.
	cfg := rules.DefaultConfig(time.Now().UTC())
	findings := rules.Evaluate(rules.SnapshotHistory{snap}, cm, cfg)

	if *asJSON {
		return emitJSON(os.Stdout, snap, findings)
	}
	renderReport(os.Stdout, snap, findings, usedDemo, *verbose)
	return nil
}

// readSnapshot resolves the snapshot bytes from a file, stdin, or the built-in
// demo. usedDemo is true when the embedded sample was used.
func readSnapshot(path string) (data []byte, usedDemo bool, err error) {
	switch {
	case path == "-":
		b, err := io.ReadAll(os.Stdin)
		return b, false, err
	case path != "":
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, false, err
		}
		return b, false, nil
	}
	// No --snapshot: use stdin if piped, else the embedded demo.
	if info, _ := os.Stdin.Stat(); info != nil && (info.Mode()&os.ModeCharDevice) == 0 {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, false, err
		}
		if len(b) > 0 {
			return b, false, nil
		}
	}
	if len(sampleSnapshot) == 0 {
		return nil, false, errors.New("no snapshot given and no embedded demo available")
	}
	return sampleSnapshot, true, nil
}

func emitJSON(w io.Writer, snap rules.Snapshot, fs []rules.Finding) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"cluster":            snap.Cluster.Name,
		"taken_at":           snap.TakenAt,
		"total_saving_eur":   rules.TotalSavingsEUR(fs),
		"findings":           fs,
	})
}
