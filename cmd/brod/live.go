package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/brodsh/brod-cli/internal/collect"
	"github.com/brodsh/brod-cli/internal/kafka"
	"github.com/brodsh/brod-cli/internal/rules"
)

// readOnlyBanner is printed before every live cluster connection. PILLAR
// (read-only by design): it states, in plain terms, the only request types brod
// will ever issue.
const readOnlyBanner = "brod is read-only: it issues only Admin/Describe/Metadata/ListOffsets/OffsetFetch — no produce, no consume, no message payloads ever."

// connectFlags are the cluster-connection flags shared by `collect` and
// `scan --bootstrap`.
type connectFlags struct {
	bootstrap       *string
	tls             *bool
	saslMechanism   *string
	user            *string
	pass            *string
	clusterName     *string
	sampleWindow    *time.Duration
	requireReadOnly *bool
	includeInternal *bool
}

func registerConnectFlags(fs *flag.FlagSet) connectFlags {
	return connectFlags{
		bootstrap:       fs.String("bootstrap", "", "comma-separated bootstrap servers (host:port)"),
		tls:             fs.Bool("tls", false, "use TLS for the broker connection"),
		saslMechanism:   fs.String("sasl-mechanism", "", "plain | scram-sha-256 | scram-sha-512 (default: none)"),
		user:            fs.String("user", "", "SASL username (or env BROD_KAFKA_USER)"),
		pass:            fs.String("pass", "", "SASL password (or env BROD_KAFKA_PASS)"),
		clusterName:     fs.String("cluster-name", "", "label for the snapshot (default: first bootstrap host)"),
		sampleWindow:    fs.Duration("sample-window", 30*time.Second, "throughput sampling window; 0 = single-shot (no throughput estimate)"),
		requireReadOnly: fs.Bool("require-readonly", false, "fail closed if the credential has detectable write/topic-READ ACLs"),
		includeInternal: fs.Bool("include-internal", false, "include internal topics (e.g. __consumer_offsets)"),
	}
}

// connectAndCollect runs the full live path: connect read-only, optionally
// verify the credential, then build a snapshot. Credentials are never printed.
func connectAndCollect(ctx context.Context, cf connectFlags) (rules.Snapshot, error) {
	if strings.TrimSpace(*cf.bootstrap) == "" {
		return rules.Snapshot{}, fmt.Errorf("--bootstrap is required (comma-separated host:port)")
	}

	// Banner first — visible before any network contact.
	fmt.Fprintln(os.Stderr, "  "+readOnlyBanner)

	cfg := kafka.Config{
		Bootstrap: splitCSV(*cf.bootstrap),
		TLS:       *cf.tls,
		SASL: kafka.SASLConfig{
			Mechanism: kafka.SASLMechanism(*cf.saslMechanism),
			User:      *cf.user,
			Pass:      *cf.pass,
		},
	}
	client, err := kafka.Connect(ctx, cfg)
	if err != nil {
		return rules.Snapshot{}, err
	}
	defer client.Close()

	if *cf.requireReadOnly {
		if err := enforceReadOnly(ctx, client); err != nil {
			return rules.Snapshot{}, err
		}
	}

	clusterLabel := *cf.clusterName
	if clusterLabel == "" {
		clusterLabel = firstHost(*cf.bootstrap)
	}

	snap, err := collect.Build(ctx, client, collect.Options{
		ClusterName:     clusterLabel,
		Provider:        "", // provider is applied at render/cost time below
		Now:             time.Now().UTC(),
		SampleWindow:    *cf.sampleWindow,
		IncludeInternal: *cf.includeInternal,
	})
	if err != nil {
		return rules.Snapshot{}, err
	}
	return snap, nil
}

// enforceReadOnly runs the best-effort ACL probe (A-5). On detected violations
// it returns an error carrying a copy-pasteable minimal read-only ACL. If the
// cluster doesn't expose ACLs, it prints an honest "could not verify" note and
// proceeds (the banner already states intent; full enforcement is SaaS).
func enforceReadOnly(ctx context.Context, c *kafka.Client) error {
	chk, err := c.CheckReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("read-only check failed: %w", err)
	}
	if !chk.Supported {
		fmt.Fprintln(os.Stderr, "  note: cluster did not expose ACLs (no authorizer or insufficient Describe) — could not verify read-only; proceeding on the read-only banner's guarantee. Full enforcement is a brod.sh (SaaS) feature.")
		return nil
	}
	if len(chk.Violations) > 0 {
		var b strings.Builder
		fmt.Fprintln(&b, "credential is NOT read-only — refusing to proceed (--require-readonly). Detected grants:")
		for _, v := range chk.Violations {
			fmt.Fprintf(&b, "    - %s\n", v)
		}
		fmt.Fprint(&b, minimalReadOnlyACL())
		return fmt.Errorf("%s", b.String())
	}
	fmt.Fprintln(os.Stderr, "  read-only check: passed (no write/topic-READ grants detected for this principal).")
	return nil
}

// minimalReadOnlyACL returns a copy-pasteable kafka-acls command granting only
// what brod needs: cluster + topic + group DESCRIBE, and group DESCRIBE for
// committed-offset reads. No READ on topics (that would permit consuming).
func minimalReadOnlyACL() string {
	return strings.TrimLeft(`
  Grant a minimal read-only principal instead (adjust --bootstrap-server and User:brod):

    kafka-acls --bootstrap-server <host:9092> \
      --add --allow-principal User:brod \
      --operation Describe --operation DescribeConfigs \
      --cluster --topic '*' --group '*'

  This permits metadata/describe/offset reads only — no Read (consume), no Write, no Alter.
`, "\n")
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func firstHost(bootstrap string) string {
	hosts := splitCSV(bootstrap)
	if len(hosts) == 0 {
		return "kafka-cluster"
	}
	// Strip the :port for a cleaner label.
	if i := strings.LastIndex(hosts[0], ":"); i > 0 {
		return hosts[0][:i]
	}
	return hosts[0]
}

// cmdCollect implements `brod collect --bootstrap ... [--out file]`: connect,
// build a snapshot, write it as JSON (round-trips with `brod scan --snapshot`).
func cmdCollect(argv []string) error {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	cf := registerConnectFlags(fs)
	out := fs.String("out", "", "write snapshot JSON to this file ('-' or empty = stdout)")
	if err := fs.Parse(argv); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), collectTimeout(*cf.sampleWindow))
	defer cancel()

	snap, err := connectAndCollect(ctx, cf)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding snapshot: %w", err)
	}
	data = append(data, '\n')

	if *out == "" || *out == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		return fmt.Errorf("writing snapshot: %w", err)
	}
	fmt.Fprintf(os.Stderr, "  wrote snapshot for %d topics to %s\n", len(snap.Topics), *out)
	return nil
}

// collectTimeout gives the sample window room plus a fixed budget for the admin
// round-trips.
func collectTimeout(window time.Duration) time.Duration {
	return window + 60*time.Second
}
