//go:build integration

// Integration test for the live collector against a real broker (Redpanda,
// Kafka-API compatible). Gated behind the `integration` build tag so the
// default `go test ./...` stays fast and needs no Docker:
//
//	go test -tags=integration ./internal/collect/...
//
// It creates topics on a throwaway Redpanda container, runs the read-only
// collector, and asserts a sane snapshot plus that the rules fire. If Docker
// isn't available in the environment, the test skips gracefully rather than
// failing.
package collect

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/brodsh/brod-cli/internal/cost"
	"github.com/brodsh/brod-cli/internal/kafka"
	"github.com/brodsh/brod-cli/internal/rules"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuild_AgainstRedpanda(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		// No Docker / can't pull image => skip, don't fail.
		if isDockerUnavailable(err) {
			t.Skipf("skipping integration test: container could not start (%v)", err)
		}
		t.Fatalf("starting redpanda: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(container)
	})

	seed, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}

	// --- Seed the cluster with topics via a throwaway admin client.
	// (This setup uses kadm.CreateTopics directly in TEST code; the read-only
	// guard forbids consume entrypoints in the package, not admin writes here.)
	seedTopics(t, ctx, seed)

	// --- Run the read-only collector.
	client, err := kafka.Connect(ctx, kafka.Config{Bootstrap: []string{seed}})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	snap, err := Build(ctx, client, Options{
		ClusterName:  "it-cluster",
		Provider:     "self",
		Now:          time.Now().UTC(),
		SampleWindow: 0, // single-shot; throughput estimation needs no broker here
	})
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}

	// --- Assert a sane snapshot.
	byName := map[string]rules.Topic{}
	for _, tp := range snap.Topics {
		byName[tp.Name] = tp
	}
	wasteful, ok := byName["wasteful"]
	if !ok {
		t.Fatalf("topic 'wasteful' missing from snapshot; got %v", snap.Topics)
	}
	// 16 partitions, RF 1 on the single-node container.
	if wasteful.Partitions != 16 {
		t.Errorf("wasteful partitions = %d, want 16", wasteful.Partitions)
	}
	if wasteful.ReplicationFactor != 1 {
		t.Errorf("wasteful RF = %d, want 1", wasteful.ReplicationFactor)
	}
	if wasteful.CompressionType != "none" {
		t.Errorf("wasteful compression = %q, want none", wasteful.CompressionType)
	}
	if wasteful.MaxConsumerLagMs != -1 {
		t.Errorf("MaxConsumerLagMs = %d, want -1", wasteful.MaxConsumerLagMs)
	}
	if len(snap.Brokers) == 0 {
		t.Error("snapshot has no brokers")
	}

	// --- Assert the rules fire. R4 (over-partitioning) triggers deterministically
	// on the 16-partition 'wasteful' topic regardless of throughput, so the IT is
	// stable on an idle single-node cluster.
	cm := cost.CostModel{Provider: "self", MonthlyCostEUR: 1000, Weights: cost.DefaultWeights()}
	cfg := rules.DefaultConfig(time.Now().UTC())
	findings := rules.Evaluate(rules.SnapshotHistory{snap}, cm, cfg)
	if len(findings) == 0 {
		t.Fatal("expected at least one finding from the collected snapshot, got none")
	}
	for _, f := range findings {
		if f.Basis == "" {
			t.Errorf("finding %s has empty basis (pillar: every € carries a labeled basis)", f.RuleID)
		}
	}
	t.Logf("collected %d topics, %d brokers, %d findings", len(snap.Topics), len(snap.Brokers), len(findings))
}

// seedTopics creates a couple of topics with deliberately wasteful configs.
func seedTopics(t *testing.T, ctx context.Context, seed string) {
	t.Helper()
	kcl, err := kgo.NewClient(kgo.SeedBrokers(seed))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer kcl.Close()
	adm := kadm.NewClient(kcl)

	// 'wasteful': 16 partitions, RF 1, uncompressed — over-partitioned, so R4
	// fires deterministically even with no traffic.
	resp, err := adm.CreateTopic(ctx, 16, 1, map[string]*string{
		"compression.type": strp("none"),
		"retention.ms":     strp("604800000"),
	}, "wasteful")
	if err != nil || resp.Err != nil {
		t.Fatalf("create topic wasteful: %v / %v", err, resp.Err)
	}
	resp2, err := adm.CreateTopic(ctx, 1, 1, map[string]*string{
		"compression.type": strp("zstd"),
	}, "tidy")
	if err != nil || resp2.Err != nil {
		t.Fatalf("create topic tidy: %v / %v", err, resp2.Err)
	}
}

// isDockerUnavailable best-effort detects "no docker daemon" style errors so the
// test skips instead of failing in CI environments without Docker.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, s := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"rootless Docker not found",
		"failed to find a viable Docker",
		"no such host",
		"connection refused",
		"permission denied",
	} {
		if strings.Contains(strings.ToLower(msg), strings.ToLower(s)) {
			return true
		}
	}
	return errors.Is(err, context.DeadlineExceeded)
}
