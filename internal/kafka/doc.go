// Package kafka is the read-only boundary to a customer's Kafka cluster.
//
// PILLAR (read-only by design) + PILLAR (metadata only): this package may ONLY
// ever expose Admin / Describe / Metrics calls. It must NEVER instantiate a
// consumer against customer topic data or issue a Fetch for message payloads.
//
// There is intentionally no client implementation yet (pre-validation phase).
// The CLI v0 reads metadata from a snapshot file, not from a live cluster. When
// a live read-only client is added here, it stays admin/describe/metrics only —
// enforced by guard_test.go, which fails the build if a Fetch/Consume path
// appears anywhere in the repo.
package kafka
