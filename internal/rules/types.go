// Package rules is the heart of Brod: a PURE evaluation of Kafka metadata
// snapshots into euro-quantified waste Findings.
//
// PURITY CONTRACT (enforced by purity_test.go):
//   - no network, no DB, no filesystem, no os access
//   - no clock reads (time.Now / time.Since) — the evaluation time is INJECTED
//     via RuleConfig.Now
//   - same inputs -> same outputs
//
// This is the architectural lever that lets the open-source `brod` CLI and the
// SaaS backend share this exact code. The CLI runs the static-capable subset
// (R4, R5, R6 fully; R1, R3 as single-snapshot approximations). Continuous
// rules needing history are SaaS-only.
//
// PILLAR: metadata only. Nothing here ever touches message payloads — the
// Snapshot type carries topic/group/broker metadata exclusively.
package rules

import "time"

// Severity ranks a finding. `plan` means it needs a migration and can never be
// shipped as a one-click PR (e.g. partition count can't shrink in place).
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
	SeverityPlan     Severity = "plan"
)

// Confidence reflects how trustworthy the euro figure is. Single-snapshot
// approximations are `low`; figures only become `high`/measured once a fix is
// applied and re-measured (SaaS-side).
type Confidence string

const (
	ConfidenceLow    Confidence = "low"
	ConfidenceMedium Confidence = "medium"
	ConfidenceHigh   Confidence = "high"
)

// Snapshot is one point-in-time view of a cluster's metadata. The CLI evaluates
// a single Snapshot; the SaaS passes a history (see SnapshotHistory).
type Snapshot struct {
	TakenAt time.Time      `json:"taken_at"`
	Cluster ClusterInfo    `json:"cluster"`
	Topics  []Topic        `json:"topics"`
	Groups  []ConsumerGrp  `json:"groups"`
	Brokers []Broker       `json:"brokers"`
}

// SnapshotHistory is an ordered series (oldest first). The CLI supplies exactly
// one element; the SaaS supplies a trailing window for the history-dependent
// rules.
type SnapshotHistory []Snapshot

// Latest returns the most recent snapshot, or false if the history is empty.
func (h SnapshotHistory) Latest() (Snapshot, bool) {
	if len(h) == 0 {
		return Snapshot{}, false
	}
	return h[len(h)-1], true
}

// ClusterInfo carries cluster-level identity. Provider drives cost presets.
type ClusterInfo struct {
	Name     string `json:"name"`
	Provider string `json:"provider"` // "confluent" | "msk" | "self"
}

// Topic is metadata for one topic. RetainedBytes is summed across partitions.
// Throughput figures are 30d averages; Source labels how they were obtained so
// downstream euros can be honestly basis-labeled.
type Topic struct {
	Name              string  `json:"name"`
	Partitions        int     `json:"partitions"`
	ReplicationFactor int     `json:"replication_factor"`
	RetentionMs       int64   `json:"retention_ms"` // -1 = infinite
	CompressionType   string  `json:"compression_type"`
	CleanupPolicy     string  `json:"cleanup_policy"`
	RetainedBytes     int64   `json:"retained_bytes"`
	BytesInPerSec     float64 `json:"bytes_in_per_sec"`
	BytesOutPerSec    float64 `json:"bytes_out_per_sec"`
	ThroughputSource  string  `json:"throughput_source"` // "metrics" | "estimated" | "none"
	PartitionBytes    []int64 `json:"partition_bytes,omitempty"`
	EarliestOffset    int64   `json:"earliest_offset"`
	LatestOffset      int64   `json:"latest_offset"`
	// MaxConsumerLagMs is the max lag-time across groups consuming this topic,
	// in ms; -1 if unknown (then history-dependent rules skip the topic).
	MaxConsumerLagMs int64 `json:"max_consumer_lag_ms"`
	// ActiveConsumers is the count of non-empty groups consuming this topic.
	ActiveConsumers int `json:"active_consumers"`
}

// ConsumerGrp is consumer-group metadata. State follows Kafka group states
// (Stable, Empty, Dead, ...). LastCommitAgeDays is -1 when unknown.
type ConsumerGrp struct {
	GroupID           string `json:"group_id"`
	State             string `json:"state"`
	LastCommitAgeDays int    `json:"last_commit_age_days"`
}

// Broker is broker metadata. LogDirBytes is the broker's on-disk log size.
type Broker struct {
	ID         int   `json:"id"`
	LogDirBytes int64 `json:"log_dir_bytes"`
}

// Remediation is the exact, reviewable change a fix would ship as a PR. OneClick
// is false for `plan` findings (they require a migration the customer drives).
type Remediation struct {
	Summary         string `json:"summary"`
	KafkaConfigsCmd string `json:"kafka_configs_cmd,omitempty"`
	TerraformDiff   string `json:"terraform_diff,omitempty"`
	StrimziYAML     string `json:"strimzi_yaml,omitempty"`
	OneClick        bool   `json:"one_click"`
}

// Finding is a single euro-quantified waste detection. Basis is mandatory and
// carries the formula + an honesty label (estimate / assumption / measured) —
// PILLAR: never present an estimate as a measurement.
type Finding struct {
	RuleID       string         `json:"rule_id"`
	RuleVersion  int            `json:"rule_version"`
	Title        string         `json:"title"`
	Severity     Severity       `json:"severity"`
	ResourceKey  string         `json:"resource_key"`
	Evidence     map[string]any `json:"evidence"`
	EstSavingEUR float64        `json:"est_saving_eur"`
	Basis        string         `json:"basis"`
	Confidence   Confidence     `json:"confidence"`
	Remediation  Remediation    `json:"remediation"`
}
