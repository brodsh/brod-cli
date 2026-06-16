# brod — read-only Kafka FinOps CLI

`brod` is the open-source, MIT-licensed funnel for [brod.sh](https://brod.sh). It runs
the **static-capable subset of the shared rules engine** over a metadata snapshot of your
Apache Kafka cluster and prints a **euro-ranked waste report**.

It embodies the three pillars:

1. **Read-only by design** — `brod` only ever reads a snapshot file. It cannot connect to,
   let alone modify, a cluster.
2. **Metadata only** — the snapshot carries topic / consumer-group / broker metadata. Never
   message payloads. (Enforced by a test that fails the build if a consume/fetch path appears.)
3. **Euros, not metrics** — every finding is a € figure with a labeled basis
   (assumption / approximation / derived). Estimates are never presented as measurements.

## Install

```sh
curl -fsSL brod.sh | sh
```

## Use

```sh
# Try it on the built-in demo snapshot:
brod scan

# Scan a LIVE cluster (read-only — Admin/Describe/Metadata only, never payloads):
brod scan --bootstrap broker1:9092,broker2:9092 --cost 2000 --provider msk

# …with TLS + SASL (creds via flags or BROD_KAFKA_USER / BROD_KAFKA_PASS):
brod scan --bootstrap broker:9093 --tls --sasl-mechanism scram-sha-256 --user me

# Write a snapshot to inspect/share, then scan it offline:
brod collect --bootstrap broker:9092 --out my-cluster.json
brod scan --snapshot my-cluster.json --cost 2000 --provider msk --verbose

# Attribute the bill across topics / teams (40/40/20 model):
brod cost --snapshot my-cluster.json --cost 2000 \
  --team 'orders-.*=Checkout' --team '.*-analytics=Data'

# See the input format / machine-readable output:
brod sample > my-cluster.json
brod scan --snapshot my-cluster.json --json
```

> **Read-only, enforced.** Live mode issues only Admin/Describe/Metadata/ListOffsets/OffsetFetch —
> there is no produce or consume path, so brod *cannot* read message payloads (a build-time guard
> test fails if one is ever added). Add `--require-readonly` to fail closed on an over-privileged
> credential.

| Flag | Meaning |
|---|---|
| `--snapshot FILE` | snapshot JSON (`-` for stdin); omitted → built-in demo |
| `--cost EUR` | cluster monthly cost → **derived** unit rates instead of assumed presets |
| `--provider P` | `confluent` \| `msk` \| `self` (default: from snapshot, else self) |
| `--json` | emit findings as JSON |
| `--verbose` | show the exact config diff (remediation) per finding |

## Rules the CLI runs

The CLI runs the single-snapshot subset; the SaaS runs the full, history-aware set and books
**measured** savings after a fix lands.

| Rule | What | CLI coverage |
|---|---|---|
| **R1** | Dead topic | partial (single-snapshot approximation, labeled) |
| **R2** | Zombie consumer group | hygiene (€0); needs a last-commit age |
| **R3** | Retention overkill | partial (needs a lag-time horizon in the snapshot) |
| **R4** | Over-partitioning | full — severity `plan` (never a one-click PR) |
| **R5** | Missing compression | full |
| **R6** | Replication mismatch (non-prod RF) | full |
| **R7** | Partition skew | reliability (€0); from per-partition sizes |
| **R8** | Orphaned Streams internal topics (`-changelog`/`-repartition`) | full |

The same `internal/rules` package powers both the CLI and the brod SaaS — verbatim. It is
**pure** (no I/O, no clock; evaluation time is injected) and that purity is enforced by a
test, which is what lets the two share it.

Every report's last line points back to [brod.sh](https://brod.sh) for continuous monitoring
+ per-team cost attribution.

---

<sub>This repository is the published open-source CLI. It is maintained as part of the brod
project; issues and PRs welcome.</sub>
