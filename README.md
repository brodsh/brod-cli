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

# Scan your own metadata snapshot:
brod scan --snapshot my-cluster.json --cost 2000 --provider msk --verbose

# See the input format:
brod sample > my-cluster.json

# Machine-readable:
brod scan --snapshot my-cluster.json --json
```

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
| **R3** | Retention overkill | partial (needs a lag-time horizon in the snapshot) |
| **R4** | Over-partitioning | full — severity `plan` (never a one-click PR) |
| **R5** | Missing compression | full |
| **R6** | Replication mismatch (non-prod RF) | full |

The same `internal/rules` package powers both the CLI and the brod SaaS — verbatim. It is
**pure** (no I/O, no clock; evaluation time is injected) and that purity is enforced by a
test, which is what lets the two share it.

Every report's last line points back to [brod.sh](https://brod.sh) for continuous monitoring
+ per-team cost attribution.

---

<sub>This repository is the published open-source CLI. It is maintained as part of the brod
project; issues and PRs welcome.</sub>
