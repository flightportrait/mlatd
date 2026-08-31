# Migrating from wiedehopf/mlat-server

Feeders need no change at all: mlat-client connects, handshakes, and
syncs against mlatd exactly as before, including zlib/zlib2 compression
and selective traffic. Migration is a server-side swap.

## Flag mapping

| mlat-server | mlatd | notes |
|---|---|---|
| `--client-listen [host:]tcp[:udp]` | `--client-listen [host:]tcp` | TCP only for now; a UDP port suffix is not accepted — drop it |
| `--work-dir DIR` | `--work-dir DIR` | mlatd writes only `sync.json` there (same shape, every 15 s); no other state, nothing to migrate |
| `--write-csv FILE` | `--write-csv FILE` | same column format; optional in mlatd |
| `--basestation-listen [host:]port` | `--basestation-listen [host:]port` | same SBS output |
| `--filtered-basestation-listen` | not yet | the SBS listener carries raw fixes |
| `--status-interval N` | not yet | a periodic stats line goes to stdout instead |
| (Kalman result columns) | `--write-filtered-csv` | alpha-beta smoothing, experimental, off by default — it measured worse than raw output on real traffic |
| — | `--shards`, `--shard-cell-deg`, `--shard-cap` | internal partitioning; defaults suit a continental network. Replaces running multiple hand-partitioned instances |
| — | `--self-truth-csv` | live accuracy self-measurement via ADS-B holdout |

## Operational differences worth knowing

- **One instance replaces many.** The reason large deployments run
  several mlat-server processes — sync-state blowup and single-core
  solving — is handled internally by geographic shards. Start with one
  instance and the default partition dials.
- **Memory is flat.** Clock-pair state is constant-size with
  exponential decay; expect tens of MB where mlat-server used hundreds,
  and no growth over time (verified over a 1-hour soak, 200k results).
- **Results appear faster and more often.** mlatd resolves more groups
  from the same traffic. Downstream consumers that assumed
  mlat-server's output rate (rate limiting, dedupe windows) may need
  their assumptions rechecked.
- **`sync.json` keeps working** for dashboards that read it, but its
  contents come from mlatd's own pair models; absolute numbers differ
  from mlat-server's while meaning the same thing.
- **Clock handling**: `clock_reset`/jump detection is automatic per
  pair (8-reject reset); there is no per-receiver manual intervention.
- **Privacy**: as with mlat-server, receiver coordinates arrive in the
  handshake. Bind the client port accordingly (see
  `compose.example.yml`).

## Verifying the swap on your own traffic

The
[mlat-bench](https://github.com/yoanntlm/mlat-bench) `record` proxy
taps your live feeder traffic to a capture file; `replay` then feeds
that identical capture to your current mlat-server and to mlatd, and
`score`/`diff` compare the two runs. Ten minutes of your real traffic
is enough for a decision.
