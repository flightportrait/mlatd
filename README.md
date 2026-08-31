# mlatd

An MLAT (multilateration) server for Mode S aircraft tracking. It speaks
the wire protocol of [mlat-client] — the client every ADS-B feeder image
already ships — and is built as a drop-in replacement for
[wiedehopf/mlat-server]: same handshake, same compression modes, same
selective-traffic behavior, same result return, same `sync.json`, same
flag names where it matters. Point existing clients at it and they sync.

Written in Rust. One binary, no runtime dependencies, small enough to
audit in an afternoon.

**Status: pre-release.** Proven against a replay benchmark and real
recorded traffic (below), not yet by long production deployments. The
repo is private while that last step happens.

[mlat-client]: https://github.com/mutability/mlat-client
[wiedehopf/mlat-server]: https://github.com/wiedehopf/mlat-server

## Why it exists

The Python mlat-server has run the open-skies world for a decade and its
multilateration heuristics are genuinely good — mlatd ports many of them.
What it can't do is scale: pairwise clock sync is O(n²) in stored state,
solving is single-process, and large aggregators cope by hand-partitioning
receivers across instances.

mlatd keeps the proven estimation behavior and rebuilds the machinery:
lock-free geographic sharding inside one process, constant-memory clock
models, and message-passing instead of shared state.

Measured (2026-09-01, [mlat-bench] replay of identical captured traffic;
LocaRDS = 316 real receivers, 10 minutes, ground truth by holdout):

| | mlat-server (patched¹) | mlatd |
|---|---|---|
| positions produced | 7,381 | 35,769 |
| horizontal error p50 / p99 | 135 m / 2,069 m | 94 m / 901 m |
| junk rate (bad-address fixes) | 11.6 % | 0.18 % |
| CPU / RSS | 54 % / 775 MB | 14 % / 55 MB |

Scale: an 800-receiver synthetic world replayed at 4× compression
(3,200-receiver-equivalent load) holds p50 48 m — unchanged from
real-time — at ~3.7 cores and 72 MB RSS. Extrapolated, a global network
of ~10k feeders fits one commodity box; that remains extrapolation until
measured.

¹ Patched: upstream crashes on this dataset (NaN variance →
`ValueError`, drops all pending work each cycle); we fixed it in the
container to get fair numbers, and reported the bug upstream. Every
number above is reproducible from the bench repo: same capture, both
servers, one command each.

[mlat-bench]: https://github.com/yoanntlm/mlat-bench

## Quickstart

```sh
docker build -t mlatd . && docker run --rm -p 31090:31090 mlatd \
    --client-listen 0.0.0.0:31090 --basestation-listen 0.0.0.0:31003
# or, with a toolchain:
cargo run --release -p mlatd -- --client-listen 31090
```

Feeder side (any readsb/ultrafeeder image):

```
mlat,<your-host>,31090,uuid=<station-uuid>
```

`compose.example.yml` is the long-running wiring, including the bind
doctrine: the client port carries receiver coordinates — keep it on a
private interface or behind deliberate ingress.

## Flags

| flag | default | |
|---|---|---|
| `--client-listen` | — | mlat-client port; `[host:]port`, bare port binds 0.0.0.0 (oracle-compatible) |
| `--basestation-listen` | off | SBS/BaseStation output — what readsb ingests |
| `--work-dir` | off | writes `sync.json` every 15 s in mlat-server's shape, for existing monitoring |
| `--write-csv` | off | results CSV, mlat-server column format |
| `--self-truth-csv` | off | also multilaterate ADS-B frames and score each fix against the aircraft's own broadcast position — live accuracy without external truth (field-calibrated: reports ~126 m where holdout truth says 94–101 m) |
| `--shards` | auto (cores−2) | independent geographic slices within the process |
| `--shard-cell-deg` / `--shard-cap` | 5.0 / 64 | partition geometry: 5° suits continental networks, 2° dense metros; cap spills growth to neighbors |
| `--write-filtered-csv` | off | alpha-beta-smoothed results (experimental; benched *worse* on real traffic — measure before trusting) |
| `--time-scale` / `--group-window-ms` | 1 / 900 | bench-replay support; leave alone in production |

## Compatibility surface

Works today, verified against real mlat-client end to end: JSON
handshake (`compress` none/zlib/zlib2), selective traffic
(`start_sending`/`stop_sending`), sync + mlat messages for every
documented clock type, result return in the client's expected format,
SBS output, `sync.json`, oracle flag aliases.

Not there yet, honestly: UDP transport, the filtered-basestation
listener, `--status-interval` stats output, Kalman-smoothed result
columns (our smoothing experiment lost to raw output on real data and
ships off by default). If your deployment needs one of these, say so —
they're scoped, not hard.

Migrating from mlat-server: [docs/MIGRATION.md](docs/MIGRATION.md).

## Design in one paragraph

Receivers are assigned to geographic shards (region-growing over
lat/lon cells, capacity-capped); each shard is a single task owning its
receivers, clock pairs, and aircraft — no locks anywhere. Clock sync is
per-receiver-pair online regression with exponential decay (constant
memory per pair), an outlier gate, and automatic reset on clock jumps.
Solving is Gauss-Newton on (lat, lon, t) with covariance-based error
estimates, per-cluster reference election, and the accuracy gates ported
from mlat-server's decade of field tuning. One output task dedupes
shard-boundary aircraft and fans out to CSV/SBS/clients.

## Development

Until public launch, development happens in [mlat-bench], the replay
harness this server was built against — every heuristic here was
accepted or rejected by measurement there, and the rejected ones are
kept as commented evidence. The `crates/` tree in this repo is
published from there by `tools/publish_mlatd.sh`; packaging and docs
are authored here. At launch the crates move here for good and the
bench consumes them as a dependency.

## Credit and license

mlatd is an independent implementation, but it stands on
[mutability/mlat-server] by Oliver Jowett and the maintained
[wiedehopf/mlat-server] fork: the protocol, the clock-type table, and
several estimation heuristics were learned from their published work
and a decade of their production tuning. Thank you.

No code is derived from either (both AGPLv3); mlatd is licensed
MIT OR Apache-2.0, at your option.

[mutability/mlat-server]: https://github.com/mutability/mlat-server
