# mlatd

An MLAT server for Mode S multilateration. It speaks the [mlat-client]
wire protocol and is a drop-in replacement for [wiedehopf/mlat-server]:
same handshake, compression modes, selective traffic, result return,
`sync.json`, and flag names. Rust, one binary.

Pre-release. Benchmarked against mlat-server on replayed real traffic
in [mlat-bench]; not yet proven by long production deployments.

[mlat-client]: https://github.com/mutability/mlat-client
[wiedehopf/mlat-server]: https://github.com/wiedehopf/mlat-server
[mlat-bench]: https://github.com/yoanntlm/mlat-bench

## Design

Receivers are assigned to geographic shards inside one process; each
shard is a single task owning its receivers, clock pairs, and aircraft,
with no shared state. Clock sync is per-pair online regression with
exponential decay — constant memory — plus an outlier gate and
automatic reset on clock jumps. Solving is Gauss-Newton on
(lat, lon, t) with covariance error estimates and per-cluster
reference election.

## Run

```sh
docker build -t mlatd . && docker run --rm -p 31090:31090 mlatd \
    --client-listen 0.0.0.0:31090 --basestation-listen 0.0.0.0:31003
```

Feeder side, any readsb/ultrafeeder image:

```
mlat,<host>,31090,uuid=<station-uuid>
```

`compose.example.yml` is the long-running wiring. The client port
carries receiver coordinates; bind it to a private interface.

## Flags

| flag | default | |
|---|---|---|
| `--client-listen` | — | mlat-client port; `[host:]port`, bare port binds 0.0.0.0 |
| `--basestation-listen` | off | SBS/BaseStation output |
| `--work-dir` | off | writes `sync.json` every 15 s, mlat-server's shape |
| `--write-csv` | off | results CSV, mlat-server column format |
| `--self-truth-csv` | off | also multilaterate ADS-B frames and score each fix against the aircraft's own broadcast position |
| `--shards` | auto (cores−2) | geographic slices within the process |
| `--shard-cell-deg` / `--shard-cap` | 5.0 / 64 | partition cell size and per-shard receiver capacity |
| `--write-filtered-csv` | off | alpha-beta-smoothed results (experimental) |
| `--time-scale` / `--group-window-ms` | 1 / 900 | bench-replay support; leave alone in production |

## Compatibility

Verified against real mlat-client end to end: JSON handshake
(`compress` none/zlib/zlib2), selective traffic, sync and mlat
messages for every documented clock type, result return, SBS output,
`sync.json`, oracle flag aliases.

Not implemented: UDP transport, the filtered-basestation listener,
`--status-interval`, Kalman result columns.

Migrating from mlat-server: [docs/MIGRATION.md](docs/MIGRATION.md).

## Development

Developed against [mlat-bench], a replay harness with ground truth by
construction; benchmark results and methodology live there. Until
public launch the `crates/` tree here is published from that workspace
by its `tools/publish_mlatd.sh`; packaging and docs are authored here.

## License and credit

MIT OR Apache-2.0. mlatd contains no code from
[mutability/mlat-server] (Oliver Jowett) or the wiedehopf fork, both
AGPLv3; it was written from scratch in Rust. The wire protocol, the
clock-type table, and several estimation heuristics follow their
published behavior.

[mutability/mlat-server]: https://github.com/mutability/mlat-server
