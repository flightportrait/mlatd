# mlatd

An MLAT server for Mode S multilateration. It speaks the [mlat-client]
wire protocol and is a drop-in replacement for [wiedehopf/mlat-server]:
same handshake, compression modes, selective traffic, result return,
`sync.json`, and flag names. Rust, one binary.

Pre-release: benchmarked against mlat-server on replayed real traffic
in [mlat-bench]; no long-running production deployment yet.

[mlat-client]: https://github.com/mutability/mlat-client
[wiedehopf/mlat-server]: https://github.com/wiedehopf/mlat-server
[mlat-bench]: https://github.com/yoanntlm/mlat-bench

## Design

Receivers are assigned to geographic shards; each shard is one task
owning its receivers, clock pairs, and aircraft, so there is no shared
state and no locking. Clock sync is per-pair online regression with
exponential decay; state per pair is constant-size. An outlier gate
rejects bad sync observations, and repeated rejections reset the pair
after a clock jump. Positions are solved by Gauss-Newton iteration on
(lat, lon, t), with error estimates from the solution covariance and a
reference receiver elected per cluster.

## Run

```sh
docker build -t mlatd . && docker run --rm -p 31090:31090 mlatd \
    --client-listen 0.0.0.0:31090 --basestation-listen 0.0.0.0:31003
```

Feeder side, any readsb/ultrafeeder image:

```
mlat,<host>,31090,uuid=<station-uuid>
```

`compose.example.yml` runs it as a service. The client port carries
receiver coordinates; bind it to a private interface.

## Flags

| flag | default | |
|---|---|---|
| `--client-listen` | — | mlat-client port; `[host:]port`, bare port binds 0.0.0.0 |
| `--basestation-listen` | off | SBS/BaseStation output |
| `--work-dir` | off | writes `sync.json` every 15 s, in mlat-server's format |
| `--write-csv` | off | results CSV, mlat-server column format |
| `--self-truth-csv` | off | also multilaterate ADS-B frames and score each fix against the aircraft's own broadcast position |
| `--shards` | auto (cores−2) | geographic slices within the process |
| `--shard-cell-deg` / `--shard-cap` | 5.0 / 64 | partition cell size and per-shard receiver capacity |
| `--write-filtered-csv` | off | alpha-beta-smoothed results (experimental) |
| `--time-scale` / `--group-window-ms` | 1 / 900 | bench-replay support; leave alone in production |

## Compatibility

Verified against real mlat-client end to end: JSON handshake
(`compress` none/zlib/zlib2), selective traffic, sync and mlat messages
for every documented clock type, result return, SBS output,
`sync.json`, mlat-server flag aliases.

Not implemented: UDP transport, the filtered-basestation listener,
`--status-interval`, Kalman result columns.

Migrating from mlat-server: [docs/MIGRATION.md](docs/MIGRATION.md).

## Development

Developed against [mlat-bench], a replay harness that scores servers
against known ground truth; benchmark results and methodology live
there. Until public launch, `crates/` here is published from that
workspace by its `tools/publish_mlatd.sh`; packaging and docs are
authored in this repository.

## License

Two licenses, by crate:

- `crates/mlatd`, the server, is **AGPL-3.0-or-later**
  ([LICENSE-AGPL](LICENSE-AGPL)) — the same license as mlat-server.
  You can run and modify it freely; if you serve a modified mlatd to
  users over a network, you must offer them your modified source.
- `crates/mb-core`, `crates/mb-modes`, `crates/mb-proto` — geodesy,
  Mode S encoding, the wire protocol — are **MIT OR Apache-2.0**
  ([LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE)).

mlatd contains no code from [mutability/mlat-server] (Oliver Jowett)
or the [wiedehopf/mlat-server] fork; it is written from scratch in
Rust. The wire protocol, the clock-type table, and several estimation
heuristics follow their published behavior.

[mutability/mlat-server]: https://github.com/mutability/mlat-server
