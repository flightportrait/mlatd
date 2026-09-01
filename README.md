# mlatd

mlatd is an MLAT server for Mode S multilateration. It implements the
wire protocol of [mlat-client]. It is a compatible replacement for
[wiedehopf/mlat-server]: the handshake, the compression modes, the
selective traffic, the result messages, the `sync.json` file, and the
flag names are the same. The server is one Rust binary.

Status: pre-release. We benchmark mlatd against mlat-server with
replayed real traffic in [mlat-bench]. There is no long production
deployment yet.

[mlat-client]: https://github.com/mutability/mlat-client
[wiedehopf/mlat-server]: https://github.com/wiedehopf/mlat-server
[mlat-bench]: https://github.com/yoanntlm/mlat-bench

## Design

The server puts each receiver in a geographic shard. Each shard is one
task. A shard owns its receivers, its clock pairs, and its aircraft.
Shards do not share memory. Clock synchronization is an online
regression for each receiver pair, with exponential decay; the state
for each pair has a constant size. A gate rejects bad synchronization
data. After repeated rejections, the server resets the pair. This
corrects clock jumps. The solver does Gauss-Newton iteration on
(lat, lon, t). The error estimate comes from the solution covariance.
Each cluster of receivers elects its own reference receiver.

## Run

```sh
docker build -t mlatd .
docker run --rm -p 31090:31090 mlatd \
    --client-listen 0.0.0.0:31090 --basestation-listen 0.0.0.0:31003
```

Add this line to the feeder configuration (readsb or ultrafeeder):

```
mlat,<host>,31090,uuid=<station-uuid>
```

Use `compose.example.yml` to run mlatd as a service. The client port
receives receiver coordinates. Bind the port to a private interface.

## Flags

| flag | default | |
|---|---|---|
| `--client-listen` | — | mlat-client port; `[host:]port`; a bare port binds 0.0.0.0 |
| `--basestation-listen` | off | SBS/BaseStation output |
| `--work-dir` | off | writes `sync.json` every 15 s, in the mlat-server format |
| `--write-csv` | off | results CSV, mlat-server column format |
| `--self-truth-csv` | off | also multilaterates ADS-B frames and scores each fix against the position the aircraft transmitted |
| `--shards` | auto (cores−2) | number of geographic shards in the process |
| `--shard-cell-deg` / `--shard-cap` | 5.0 / 64 | partition cell size and receiver capacity for each shard |
| `--write-filtered-csv` | off | alpha-beta-smoothed results (experimental) |
| `--time-scale` / `--group-window-ms` | 1 / 900 | bench-replay support; do not change in production |

## Compatibility

These functions are tested against the real mlat-client, end to end:
the JSON handshake with `compress` none, zlib, and zlib2; selective
traffic; sync and mlat messages for all documented clock types; result
return; SBS output; `sync.json`; the mlat-server flag aliases.

The server also emits the per-receiver stats push (`return_stats`),
which mlat-client turns into its `--stats-json` file.

These functions are not implemented: UDP transport, the
filtered-basestation listener, `--status-interval`, Kalman result
columns.

To migrate from mlat-server, read [docs/MIGRATION.md](docs/MIGRATION.md).

## Development

We develop mlatd against [mlat-bench], a replay harness that scores a
server's output against known ground truth over the real wire
protocol. The benchmark results and the method are in that repository.
The `crates/` directory here is a copy published from that workspace;
the packaging and the documentation are written in this repository.

## Credit

mlatd exists because of the work of Oliver Jowett (mutability), who
designed the MLAT protocol and wrote the original [mlat-server] and
[mlat-client], and of wiedehopf, who maintains the fork that the open
ADS-B networks run today. The wire protocol, the clock-type table, and
some estimation heuristics in mlatd follow the behavior of their
software and its decade of production tuning.

[mlat-server]: https://github.com/mutability/mlat-server

## License

- `crates/mlatd`, the server: **AGPL-3.0-or-later**
  ([LICENSE-AGPL](LICENSE-AGPL)). This is the license of mlat-server.
  You can run and change the software freely. If you serve a changed
  mlatd to users on a network, you must give them the changed source.
- `crates/mb-core`, `crates/mb-modes`, `crates/mb-proto` — geodesy,
  Mode S encoding, the wire protocol: **MIT**
  ([LICENSE-MIT](LICENSE-MIT)).
