# Running mlatd beside your mlat-server

A trial does not replace anything. mlatd runs as a second MLAT server on a
different port; feeders add one extra output and keep feeding everything
they already feed. Rollback is one container stop.

## 1. Start mlatd

```sh
git clone https://github.com/flightportrait/mlatd && cd mlatd
docker build -t mlatd .
docker run -d --name mlatd -v $PWD/work:/work \
    -p <bind-ip>:31090:31090 -p 127.0.0.1:31003:31003 mlatd \
    --client-listen 0.0.0.0:31090 \
    --basestation-listen 0.0.0.0:31003 \
    --work-dir /work \
    --self-truth-csv /work/selftruth.csv
```

The client port receives receiver coordinates; bind it the same way you
bind your mlat-server port. Memory: expect tens of megabytes; one instance
handles the whole network (internal geographic shards, `--shards` auto).

## 2. Point feeders at it

Each participating feeder adds one line (ultrafeeder syntax):

```
mlat,<your-host>,31090,uuid=<station-uuid>
```

Any mlat-client version works; the wire protocol, selective traffic, and
zlib2 compression (both directions) are the same as mlat-server's. Four
or more feeders with overlapping coverage produce positions.

## 3. What to watch

- Stdout: one statistics line every 10 s
  (`rx= sync_obs= solved= rejected=`).
- `work/sync.json`: mlat-server's format; existing sync dashboards read
  it unchanged.
- The SBS port (31003): feed it to a readsb instance the same way you
  ingest mlat-server results.
- `work/selftruth.csv`: mlatd also multilaterates ADS-B aircraft and
  compares each fix with the position the aircraft transmitted. This is
  live accuracy measurement without ground truth
  (rows: t,icao,err_m,est_m,n). Calibration on reference data: the
  estimate reads ~1.3-1.4x above the true error (reported p50 131 m
  where external truth measured 92 m); treat it as a conservative
  upper bound.

## 4. The rigorous comparison, when you want it

mlat-bench, our replay harness (publication pending), replays identical
traffic to two servers and scores both. Its `record` subcommand is a
transparent proxy: feeders connect to it, it forwards to your existing
mlat-server unchanged and writes a capture. `replay` then feeds that
capture to your mlat-server and to mlatd; `score` and `diff` compare.
Ten minutes of traffic decides. Before sharing a capture with anyone,
run `fuzz` on it: receiver coordinates identify homes.

## Known gaps

UDP transport, the filtered-basestation listener, and `--status-interval`
are not implemented; results return to clients in the "old" format only.
If the trial needs one of these, say so.
