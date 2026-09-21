# Migration from wiedehopf/mlat-server

Feeders do not need changes. mlat-client connects, handshakes, and
synchronizes with mlatd as before, with zlib/zlib2 compression and
selective traffic. The migration is a server-side swap.

## Flag mapping

| mlat-server | mlatd | notes |
|---|---|---|
| `--client-listen [host:]tcp[:udp]` | `--client-listen [host:]tcp` | TCP only. Remove a UDP port suffix. |
| `--work-dir DIR` | `--work-dir DIR` | `sync.json`, `clients.json`, `aircraft.json` in the same formats, every 15 s, written atomically; plus `partition.json`. There is no other state to migrate. |
| `--write-csv FILE` | `--write-csv FILE` | Same column format. Optional in mlatd. |
| `--basestation-listen [host:]port` | `--basestation-listen [host:]port` | Same SBS output; readsb pulls it with `--net-connector=<host>,<port>,sbs_in_mlat`. |
| `--basestation-connect host:port` | `--basestation-connect host:port` | Same: mlatd dials readsb (`--net-sbs-in-port`) and pushes results, reconnecting every 5 s. May repeat. |
| `--filtered-basestation-listen` / `--filtered-basestation-connect` | not available | The SBS outputs send unsmoothed fixes. Point the filtered consumer at the unfiltered flag. |
| `--status-interval N` | `--status-interval N` | Same: seconds between statistics lines, -1 disables. mlatd's line is `rx= sync_obs= solved= rejected=`. |
| (Kalman result columns) | `--write-filtered-csv` | Alpha-beta smoothing, experimental, off by default. |
| — | `--shards`, `--shard-cell-deg`, `--shard-cap` | Internal geographic partition; it adapts to feeder density on its own. This replaces manual partitioning across multiple instances. The flags are overrides. |
| (MAX_SYNC_AC = 15, fixed) | `--sync-aircraft-per-receiver` (0 = off) | Same policy when set: a receiver keeps sending sync pairs for at most this many ADS-B aircraft. mlatd does not need it for CPU; use it when feeder uplinks are the constraint. |
| — | `--self-truth-csv` | Live accuracy measurement: mlatd also multilaterates ADS-B aircraft and compares each fix with the transmitted position. |

## Operational differences

- One instance replaces many. The geographic shards do internally what
  multiple mlat-server processes did. Start with one instance and the
  default partition flags.
- Memory use is constant. The clock-pair state has a fixed size.
  Expect tens of megabytes, with no growth over time.
- mlatd produces more results from the same traffic. Examine
  downstream assumptions about the output rate.
- `sync.json` continues to work for dashboards. The values come from
  mlatd's own clock models; the numbers differ from mlat-server's
  numbers, with the same meaning. `bad_syncs` is 0 or 0.4: mlatd has one
  verdict on a receiver, the timing-bias quarantine, and 0.4 is the score
  behind the `bad_sync_timeout` of 60 s its stats push reports.
- `clients.json` and `aircraft.json` carry mlat-server's fields. Two
  differ in origin: `sync_interest` and `mlat_interest` list the aircraft
  a receiver actually reported in the last minute, not the ones the
  server asked it for (mlatd asks for everything). `mlat_kalman_count` is
  always 0 and `heading`/`speed` come from the last two fixes, since there
  is no Kalman track. The map position in `sync.json` is fudged as in
  mlat-server (1/20° grid, hidden under `privacy`), with an offset derived
  from the user name so it survives restarts.
- The server finds clock jumps for each receiver pair and resets the
  pair. No manual intervention is necessary.
- Reconnects are cheap. A feeder that reconnects takes its old slot
  back (matched by user name); the dead connection's state is freed and
  its late messages are discarded. Connections silent for 5 minutes are
  reaped.
- Receiver coordinates arrive in the handshake. Bind the client port
  to a private interface (see `compose.example.yml`).

## Test the swap with your own traffic

The [mlat-bench](https://github.com/yoanntlm/mlat-bench) `record`
proxy copies live feeder traffic to a capture file. `replay` sends the
same capture to your mlat-server and to mlatd. `score` and `diff`
compare the two runs. Ten minutes of traffic is sufficient for a
decision.
