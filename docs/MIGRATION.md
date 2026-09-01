# Migration from wiedehopf/mlat-server

Feeders do not need changes. mlat-client connects, handshakes, and
synchronizes with mlatd as before, with zlib/zlib2 compression and
selective traffic. The migration is a server-side swap.

## Flag mapping

| mlat-server | mlatd | notes |
|---|---|---|
| `--client-listen [host:]tcp[:udp]` | `--client-listen [host:]tcp` | TCP only. Remove a UDP port suffix. |
| `--work-dir DIR` | `--work-dir DIR` | mlatd writes only `sync.json` there (same format, every 15 s). There is no other state to migrate. |
| `--write-csv FILE` | `--write-csv FILE` | Same column format. Optional in mlatd. |
| `--basestation-listen [host:]port` | `--basestation-listen [host:]port` | Same SBS output. |
| `--filtered-basestation-listen` | not available | The SBS listener sends unsmoothed fixes. |
| `--status-interval N` | not available | A periodic statistics line goes to stdout. |
| (Kalman result columns) | `--write-filtered-csv` | Alpha-beta smoothing, experimental, off by default. |
| — | `--shards`, `--shard-cell-deg`, `--shard-cap` | Internal geographic partition. The defaults are for a continental network. This replaces manual partitioning across multiple instances. |
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
  numbers, with the same meaning.
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
