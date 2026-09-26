//! mlatd — an MLAT server for the mlat-client protocol.
//!
//! Connection handling and wiring live in this file: the handshake
//! (compress none/zlib/zlib2), selective traffic, per-connection routing to
//! a geographic shard, the output task (CSV, SBS, result return), sync.json
//! export, and stats. Estimation lives in state.rs, clocksync.rs, and
//! solve.rs; sharding in shard.rs.
//!
//! Bench it:
//!   cargo run -p mlatd -- --write-csv /tmp/cand.csv --time-scale 10 --group-window-ms 90
//!   cargo run -p mlat-bench -- replay <capture> --speed 10 \
//!       --addr 127.0.0.1:40160 --results-csv /tmp/cand.csv

mod clocksync;
mod shard;
mod solve;
mod state;
mod track;
mod traffic;

use anyhow::{Context, Result};
use clap::Parser;
use mb_proto::framing::ZlibFrameDecoder;
use shard::{OutMsg, Router, ShardHandle, ShardMsg};
use state::{Published, ReceiverInfo, State};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;

#[derive(Parser)]
#[command(name = "mlatd", version, about)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:40160")]
    listen: String,
    /// Results CSV output in mlat-server's column format (the bench's
    /// scoring input).
    /// Optional in production: results also flow to connected clients
    /// (return_results) and the SBS listener.
    #[arg(long)]
    write_csv: Option<std::path::PathBuf>,
    /// Message-grouping window, milliseconds of real time. At an accelerated
    /// replay divide the usual 900 by the speed factor.
    #[arg(long, default_value_t = 900)]
    group_window_ms: u64,
    /// Output/heartbeat clock runs this many times real speed — match the
    /// bench's --speed so scoring maps time correctly.
    #[arg(long, default_value_t = 1.0)]
    time_scale: f64,
    /// mlat-server-compatible alias for --listen ([host:]port accepted).
    #[arg(long)]
    client_listen: Option<String>,
    /// SBS/BaseStation output listener (what readsb ingests), e.g.
    /// 127.0.0.1:40161. mlat-server's flag name, kept for compatibility.
    #[arg(long)]
    basestation_listen: Option<String>,
    /// SBS/BaseStation output as a client: connect to host:port (a readsb
    /// --net-sbs-in-port, or any SBS listener) and push results, with
    /// reconnect. mlat-server's flag name; may repeat.
    #[arg(long)]
    basestation_connect: Vec<String>,
    /// Refuse return_results: clients get no result messages, whatever
    /// their handshake asks; results reach only the SBS output and CSVs.
    /// mlat-server has no such flag.
    #[arg(long)]
    no_client_results: bool,
    /// Work dir: sync.json, clients.json and aircraft.json are written
    /// here every 15 s in mlat-server's format, so existing monitoring
    /// keeps working (plus partition.json, the shard map).
    #[arg(long)]
    work_dir: Option<std::path::PathBuf>,
    /// Seconds between statistics lines on stdout; -1 disables them.
    /// mlat-server's flag; rounded up to the 10 s stats cadence.
    #[arg(long, default_value_t = 15, allow_hyphen_values = true)]
    status_interval: i64,
    /// Shard count (0 = auto: available cores − 2, min 1). Each shard owns
    /// an independent geographic slice; see shard.rs. Receivers on
    /// different shards never sync with each other, so more shards trade
    /// co-hearing pairs for CPU: raise it only on a CPU-bound hub.
    #[arg(long, default_value_t = 0)]
    shards: usize,
    /// Base geographic cell size for shard assignment, degrees. Dense
    /// cells split automatically, stopping at the 2° physical floor; the
    /// flag is an override, not something a deployment should need.
    #[arg(long, default_value_t = 5.0)]
    shard_cell_deg: f64,
    /// Receiver capacity per shard before region growth spills over
    /// (message rate gates growth too).
    #[arg(long, default_value_t = 64)]
    shard_cap: usize,
    /// Alpha-beta-smoothed results, same CSV format: the analogue of
    /// mlat-server's Kalman output. Experimental; on real data it measured
    /// worse than raw output.
    #[arg(long)]
    write_filtered_csv: Option<std::path::PathBuf>,
    /// Multilaterate DF17 (ADS-B) frames too and score each fix against the
    /// aircraft's own broadcast position: live accuracy without external
    /// truth. Rows: t,icao,err_m,est_m,n → this CSV.
    #[arg(long)]
    self_truth_csv: Option<std::path::PathBuf>,
    /// ADS-B aircraft a receiver keeps sending sync pairs for, at most
    /// (mlat-server's MAX_SYNC_AC = 15). Mode-S targets are never capped.
    /// 0 = unlimited, the default: on the 2026-09-21 drill a cap of 15
    /// cut sync intake 8× but cost 3 points of coverage and freed no CPU,
    /// so it is a relief valve for a saturated uplink, not a default.
    #[arg(long, default_value_t = 0)]
    sync_aircraft_per_receiver: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mlat_adsb = cli.self_truth_csv.is_some();
    let n_shards = if cli.shards == 0 {
        std::thread::available_parallelism()
            .map(|n| (n.get().saturating_sub(2)).max(1))
            .unwrap_or(1)
    } else {
        cli.shards
    };
    let listen = cli
        .client_listen
        .as_deref()
        .map(|s| {
            // Accept mlat-server's bare-port form.
            if s.contains(':') {
                s.to_string()
            } else {
                format!("0.0.0.0:{s}")
            }
        })
        .unwrap_or_else(|| cli.listen.clone());

    // One scaled-clock epoch for everything: shards, heartbeats, stamps.
    let epoch_real = std::time::Instant::now();
    let epoch_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);

    // ---- shards ----------------------------------------------------------
    let (out_tx, mut out_rx) = mpsc::channel::<OutMsg>(4096);
    let (publish, _) = tokio::sync::broadcast::channel::<Arc<Published>>(1024);
    let window = Duration::from_millis(cli.group_window_ms);
    let mut handles = Vec::new();
    for shard_id in 0..n_shards {
        let (tx, rx) = mpsc::channel::<ShardMsg>(8192);
        let state = State::new(
            shard_id,
            cli.time_scale,
            mlat_adsb,
            cli.write_filtered_csv.is_some(),
            (epoch_unix, epoch_real),
        );
        tokio::spawn(shard::run_shard(state, rx, out_tx.clone(), window));
        handles.push(Arc::new(ShardHandle {
            tx,
            receivers: std::sync::atomic::AtomicUsize::new(0),
            rate: std::sync::atomic::AtomicU64::new(0),
        }));
    }
    let router = Arc::new(Router::new(handles, cli.shard_cell_deg, cli.shard_cap));
    println!("mlatd: {n_shards} shards");
    // ---- output task: owns every writer, dedupes boundary aircraft.
    // (Channels first; the task itself spawns after the router exists,
    // because the territory gate needs it.)
    {
        use std::io::Write;
        let gate_router = router.clone();
        let publish = publish.clone();
        let mut csv = match &cli.write_csv {
            Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
            None => None,
        };
        let mut filtered = match &cli.write_filtered_csv {
            Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
            None => None,
        };
        let mut selftruth = match &cli.self_truth_csv {
            Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
            None => None,
        };
        tokio::spawn(async move {
            // Boundary aircraft may be solved by two shards within the same
            // instant; drop the twin by (icao, 100 ms bucket).
            let mut recent: std::collections::HashMap<(u32, i64), ()> = Default::default();
            let mut order: std::collections::VecDeque<(u32, i64)> = Default::default();
            while let Some(msg) = out_rx.recv().await {
                match msg {
                    OutMsg::SelfTruth(line) => {
                        if let Some(w) = selftruth.as_mut() {
                            let _ = w.write_all(line.as_bytes());
                            let _ = w.flush();
                        }
                    }
                    OutMsg::Fix(row) => {
                        // Territory gate: only the shard owning the solved
                        // position publishes. A border aircraft solved by a
                        // neighbor shard's one-sided receiver subset carries
                        // a systematic bias (measured ~200 m); the owner's
                        // solve is the balanced one.
                        if let Some(owner) = gate_router.owner_of_point(row.lat, row.lon) {
                            if owner != row.shard {
                                continue;
                            }
                        }
                        let key = (row.icao.0, (row.stamp * 10.0) as i64);
                        if recent.contains_key(&key) {
                            continue; // boundary twin
                        }
                        recent.insert(key, ());
                        order.push_back(key);
                        while order.len() > 4096 {
                            if let Some(k) = order.pop_front() {
                                recent.remove(&k);
                            }
                        }
                        if let Some(w) = csv.as_mut() {
                            let _ = w.write_all(row.csv_line.as_bytes());
                            let _ = w.flush();
                        }
                        if let (Some(w), Some(l)) = (filtered.as_mut(), &row.filtered_line) {
                            let _ = w.write_all(l.as_bytes());
                            let _ = w.flush();
                        }
                        let _ = publish.send(Arc::new(row.published));
                    }
                }
            }
        });
    }

    // Stats every 10 s, aggregated across shards: the partition's load
    // gate reads them each time; the stdout line follows --status-interval.
    {
        let router = router.clone();
        let status_every = cli.status_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            let mut prev_sync: Vec<u64> = vec![0; router.all().len()];
            let mut last_status = std::time::Instant::now();
            loop {
                tick.tick().await;
                let (mut rx_n, mut sync_o, mut solved, mut rej) = (0usize, 0u64, 0u64, 0u64);
                for (i, sh) in router.all().iter().enumerate() {
                    let (otx, orx) = oneshot::channel();
                    if sh.tx.send(ShardMsg::Stats(otx)).await.is_ok() {
                        if let Ok((a, b, c, d)) = orx.await {
                            // Per-window rate feeds the partition's
                            // capacity gate.
                            sh.rate.store(
                                b.saturating_sub(prev_sync[i]),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            prev_sync[i] = b;
                            rx_n += a;
                            sync_o += b;
                            solved += c;
                            rej += d;
                        }
                    }
                }
                let due = status_every >= 0
                    && last_status.elapsed().as_secs_f64() >= status_every as f64 * 0.95;
                if due {
                    last_status = std::time::Instant::now();
                    println!("mlatd: rx={rx_n} sync_obs={sync_o} solved={solved} rejected={rej}");
                }
                if std::env::var("MB_DEBUG_PARTITION").is_ok() {
                    for (lvl, y, x, sh, n) in router.partition_dump() {
                        println!("cell L{lvl} y{y} x{x} -> shard {sh} ({n} rx)");
                    }
                }
            }
        });
    }

    // SBS output listener: each consumer gets the broadcast fix stream.
    if let Some(addr) = cli.basestation_listen.clone() {
        let publish = publish.clone();
        tokio::spawn(async move {
            let Ok(l) = TcpListener::bind(&addr).await else {
                eprintln!("mlatd: cannot bind SBS listener {addr}");
                return;
            };
            println!("mlatd: SBS output on {addr}");
            loop {
                // Same transient failures as the client listener; a break
                // here left the process up with the SBS port dead.
                let sock = match l.accept().await {
                    Ok((sock, _)) => sock,
                    Err(e) => {
                        eprintln!("mlatd: SBS accept failed ({e}); retrying");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let rx = publish.subscribe();
                tokio::spawn(sbs_writer(sock, rx));
            }
        });
    }
    // SBS output as a client (mlat-server's --basestation-connect): dial
    // the consumer, push the same stream, redial after any failure.
    for addr in cli.basestation_connect.clone() {
        let publish = publish.clone();
        tokio::spawn(async move {
            let mut announced = false;
            loop {
                match TcpStream::connect(&addr).await {
                    Ok(sock) => {
                        println!("mlatd: SBS output connected to {addr}");
                        announced = false;
                        sbs_writer(sock, publish.subscribe()).await;
                        eprintln!("mlatd: SBS output to {addr} closed; reconnecting");
                    }
                    Err(e) => {
                        if !announced {
                            eprintln!("mlatd: SBS output to {addr} unavailable ({e}); retrying");
                            announced = true;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
    // Work-dir export for existing monitoring, merged across shards:
    // sync.json, clients.json, aircraft.json in mlat-server's shapes.
    if let Some(dir) = cli.work_dir.clone() {
        let router = router.clone();
        let _ = std::fs::create_dir_all(&dir);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            loop {
                tick.tick().await;
                let mut sync = serde_json::Map::new();
                let mut clients = serde_json::Map::new();
                let mut aircraft = serde_json::Map::new();
                for sh in router.all() {
                    let (otx, orx) = oneshot::channel();
                    if sh.tx.send(ShardMsg::SyncJson(otx)).await.is_ok() {
                        if let Ok(serde_json::Value::Object(m)) = orx.await {
                            sync.extend(m);
                        }
                    }
                    let (otx, orx) = oneshot::channel();
                    if sh.tx.send(ShardMsg::StateJson(otx)).await.is_ok() {
                        if let Ok((serde_json::Value::Object(c), serde_json::Value::Object(a))) =
                            orx.await
                        {
                            clients.extend(c);
                            // A border aircraft is known to two shards; the
                            // one that heard it last speaks for it.
                            for (icao, entry) in a {
                                let newer = aircraft
                                    .get(&icao)
                                    .and_then(|e| e["elapsed_seen"].as_f64())
                                    .is_none_or(|prev| {
                                        entry["elapsed_seen"].as_f64().unwrap_or(f64::MAX) < prev
                                    });
                                if newer {
                                    aircraft.insert(icao, entry);
                                }
                            }
                        }
                    }
                }
                write_atomic(&dir.join("sync.json"), &serde_json::Value::Object(sync));
                write_atomic(
                    &dir.join("clients.json"),
                    &serde_json::Value::Object(clients),
                );
                write_atomic(
                    &dir.join("aircraft.json"),
                    &serde_json::Value::Object(aircraft),
                );
                // partition.json beside it: the cell map as data, for
                // plots/partition.py. Cells carry no receiver positions.
                let cells: Vec<serde_json::Value> = router
                    .partition_dump()
                    .into_iter()
                    .map(|(level, y, x, shard, rx)| {
                        let size = router.cell_size_of(level);
                        serde_json::json!({
                            "level": level,
                            "lat0": f64::from(y) * size,
                            "lat1": (f64::from(y) + 1.0) * size,
                            "lon0": f64::from(x) * size,
                            "lon1": (f64::from(x) + 1.0) * size,
                            "shard": shard,
                            "rx": rx,
                        })
                    })
                    .collect();
                write_atomic(
                    &dir.join("partition.json"),
                    &serde_json::Value::Array(cells),
                );
            }
        });
    }

    let uid_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    println!("mlatd: listening on {listen}");
    let hb_real = Duration::from_secs_f64(30.0 / cli.time_scale);
    loop {
        // accept() fails transiently: EMFILE/ENFILE when descriptors run
        // out, ECONNABORTED when the peer resets mid-handshake. Returning
        // the error here ended the process; a short pause lets descriptors
        // free up and the reset is nobody's problem.
        let (stream, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                eprintln!("mlatd: accept failed ({e}); retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let router = router.clone();
        let publish = publish.clone();
        let scale = cli.time_scale;
        let uid = uid_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sync_cap = cli.sync_aircraft_per_receiver;
        let client_results = !cli.no_client_results;
        tokio::spawn(async move {
            let cfg = ClientCfg {
                hb_real,
                time_scale: scale,
                epoch: (epoch_unix, epoch_real),
                uid,
                sync_cap,
                client_results,
            };
            if let Err(e) = handle_client(stream, router, publish, cfg).await {
                eprintln!("mlatd: {peer}: {e:#}");
            }
        });
    }
}

/// Write a JSON file the way mlat-server does: to a temp file, then an
/// atomic rename, so a reader never sees a half-written document.
fn write_atomic(path: &std::path::Path, value: &serde_json::Value) {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    if std::fs::write(&tmp, bytes).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// Output clock (unix seconds, scaled) for heartbeats — shard-independent.
fn scaled_now(t0_unix: f64, t0: std::time::Instant, scale: f64) -> f64 {
    t0_unix + t0.elapsed().as_secs_f64() * scale
}

/// Per-connection settings handed to handle_client.
struct ClientCfg {
    hb_real: Duration,
    time_scale: f64,
    epoch: (f64, std::time::Instant),
    /// Process-wide serial (mlat-server's uid).
    uid: u64,
    /// ADS-B sync aircraft cap per receiver; 0 = unlimited.
    sync_cap: usize,
    /// False under --no-client-results.
    client_results: bool,
}

async fn handle_client(
    stream: TcpStream,
    router: Arc<Router>,
    publish: tokio::sync::broadcast::Sender<Arc<Published>>,
    cfg: ClientCfg,
) -> Result<()> {
    let ClientCfg {
        hb_real,
        time_scale,
        epoch,
        uid,
        sync_cap,
        client_results,
    } = cfg;
    let (conn_t0_unix, conn_t0) = epoch;
    stream.set_nodelay(true)?;
    let (source_ip, source_port) = match stream.peer_addr() {
        Ok(a) => (a.ip().to_string(), a.port()),
        Err(_) => (String::new(), 0),
    };
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);

    // ---- handshake -------------------------------------------------------
    let mut hs_line = Vec::new();
    let n = tokio::time::timeout(
        Duration::from_secs(15),
        AsyncBufReadExt::read_until(&mut (&mut rd).take(64 * 1024), b'\n', &mut hs_line),
    )
    .await
    .context("no handshake within 15 s")??;
    if n == 0 {
        anyhow::bail!("closed before handshake");
    }
    if hs_line.last() != Some(&b'\n') {
        anyhow::bail!("handshake line over 64 KiB");
    }
    let hs: serde_json::Value = serde_json::from_slice(&hs_line).context("handshake not JSON")?;
    // Compression preference: zlib2 first, mlat-server's order. At fleet
    // scale the uplink is the feeders' home bandwidth; plain lines waste it.
    let offered: Vec<String> = hs["compress"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|c| c.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let negotiated = ["zlib2", "zlib", "none"]
        .into_iter()
        .find(|m| offered.iter().any(|o| o == m));
    let Some(negotiated) = negotiated else {
        wr.write_all(b"{\"deny\":[\"no supported compression offered\"],\"reconnect_in\":300}\n")
            .await?;
        anyhow::bail!("client offered none of none/zlib/zlib2");
    };
    let (Some(lat), Some(lon), Some(alt)) =
        (hs["lat"].as_f64(), hs["lon"].as_f64(), hs["alt"].as_f64())
    else {
        wr.write_all(b"{\"deny\":[\"missing position\"],\"reconnect_in\":300}\n")
            .await?;
        anyhow::bail!("handshake missing position");
    };
    let clock_type = hs["clock_type"].as_str().unwrap_or("unknown").to_string();
    let freq_hz = match clock_type.as_str() {
        "radarcape_gps" | "radarcape" => 1e9,
        "sbs" => 20e6,
        _ => 12e6, // dump1090, beast, radarcape_12mhz, unknown
    };
    let user = hs["user"].as_str().unwrap_or("anon").to_string();
    let geo = mb_core::Geodetic {
        lat_deg: lat,
        lon_deg: lon,
        alt_m: alt,
    };
    let gps = clock_type.starts_with("radarcape_gps");
    let uuid = hs["uuid"].as_str().map(String::from);
    let privacy = hs["privacy"].as_bool().unwrap_or(false);
    // mlat-server's connection_info: "user v<proto> <clock> <client> tcp <compress>".
    let connection_info = format!(
        "{user} v{} {clock_type} {} tcp {negotiated}",
        hs["version"].as_i64().unwrap_or(0),
        hs["client_version"].as_str().unwrap_or("unknown"),
    );
    // Route by geography: this receiver's shard owns it for the process
    // lifetime.
    // The router counts the receiver at claim time; teardown decrements.
    let (_shard_idx, shard) = router.shard_for(lat, lon);
    let (otx, orx) = oneshot::channel();
    shard
        .tx
        .send(ShardMsg::AddReceiver(
            ReceiverInfo {
                user: user.clone(),
                uid,
                uuid,
                privacy,
                connection_info,
                source_ip,
                source_port,
                ecef: geo.to_ecef(),
                geo,
                freq_hz,
                gps,
                // Effective timing error: clock jitter + pair-model slack.
                jitter_s: if gps { 30e-9 } else { 150e-9 },
            },
            otx,
        ))
        .await
        .map_err(|_| anyhow::anyhow!("shard gone"))?;
    let rx = orx.await.map_err(|_| anyhow::anyhow!("shard gone"))?;
    let wants_results = client_results && hs["return_results"].as_bool().unwrap_or(false);
    let wants_stats = hs["return_stats"].as_bool().unwrap_or(false);
    // A real mlat-client sends no traffic until asked: selective traffic is
    // the request channel (observed with 5 real clients: connected, decoded
    // Beast, sent nothing). Do what mlat-server does: enable it, request
    // rate reports, and start_sending every aircraft the client reports.
    let reply = format!(
        "{{\"compress\":\"{negotiated}\",\"reconnect_in\":300,\"selective_traffic\":true,\
         \"heartbeat\":true,\"return_results\":{wants_results},\"rate_reports\":true,\
         \"return_stats\":{wants_stats},\
         \"motd\":\"FlightPortrait network MLAT (mlatd)\"}}\n"
    );
    wr.write_all(reply.as_bytes()).await?;
    println!("mlatd: {user} connected ({clock_type}, {negotiated})");

    // Single writer task: heartbeats and (if subscribed) result messages
    // funnel through one mpsc so the socket has exactly one writer. On
    // zlib2 the downlink is compressed with the same framing as the uplink
    // (jsonclient.py maps zlib2 to write_zlib; zlib and none to write_raw),
    // batched up to 1 s.
    let (tx_line, mut rx_line) = tokio::sync::mpsc::channel::<String>(256);
    let compress_down = negotiated == "zlib2";
    let mut writer = tokio::spawn(async move {
        if !compress_down {
            while let Some(l) = rx_line.recv().await {
                if wr.write_all(l.as_bytes()).await.is_err() {
                    break;
                }
            }
            return;
        }
        let mut enc = mb_proto::framing::ZlibFrameEncoder::new();
        let mut batch: Vec<u8> = Vec::new();
        let mut flush = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                l = rx_line.recv() => {
                    let Some(l) = l else { break };
                    batch.extend_from_slice(l.as_bytes());
                    if batch.len() < 32 * 1024 {
                        continue;
                    }
                    let Ok(f) = enc.encode_frame(&batch) else { break };
                    batch.clear();
                    if wr.write_all(&f).await.is_err() {
                        break;
                    }
                }
                _ = flush.tick() => {
                    if batch.is_empty() {
                        continue;
                    }
                    let Ok(f) = enc.encode_frame(&batch) else { break };
                    batch.clear();
                    if wr.write_all(&f).await.is_err() {
                        break;
                    }
                }
            }
        }
        // Flush what the loop left behind.
        if !batch.is_empty() {
            if let Ok(f) = enc.encode_frame(&batch) {
                let _ = wr.write_all(&f).await;
            }
        }
    });
    // The forwarder is aborted at teardown. It holds a clone of the
    // writer's sender and would otherwise stop only when a send fails,
    // which needs the writer gone, which needs every sender dropped: a
    // cycle. A closed receiver hears nothing, so no fix is ever for it and
    // the send that would have broken the cycle never came. Every closed
    // return_results connection then kept its socket, both zlib states and
    // three tasks for the life of the process (≈210 KB each; a hub with
    // reconnecting feeders reached 2 GB RSS in 36 h).
    let forwarder = wants_results.then(|| {
        let mut sub = publish.subscribe();
        let tx = tx_line.clone();
        tokio::spawn(async move {
            // Only fixes this receiver heard the message for, as
            // mlat-server's forward_results.
            loop {
                match sub.recv().await {
                    Ok(p) if p.is_for(uid) => {
                        if tx.send(p.result_line.clone()).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    });

    // ---- message loop ----------------------------------------------------
    // A connection silent for 5 minutes is dead (real clients heartbeat
    // every 30 s); reap it so churned feeders do not accumulate.
    const IDLE: Duration = Duration::from_secs(300);
    let mut hb = tokio::time::interval(hb_real);
    hb.tick().await; // consume immediate first tick
                     // Per-receiver stats push, every 15 s when the client asked for it
                     // (mlat-client always does; its --stats-json file is built from this).
    let mut stats_tick = tokio::time::interval(Duration::from_secs(15));
    stats_tick.tick().await;
    let mut zdec = if negotiated == "none" {
        None
    } else {
        Some(ZlibFrameDecoder::new())
    };
    let mut traffic = traffic::Traffic::new(sync_cap);
    let res: Result<()> = async {
        loop {
            match &mut zdec {
                None => {
                    let mut line = Vec::new();
                    let mut limited = (&mut rd).take(256 * 1024);
                    tokio::select! {
                        _ = hb.tick() => {
                            // try_send: a peer that stopped reading fills
                            // the writer's queue, and an awaiting send here
                            // would park the reader too, past the idle reaper.
                            let st = scaled_now(conn_t0_unix, conn_t0, time_scale);
                            let _ = tx_line.try_send(format!("{{\"heartbeat\":{{\"server_time\":{st:.3}}}}}\n"));
                        }
                        _ = stats_tick.tick(), if wants_stats => {
                            push_stats(&shard, rx, &tx_line).await;
                        }
                        r = tokio::time::timeout(IDLE, limited.read_until(b'\n', &mut line)) => {
                            let n = r.context("idle for 5 minutes")??;
                            if n == 0 { break }
                            if line.last() != Some(&b'\n') {
                                anyhow::bail!("line over 256 KiB");
                            }
                            let now_s = scaled_now(conn_t0_unix, conn_t0, time_scale);
                            process_line_tx(&shard, rx, &line, Some(&tx_line), &mut traffic, now_s).await;
                        }
                    }
                }
                Some(dec) => {
                    // Framed: 2-byte BE length + zlib payload with persistent
                    // dictionary state (mb-proto framing; the same code the
                    // capture generator uses, exercised from the other side).
                    let mut lenb = [0u8; 2];
                    tokio::select! {
                        _ = hb.tick() => {
                            let st = scaled_now(conn_t0_unix, conn_t0, time_scale);
                            let _ = tx_line.try_send(format!("{{\"heartbeat\":{{\"server_time\":{st:.3}}}}}\n"));
                            continue
                        }
                        _ = stats_tick.tick(), if wants_stats => {
                            push_stats(&shard, rx, &tx_line).await;
                            continue
                        }
                        r = tokio::time::timeout(IDLE, rd.read_exact(&mut lenb)) => {
                            if r.context("idle for 5 minutes")?.is_err() { break }
                        }
                    }
                    let want = u16::from_be_bytes(lenb) as usize;
                    let mut payload = vec![0u8; 2 + want];
                    payload[..2].copy_from_slice(&lenb);
                    if rd.read_exact(&mut payload[2..]).await.is_err() {
                        break;
                    }
                    let Ok(chunk) = dec.decode_frame(&payload) else {
                        anyhow::bail!("zlib frame decode failed for {user}");
                    };
                    let now_s = scaled_now(conn_t0_unix, conn_t0, time_scale);
                    for line in chunk.split(|b| *b == b'\n') {
                        if !line.is_empty() {
                            process_line_tx(&shard, rx, line, Some(&tx_line), &mut traffic, now_s)
                                .await;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    // Free the slot on every exit path; the generation guard makes this
    // safe against a same-user reconnect that already took the slot over.
    let _ = shard.tx.send(ShardMsg::RemoveReceiver(rx)).await;
    shard
        .receivers
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(f) = &forwarder {
        f.abort();
    }
    drop(tx_line);
    // Flush what the writer still holds, but not for long: a peer that
    // stopped reading with the socket still up blocks write_all until the
    // kernel gives up on retransmits, and the socket must not outlive the
    // connection by that much. Dropping the writer closes the write half.
    if tokio::time::timeout(Duration::from_secs(5), &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
    println!("mlatd: {user} disconnected");
    res
}

/// One stats push: mlat-server's per-receiver triple, same field names.
async fn push_stats(
    shard: &Arc<ShardHandle>,
    rx: crate::state::RxRef,
    tx_line: &tokio::sync::mpsc::Sender<String>,
) {
    let (otx, orx) = oneshot::channel();
    if shard
        .tx
        .send(ShardMsg::ReceiverStats(rx, otx))
        .await
        .is_err()
    {
        return;
    }
    if let Ok(Some((peers, outlier_percent, quarantined))) = orx.await {
        let bad_sync_timeout = if quarantined { 60 } else { 0 };
        let _ = tx_line.try_send(format!(
            "{{\"stats\":{{\"peer_count\":{peers},\"bad_sync_timeout\":{bad_sync_timeout},\"outlier_percent\":{outlier_percent:.1}}}}}\n"
        ));
    }
}

/// seen/rate_report trigger start_sending for aircraft not yet requested on
/// this connection; a real mlat-client sends nothing until asked. Sync
/// pairs beyond the per-receiver ADS-B cap turn into stop_sending
/// (traffic.rs).
async fn process_line_tx(
    shard: &Arc<ShardHandle>,
    rx: crate::state::RxRef,
    line: &[u8],
    tx: Option<&tokio::sync::mpsc::Sender<String>>,
    traffic: &mut traffic::Traffic,
    at_scaled: f64,
) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) else {
        return;
    };
    // Aircraft the client offers (seen list / rate_report keys) → request
    // everything we have not requested yet.
    if let Some(tx) = tx {
        let mut fresh: Vec<String> = Vec::new();
        if let Some(seen) = v.get("seen").and_then(|x| x.as_array()) {
            for a in seen {
                if let Some(h) = a.as_str() {
                    let h = h.to_lowercase();
                    if traffic.offered(&h, at_scaled) {
                        fresh.push(h);
                    }
                }
            }
        }
        if let Some(rr) = v.get("rate_report").and_then(|x| x.as_object()) {
            for k in rr.keys() {
                let k = k.to_lowercase();
                if traffic.offered(&k, at_scaled) {
                    fresh.push(k);
                }
            }
        }
        if !fresh.is_empty() {
            let msg = format!(
                "{{\"start_sending\":{}}}\n",
                serde_json::to_string(&fresh).unwrap_or_default()
            );
            let _ = tx.try_send(msg);
        }
        if let Some(lost) = v.get("lost").and_then(|x| x.as_array()) {
            for a in lost.iter().filter_map(|a| a.as_str()) {
                traffic.lost(&a.to_lowercase());
            }
        }
    }
    if let Some(sy) = v.get("sync") {
        let (Some(et), Some(ot), Some(em), Some(om)) = (
            sy["et"].as_f64(),
            sy["ot"].as_f64(),
            sy["em"].as_str(),
            sy["om"].as_str(),
        ) else {
            return;
        };
        // The ADS-B cap: a sync pair from an aircraft beyond this
        // receiver's quota is dropped and the client told to stop it.
        if let Some(icao) = traffic::adsb_icao(em) {
            if !traffic.on_sync(&icao, at_scaled) {
                if let Some(tx) = tx {
                    let _ = tx.try_send(format!("{{\"stop_sending\":[\"{icao}\"]}}\n"));
                }
                return;
            }
        }
        let _ = shard
            .tx
            .send(ShardMsg::Sync {
                rx,
                et,
                ot,
                em: em.to_string(),
                om: om.to_string(),
                at_scaled,
            })
            .await;
    } else if let Some(ml) = v.get("mlat") {
        let (Some(t), Some(m)) = (ml["t"].as_f64(), ml["m"].as_str()) else {
            return;
        };
        let _ = shard
            .tx
            .send(ShardMsg::Mlat {
                rx,
                t,
                m: m.to_string(),
                at_scaled,
            })
            .await;
    } else if v.get("clock_reset").is_some() || v.get("clock_jump").is_some() {
        let _ = shard.tx.send(ShardMsg::ClockReset(rx)).await;
    }
    // seen/lost/heartbeat/rate_report/input_*: no state needed yet.
}

/// Write the broadcast fix stream to one SBS consumer until it goes away.
/// readsb drops an SBS input that stays silent for 70 s; a bare newline
/// every 30 s keeps it up through quiet periods and its parser ignores
/// lines that do not start with MSG.
async fn sbs_writer(mut sock: TcpStream, mut rx: tokio::sync::broadcast::Receiver<Arc<Published>>) {
    let mut keepalive = tokio::time::interval(Duration::from_secs(30));
    keepalive.tick().await;
    loop {
        let bytes: Vec<u8> = tokio::select! {
            r = rx.recv() => match r {
                Ok(p) => p.sbs_line.as_bytes().to_vec(),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            },
            _ = keepalive.tick() => b"\n".to_vec(),
        };
        if sock.write_all(&bytes).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    /// A return_results connection that closes must finish its teardown.
    /// The result forwarder held a clone of the writer's sender and only
    /// stopped when a send failed; a gone receiver hears nothing, so no
    /// fix was ever for it, the send never happened, and handle_client
    /// sat in `writer.await` forever with the socket and zlib state.
    #[tokio::test]
    async fn closed_results_connection_tears_down() {
        let (out_tx, _out_rx) = mpsc::channel::<OutMsg>(64);
        let (publish, _keep) = tokio::sync::broadcast::channel::<Arc<Published>>(16);
        let (tx, rx) = mpsc::channel::<ShardMsg>(64);
        let epoch = (0.0, std::time::Instant::now());
        tokio::spawn(shard::run_shard(
            State::new(0, 1.0, false, false, epoch),
            rx,
            out_tx,
            Duration::from_millis(900),
        ));
        let handle = Arc::new(ShardHandle {
            tx,
            receivers: AtomicUsize::new(0),
            rate: AtomicU64::new(0),
        });
        let router = Arc::new(Router::new(vec![handle], 1.0, 1000));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let cfg = ClientCfg {
                hb_real: Duration::from_secs(30),
                time_scale: 1.0,
                epoch,
                uid: 1,
                sync_cap: 0,
                client_results: true,
            };
            handle_client(stream, router, publish, cfg).await
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(
            b"{\"version\":3,\"compress\":[\"none\"],\"return_results\":true,\
              \"user\":\"t\",\"lat\":1.0,\"lon\":103.0,\"alt\":10.0,\
              \"clock_type\":\"dump1090\"}\n",
        )
        .await
        .unwrap();
        let mut reply = vec![0u8; 4096];
        assert!(c.read(&mut reply).await.unwrap() > 0, "handshake reply");
        drop(c);
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("handle_client returns once the client has closed")
            .unwrap()
            .unwrap();
    }
}
