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
    /// Work dir: sync.json is written here every 15 s in mlat-server's
    /// format, so existing monitoring keeps working.
    #[arg(long)]
    work_dir: Option<std::path::PathBuf>,
    /// Shard count (0 = auto: available cores − 2, min 1). Each shard owns
    /// an independent geographic slice; see shard.rs.
    #[arg(long, default_value_t = 0)]
    shards: usize,
    /// Geographic cell size for shard assignment, degrees. 5 suits sparse
    /// continental networks; 2 suits dense metros under heavy load.
    #[arg(long, default_value_t = 5.0)]
    shard_cell_deg: f64,
    /// Receiver capacity per shard before region growth spills over.
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

    // ---- output task: owns every writer, dedupes boundary aircraft -------
    let (out_tx, mut out_rx) = mpsc::channel::<OutMsg>(4096);
    let (publish, _) = tokio::sync::broadcast::channel::<Arc<Published>>(1024);
    {
        use std::io::Write;
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

    // ---- shards ----------------------------------------------------------
    let window = Duration::from_millis(cli.group_window_ms);
    let mut handles = Vec::new();
    for _ in 0..n_shards {
        let (tx, rx) = mpsc::channel::<ShardMsg>(8192);
        let state = State::new(
            cli.time_scale,
            mlat_adsb,
            cli.write_filtered_csv.is_some(),
            (epoch_unix, epoch_real),
        );
        tokio::spawn(shard::run_shard(state, rx, out_tx.clone(), window));
        handles.push(Arc::new(ShardHandle {
            tx,
            receivers: std::sync::atomic::AtomicUsize::new(0),
        }));
    }
    let router = Arc::new(Router::new(handles, cli.shard_cell_deg, cli.shard_cap));
    println!("mlatd: {n_shards} shards");

    // Stats line every 10 s, aggregated across shards.
    {
        let router = router.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let (mut rx_n, mut sync_o, mut solved, mut rej) = (0usize, 0u64, 0u64, 0u64);
                for sh in router.all() {
                    let (otx, orx) = oneshot::channel();
                    if sh.tx.send(ShardMsg::Stats(otx)).await.is_ok() {
                        if let Ok((a, b, c, d)) = orx.await {
                            rx_n += a;
                            sync_o += b;
                            solved += c;
                            rej += d;
                        }
                    }
                }
                println!("mlatd: rx={rx_n} sync_obs={sync_o} solved={solved} rejected={rej}");
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
                let Ok((mut sock, _)) = l.accept().await else {
                    break;
                };
                let mut rx = publish.subscribe();
                tokio::spawn(async move {
                    while let Ok(p) = rx.recv().await {
                        if sock.write_all(p.sbs_line.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
    }
    // sync.json export for existing monitoring, merged across shards.
    if let Some(dir) = cli.work_dir.clone() {
        let router = router.clone();
        let _ = std::fs::create_dir_all(&dir);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            loop {
                tick.tick().await;
                let mut merged = serde_json::Map::new();
                for sh in router.all() {
                    let (otx, orx) = oneshot::channel();
                    if sh.tx.send(ShardMsg::SyncJson(otx)).await.is_ok() {
                        if let Ok(serde_json::Value::Object(m)) = orx.await {
                            merged.extend(m);
                        }
                    }
                }
                let _ = std::fs::write(
                    dir.join("sync.json"),
                    serde_json::to_vec(&serde_json::Value::Object(merged)).unwrap_or_default(),
                );
            }
        });
    }

    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    println!("mlatd: listening on {listen}");
    let hb_real = Duration::from_secs_f64(30.0 / cli.time_scale);
    loop {
        let (stream, peer) = listener.accept().await?;
        let router = router.clone();
        let publish = publish.clone();
        let scale = cli.time_scale;
        tokio::spawn(async move {
            if let Err(e) = handle_client(
                stream,
                router,
                publish,
                hb_real,
                scale,
                (epoch_unix, epoch_real),
            )
            .await
            {
                eprintln!("mlatd: {peer}: {e:#}");
            }
        });
    }
}

/// Output clock (unix seconds, scaled) for heartbeats — shard-independent.
fn scaled_now(t0_unix: f64, t0: std::time::Instant, scale: f64) -> f64 {
    t0_unix + t0.elapsed().as_secs_f64() * scale
}

async fn handle_client(
    stream: TcpStream,
    router: Arc<Router>,
    publish: tokio::sync::broadcast::Sender<Arc<Published>>,
    hb_real: Duration,
    time_scale: f64,
    epoch: (f64, std::time::Instant),
) -> Result<()> {
    let (conn_t0_unix, conn_t0) = epoch;
    stream.set_nodelay(true)?;
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
    // Route by geography: this receiver's shard owns it for the process
    // lifetime.
    let (_shard_idx, shard) = router.shard_for(lat, lon);
    shard
        .receivers
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (otx, orx) = oneshot::channel();
    shard
        .tx
        .send(ShardMsg::AddReceiver(
            ReceiverInfo {
                user: user.clone(),
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
    let wants_results = hs["return_results"].as_bool().unwrap_or(false);
    // A real mlat-client sends no traffic until asked: selective traffic is
    // the request channel (observed with 5 real clients: connected, decoded
    // Beast, sent nothing). Do what mlat-server does: enable it, request
    // rate reports, and start_sending every aircraft the client reports.
    let reply = format!(
        "{{\"compress\":\"{negotiated}\",\"reconnect_in\":300,\"selective_traffic\":true,\
         \"heartbeat\":true,\"return_results\":{wants_results},\"rate_reports\":true,\
         \"motd\":\"mlat-bench candidate mlatd\"}}\n"
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
    let writer = tokio::spawn(async move {
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
    if wants_results {
        let mut sub = publish.subscribe();
        let tx = tx_line.clone();
        tokio::spawn(async move {
            while let Ok(p) = sub.recv().await {
                if tx.send(p.result_line.clone()).await.is_err() {
                    break;
                }
            }
        });
    }

    // ---- message loop ----------------------------------------------------
    // A connection silent for 5 minutes is dead (real clients heartbeat
    // every 30 s); reap it so churned feeders do not accumulate.
    const IDLE: Duration = Duration::from_secs(300);
    let mut hb = tokio::time::interval(hb_real);
    hb.tick().await; // consume immediate first tick
    let mut zdec = if negotiated == "none" {
        None
    } else {
        Some(ZlibFrameDecoder::new())
    };
    let mut requested: std::collections::HashSet<String> = std::collections::HashSet::new();
    let res: Result<()> = async {
        loop {
            match &mut zdec {
                None => {
                    let mut line = Vec::new();
                    let mut limited = (&mut rd).take(256 * 1024);
                    tokio::select! {
                        _ = hb.tick() => {
                            let st = scaled_now(conn_t0_unix, conn_t0, time_scale);
                            let _ = tx_line.send(format!("{{\"heartbeat\":{{\"server_time\":{st:.3}}}}}\n")).await;
                        }
                        r = tokio::time::timeout(IDLE, limited.read_until(b'\n', &mut line)) => {
                            let n = r.context("idle for 5 minutes")??;
                            if n == 0 { break }
                            if line.last() != Some(&b'\n') {
                                anyhow::bail!("line over 256 KiB");
                            }
                            let now_s = scaled_now(conn_t0_unix, conn_t0, time_scale);
                            process_line_tx(&shard, rx, &line, Some(&tx_line), &mut requested, now_s).await;
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
                            let _ = tx_line.send(format!("{{\"heartbeat\":{{\"server_time\":{st:.3}}}}}\n")).await;
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
                            process_line_tx(&shard, rx, line, Some(&tx_line), &mut requested, now_s)
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
    drop(tx_line);
    let _ = writer.await;
    println!("mlatd: {user} disconnected");
    res
}

/// seen/rate_report trigger start_sending for aircraft not yet requested on
/// this connection; a real mlat-client sends nothing until asked.
async fn process_line_tx(
    shard: &Arc<ShardHandle>,
    rx: crate::state::RxRef,
    line: &[u8],
    tx: Option<&tokio::sync::mpsc::Sender<String>>,
    requested: &mut std::collections::HashSet<String>,
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
                    if requested.insert(h.to_lowercase()) {
                        fresh.push(h.to_lowercase());
                    }
                }
            }
        }
        if let Some(rr) = v.get("rate_report").and_then(|x| x.as_object()) {
            for k in rr.keys() {
                if requested.insert(k.to_lowercase()) {
                    fresh.push(k.to_lowercase());
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
