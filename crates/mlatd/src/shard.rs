//! Internal sharding: the planet on one binary, no partitioning ops.
//!
//! The single-mutex design capped at ~800 dense receivers on one core
//! (bench: real-time fine at 124% CPU, 2× load collapsed at a pinned 143%).
//! Physics makes sharding natural: sync pairs and solve clusters only form
//! between CO-HEARING receivers — geographic neighbors. So:
//!
//! - Each shard OWNS a full `State` inside its own task. No locks anywhere;
//!   connections send parsed messages over an mpsc.
//! - A receiver is assigned at handshake by its 5° geo-cell; cells stick to
//!   the least-loaded shard at first sight, so neighborhoods stay together
//!   and load spreads by actual receiver density (longitude bands would put
//!   all of Europe on one shard).
//! - Cross-shard receiver pairs simply never form — the same tradeoff as the
//!   oracle's --partition, but geography-aligned so the boundary is thin.
//!   A boundary aircraft heard by two shards may be solved by both; the
//!   output task dedupes by (icao, time bucket).
//! - One output task owns the CSV writers and fan-out channel; shards send
//!   it finished rows. Writers never contend with solving.

use crate::state::{Published, ReceiverInfo, State};
use mb_core::Icao;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub enum ShardMsg {
    AddReceiver(ReceiverInfo, oneshot::Sender<usize>),
    Sync {
        rx: usize,
        et: f64,
        ot: f64,
        em: String,
        om: String,
        /// Output-clock time at CONNECTION READ — before shard queueing.
        /// Stamping at shard-processing time lagged by the queue depth under
        /// load (bench: flat 2.2 km at 4×, heartbeats bypass queues).
        at_scaled: f64,
    },
    Mlat {
        rx: usize,
        t: f64,
        m: String,
        at_scaled: f64,
    },
    ClockReset {
        rx: usize,
    },
    Stats(oneshot::Sender<(usize, u64, u64, u64)>),
    SyncJson(oneshot::Sender<serde_json::Value>),
}

/// Shard → output task traffic.
pub enum OutMsg {
    Fix(OutRow),
    SelfTruth(String),
}

/// What a shard emits for one accepted fix.
pub struct OutRow {
    pub icao: Icao,
    pub stamp: f64,
    pub csv_line: String,
    pub filtered_line: Option<String>,
    pub published: Published,
}

pub struct ShardHandle {
    pub tx: mpsc::Sender<ShardMsg>,
    pub receivers: std::sync::atomic::AtomicUsize,
}

pub struct Router {
    shards: Vec<Arc<ShardHandle>>,
    /// Geographic cell size (degrees) and per-shard receiver capacity —
    /// deployment tunables with a measured tradeoff (see shard bench notes):
    /// 5°/64 suits sparse-continental real networks (LocaRDS: full
    /// recovery); dense synthetic metros under extreme load prefer 2° or
    /// plain least-loaded. No constant serves every geometry.
    cell_deg: f64,
    cap: usize,
    /// 2°×2° cell → shard index, sticky from first sight (5° quantized dense
    /// metros into ~9 blocks and starved the partition of granularity).
    cells: std::sync::Mutex<HashMap<(i16, i16), usize>>,
}

impl Router {
    pub fn new(shards: Vec<Arc<ShardHandle>>, cell_deg: f64, cap: usize) -> Self {
        Router {
            shards,
            cell_deg,
            cap,
            cells: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Shard for a receiver at (lat, lon): its cell's shard, remembered
    /// forever. A NEW cell joins the shard owning most of its 8 neighbors —
    /// geography first, load second. Pure least-loaded assignment scattered
    /// adjacent European cells across 20 shards and sliced real co-hearing
    /// neighborhoods apart (LocaRDS: results −32%, coverage 27→18%);
    /// neighbor affinity keeps regions contiguous, so parallelism spreads
    /// only where the map actually spreads.
    pub fn shard_for(&self, lat: f64, lon: f64) -> (usize, Arc<ShardHandle>) {
        let cell = (
            (lat / self.cell_deg).floor() as i16,
            (lon / self.cell_deg).floor() as i16,
        );
        let mut cells = self.cells.lock().unwrap();
        let idx = match cells.get(&cell) {
            Some(&i) => i,
            None => {
                // Region growing with a capacity cap: prefer the neighbor
                // shard, but once a shard holds ~CAP receivers the frontier
                // starts a new one (itself contiguous). Pure affinity fused
                // dense w800 into one shard (coverage 66→45% at 4×); pure
                // least-loaded shredded real Europe (LocaRDS −32%).
                let cap = self.cap;
                let mut votes: HashMap<usize, usize> = HashMap::new();
                for dy in -1i16..=1 {
                    for dx in -1i16..=1 {
                        if let Some(&n) = cells.get(&(cell.0 + dy, cell.1 + dx)) {
                            if self.shards[n]
                                .receivers
                                .load(std::sync::atomic::Ordering::Relaxed)
                                < cap
                            {
                                *votes.entry(n).or_insert(0) += 1;
                            }
                        }
                    }
                }
                let i = votes
                    .into_iter()
                    .max_by_key(|&(shard, n)| {
                        // Most neighboring cells wins; break ties toward the
                        // less-loaded shard.
                        (
                            n,
                            usize::MAX
                                - self.shards[shard]
                                    .receivers
                                    .load(std::sync::atomic::Ordering::Relaxed),
                        )
                    })
                    .map(|(shard, _)| shard)
                    .unwrap_or_else(|| {
                        self.shards
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, s)| {
                                s.receivers.load(std::sync::atomic::Ordering::Relaxed)
                            })
                            .map(|(i, _)| i)
                            .unwrap_or(0)
                    });
                cells.insert(cell, i);
                i
            }
        };
        (idx, self.shards[idx].clone())
    }

    pub fn all(&self) -> &[Arc<ShardHandle>] {
        &self.shards
    }
}

/// One shard's event loop: owns its State, processes messages + periodic
/// sweeps, forwards accepted fixes to the output task.
pub async fn run_shard(
    mut state: State,
    mut rx: mpsc::Receiver<ShardMsg>,
    out: mpsc::Sender<OutMsg>,
    group_window: std::time::Duration,
) {
    state.set_output(out);
    let mut sweep = tokio::time::interval(std::time::Duration::from_millis(10));
    loop {
        tokio::select! {
            _ = sweep.tick() => state.sweep(group_window),
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    ShardMsg::AddReceiver(info, reply) => {
                        let id = state.add_receiver(info);
                        let _ = reply.send(id);
                    }
                    ShardMsg::Sync { rx, et, ot, em, om, at_scaled } => {
                        state.on_sync(rx, et, ot, &em, &om, at_scaled)
                    }
                    ShardMsg::Mlat { rx, t, m, at_scaled } => state.on_mlat(rx, t, &m, at_scaled),
                    ShardMsg::ClockReset { rx } => state.clock_reset(rx),
                    ShardMsg::Stats(reply) => {
                        let _ = reply.send((
                            state.receivers.len(),
                            state.stats_sync_obs,
                            state.stats_solved,
                            state.stats_rejected,
                        ));
                    }
                    ShardMsg::SyncJson(reply) => {
                        let _ = reply.send(state.sync_json());
                    }
                }
            }
        }
    }
}
