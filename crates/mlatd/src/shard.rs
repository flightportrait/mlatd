//! Geographic sharding inside one process.
//!
//! The earlier single-mutex design capped at ~800 dense receivers on one
//! core (bench: real time ran at 124 % CPU; 2× load collapsed at a pinned
//! 143 %). Sync pairs and solve clusters only form between receivers that
//! hear the same aircraft, and those are geographic neighbors, so:
//!
//! - Each shard owns a full `State` inside its own task. There are no
//!   locks; connections send parsed messages over an mpsc channel.
//! - A receiver is assigned at handshake by its geographic cell. New cells
//!   join the shard that owns most of their neighbor cells, up to a
//!   capacity cap (see shard_for); regions stay contiguous.
//! - Cross-shard receiver pairs never form. This is the same tradeoff as
//!   mlat-server's --partition, but the boundary follows geography, so it
//!   is thin. A boundary aircraft heard by two shards may be solved by
//!   both; the output task dedupes by (icao, time bucket).
//! - One output task owns the CSV writers and the fan-out channel; shards
//!   send it finished rows. Writers never contend with solving.

use crate::state::{Published, ReceiverInfo, State};
use mb_core::Icao;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub enum ShardMsg {
    AddReceiver(ReceiverInfo, oneshot::Sender<(usize, u32)>),
    RemoveReceiver {
        rx: usize,
        gen: u32,
    },
    Sync {
        rx: usize,
        gen: u32,
        et: f64,
        ot: f64,
        em: String,
        om: String,
        /// Output-clock time at connection read, before shard queueing.
        /// Stamping at shard-processing time lags by the queue depth under
        /// load (bench: flat 2.2 km error at 4×).
        at_scaled: f64,
    },
    Mlat {
        rx: usize,
        gen: u32,
        t: f64,
        m: String,
        at_scaled: f64,
    },
    ClockReset {
        rx: usize,
        gen: u32,
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
    /// Geographic cell size (degrees) and per-shard receiver capacity.
    /// Deployment tunables with a measured tradeoff: 5°/64 suits sparse
    /// continental networks (LocaRDS recovers fully); dense metros under
    /// extreme load prefer 2°. No constant serves every geometry.
    cell_deg: f64,
    cap: usize,
    /// Geographic cell → shard index, sticky from first assignment.
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

    /// Shard for a receiver at (lat, lon): its cell's shard, remembered for
    /// the process lifetime. A new cell joins the shard that owns most of
    /// its 8 neighbor cells; load only breaks ties. Pure least-loaded
    /// assignment scattered adjacent European cells across 20 shards and
    /// cut real co-hearing neighborhoods apart (LocaRDS: results −32 %,
    /// coverage 27 → 18 %). Neighbor affinity keeps regions contiguous.
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
                // shard, but once a shard holds cap receivers the frontier
                // starts a new contiguous region. Uncapped affinity fused a
                // dense 800-receiver world into one shard (coverage
                // 66 → 45 % at 4×); pure least-loaded cut real Europe apart
                // (LocaRDS −32 %).
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
                        let _ = reply.send(state.add_receiver(info));
                    }
                    ShardMsg::RemoveReceiver { rx, gen } => state.remove_receiver(rx, gen),
                    ShardMsg::Sync { rx, gen, et, ot, em, om, at_scaled } => {
                        state.on_sync(rx, gen, et, ot, &em, &om, at_scaled)
                    }
                    ShardMsg::Mlat { rx, gen, t, m, at_scaled } => {
                        state.on_mlat(rx, gen, t, &m, at_scaled)
                    }
                    ShardMsg::ClockReset { rx, gen } => state.clock_reset(rx, gen),
                    ShardMsg::Stats(reply) => {
                        let _ = reply.send((
                            state.live_receivers(),
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
