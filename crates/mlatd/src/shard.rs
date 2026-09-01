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

use crate::state::{Published, ReceiverInfo, RxRef, State};
use mb_core::Icao;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub enum ShardMsg {
    AddReceiver(ReceiverInfo, oneshot::Sender<RxRef>),
    RemoveReceiver(RxRef),
    Sync {
        rx: RxRef,
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
        rx: RxRef,
        t: f64,
        m: String,
        at_scaled: f64,
    },
    ClockReset(RxRef),
    /// Stats-push fields for one receiver: (peer_count, outlier_percent,
    /// quarantined).
    ReceiverStats(RxRef, oneshot::Sender<Option<(usize, f64, bool)>>),
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
    /// The emitting shard; the output task publishes a fix only when the
    /// solved position lies in this shard's territory, so a border
    /// aircraft solved by two shards (each with its own one-sided receiver
    /// subset) yields exactly the owner's solve.
    pub shard: usize,
    pub lat: f64,
    pub lon: f64,
    pub icao: Icao,
    pub stamp: f64,
    pub csv_line: String,
    pub filtered_line: Option<String>,
    pub published: Published,
}

pub struct ShardHandle {
    pub tx: mpsc::Sender<ShardMsg>,
    pub receivers: std::sync::atomic::AtomicUsize,
    /// Sync observations in the last stats window, written by the stats
    /// task. The load half of the capacity gate: receivers under a busy
    /// sky cost more than the same count under a quiet one.
    pub rate: std::sync::atomic::AtomicU64,
}

/// A partition cell: (level, lat index, lon index). Level 0 cells are
/// `base_deg` wide; each level halves the size. Dense cells split, so the
/// partition adapts to feeder density instead of being tuned for it.
type CellKey = (u8, i16, i16);

/// Splitting never goes below this cell size. The physical floor, found
/// by measurement: 1° cells (smaller than the radius over which receivers
/// co-hear an aircraft) gave every shard a one-sided clipped view of the
/// sky and a systematic ~170 m position bias. Cells must stay at least as
/// large as the co-hearing scale.
const MIN_CELL_DEG: f64 = 2.0;
/// A leaf cell splits only when it cannot fit in one shard at all: at
/// the default receiver cap. Splitting any earlier carves one sky's
/// co-hearing receivers across shards and loses solves — measured on 316
/// real European receivers, where thresholds of 16 and 32 each cost a
/// third of the output. Real continental cells stay whole; only
/// metro-density cells (100+ receivers) subdivide.
const SPLIT_AT: usize = 64;
/// Rate half of the capacity gate: sync observations per 10 s stats
/// window. Far above any healthy shard in the world-scale runs; the gate
/// exists to stop growth into an overloaded shard, not to fine-balance.
const CAP_RATE_PER_WINDOW: u64 = 50_000;

#[derive(Default)]
struct CellMap {
    owner: std::collections::HashMap<CellKey, usize>,
    rx_count: std::collections::HashMap<CellKey, usize>,
    split: std::collections::HashSet<CellKey>,
}

pub struct Router {
    shards: Vec<Arc<ShardHandle>>,
    /// Base (level 0) cell size in degrees. Splitting goes finer on its
    /// own; the flag exists as an override, not a requirement.
    base_deg: f64,
    cap: usize,
    map: std::sync::Mutex<CellMap>,
}

impl Router {
    pub fn new(shards: Vec<Arc<ShardHandle>>, base_deg: f64, cap: usize) -> Self {
        Router {
            shards,
            base_deg,
            cap,
            map: std::sync::Mutex::new(CellMap::default()),
        }
    }

    fn cell_size(&self, level: u8) -> f64 {
        self.base_deg / f64::from(1u32 << level)
    }

    fn cell_at(&self, level: u8, lat: f64, lon: f64) -> CellKey {
        let size = self.cell_size(level);
        (
            level,
            (lat / size).floor() as i16,
            (lon / size).floor() as i16,
        )
    }

    /// True when a cell at this level may split further.
    fn splittable(&self, level: u8) -> bool {
        self.cell_size(level + 1) >= MIN_CELL_DEG
    }

    /// Deepest unsplit cell containing the point.
    fn leaf_at(&self, map: &CellMap, lat: f64, lon: f64) -> CellKey {
        let mut level = 0u8;
        loop {
            let key = self.cell_at(level, lat, lon);
            if !map.split.contains(&key) {
                return key;
            }
            level += 1;
        }
    }

    /// The shard that effectively owns a point: the leaf cell's mapping,
    /// else the nearest mapped ancestor (a split parent keeps owning the
    /// areas its children have not claimed yet).
    fn owner_at(&self, map: &CellMap, lat: f64, lon: f64) -> Option<usize> {
        let leaf = self.leaf_at(map, lat, lon);
        for level in (0..=leaf.0).rev() {
            if let Some(&sh) = map.owner.get(&self.cell_at(level, lat, lon)) {
                return Some(sh);
            }
        }
        None
    }

    fn has_capacity(&self, shard: usize) -> bool {
        let h = &self.shards[shard];
        h.receivers.load(std::sync::atomic::Ordering::Relaxed) < self.cap
            && h.rate.load(std::sync::atomic::Ordering::Relaxed) < CAP_RATE_PER_WINDOW
    }

    fn load_of(&self, shard: usize) -> (u64, usize) {
        let h = &self.shards[shard];
        (
            h.rate.load(std::sync::atomic::Ordering::Relaxed),
            h.receivers.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Shard for a receiver at (lat, lon), remembered for the process
    /// lifetime. A new cell joins the shard that owns most of its 8
    /// geometric neighbors; capacity (receivers AND rate) filters the
    /// vote, ties go to the lighter shard, and with no eligible neighbor
    /// the lightest shard starts a region. Measured history behind the
    /// rules: pure least-loaded cut real co-hearing neighborhoods apart
    /// (LocaRDS results −32 %); uncapped affinity fused a dense
    /// 800-receiver world into one shard (coverage 66 → 45 % at 4×).
    pub fn shard_for(&self, lat: f64, lon: f64) -> (usize, Arc<ShardHandle>) {
        let mut map = self.map.lock().unwrap();
        let key = self.leaf_at(&map, lat, lon);
        let idx = match map.owner.get(&key).copied() {
            Some(i) => i,
            None => {
                let size = self.cell_size(key.0);
                let clat = (f64::from(key.1) + 0.5) * size;
                let clon = (f64::from(key.2) + 0.5) * size;
                let mut votes: std::collections::HashMap<usize, usize> =
                    std::collections::HashMap::new();
                for dy in -1i8..=1 {
                    for dx in -1i8..=1 {
                        if dx == 0 && dy == 0 {
                            continue;
                        }
                        let nlat = clat + f64::from(dy) * size;
                        let nlon = clon + f64::from(dx) * size;
                        if let Some(n) = self.owner_at(&map, nlat, nlon) {
                            if self.has_capacity(n) {
                                *votes.entry(n).or_insert(0) += 1;
                            }
                        }
                    }
                }
                let i = votes
                    .into_iter()
                    .max_by_key(|&(shard, n)| {
                        let (rate, rx) = self.load_of(shard);
                        (n, u64::MAX - rate, usize::MAX - rx)
                    })
                    .map(|(shard, _)| shard)
                    // Subdivision is not growth: a split cell's children go
                    // home to the ancestor's shard even when it is full —
                    // splitting a crowded cell must never carve one sky's
                    // co-hearing receivers across shards (measured on real
                    // European data: scattering the children cost a third
                    // of all solves). The capacity gate above only stops a
                    // shard from claiming new ground.
                    .or_else(|| self.owner_at(&map, lat, lon))
                    .unwrap_or_else(|| {
                        (0..self.shards.len())
                            .min_by_key(|&sh| self.load_of(sh))
                            .unwrap_or(0)
                    });
                map.owner.insert(key, i);
                i
            }
        };
        // Split bookkeeping: this cell just gained a receiver. Reconnect
        // churn inflates the count and can only split a cell earlier;
        // splits are one-way and invisible until growth diverges (children
        // inherit the parent's shard through owner_at).
        let count = map.rx_count.entry(key).or_insert(0);
        *count += 1;
        if *count > SPLIT_AT && self.splittable(key.0) {
            map.split.insert(key);
        }
        (idx, self.shards[idx].clone())
    }

    pub fn all(&self) -> &[Arc<ShardHandle>] {
        &self.shards
    }

    /// The shard whose territory contains this point, if any cell around
    /// it is mapped. Used by the output task's territory gate.
    pub fn owner_of_point(&self, lat: f64, lon: f64) -> Option<usize> {
        let map = self.map.lock().unwrap();
        self.owner_at(&map, lat, lon)
    }

    /// Debug view: (cell, owner, receivers-assigned-via-cell), for
    /// partition inspection under MB_DEBUG_PARTITION.
    pub fn partition_dump(&self) -> Vec<(u8, i16, i16, usize, usize)> {
        let map = self.map.lock().unwrap();
        let mut v: Vec<_> = map
            .owner
            .iter()
            .map(|(k, &sh)| (k.0, k.1, k.2, sh, map.rx_count.get(k).copied().unwrap_or(0)))
            .collect();
        v.sort();
        v
    }

    #[cfg(test)]
    fn leaf_level(&self, lat: f64, lon: f64) -> u8 {
        self.leaf_at(&self.map.lock().unwrap(), lat, lon).0
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
                    ShardMsg::RemoveReceiver(rx) => state.remove_receiver(rx),
                    ShardMsg::Sync { rx, et, ot, em, om, at_scaled } => {
                        state.on_sync(rx, et, ot, &em, &om, at_scaled)
                    }
                    ShardMsg::Mlat { rx, t, m, at_scaled } => state.on_mlat(rx, t, &m, at_scaled),
                    ShardMsg::ClockReset(rx) => state.clock_reset(rx),
                    ShardMsg::ReceiverStats(rx, reply) => {
                        let _ = reply.send(state.receiver_stats(rx));
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    fn router(n: usize, cap: usize) -> Router {
        let shards = (0..n)
            .map(|_| {
                let (tx, _rx) = mpsc::channel(1);
                Arc::new(ShardHandle {
                    tx,
                    receivers: AtomicUsize::new(0),
                    rate: AtomicU64::new(0),
                })
            })
            .collect();
        Router::new(shards, 8.0, cap)
    }

    #[test]
    fn neighbor_cells_join_the_same_shard() {
        let r = router(8, 64);
        let (a, _) = r.shard_for(47.0, -1.0);
        let (b, _) = r.shard_for(47.5, 7.5); // adjacent 8° cell to the east
        assert_eq!(a, b, "a new cell joins its neighbor's shard");
    }

    #[test]
    fn dense_cells_split_and_children_inherit() {
        let r = router(4, 1000);
        let (owner, _) = r.shard_for(1.3, 103.8);
        assert_eq!(r.leaf_level(1.3, 103.8), 0);
        for _ in 0..SPLIT_AT {
            r.shard_for(1.3, 103.8);
        }
        assert_eq!(r.leaf_level(1.3, 103.8), 1, "crowded cell splits");
        // A feeder elsewhere in the split square maps one level finer and
        // inherits the parent's shard through the ancestor fallback.
        let (child_owner, _) = r.shard_for(6.9, 100.1);
        assert_eq!(child_owner, owner);
    }

    #[test]
    fn overloaded_shard_stops_absorbing_new_cells() {
        let r = router(2, 1000);
        let (a, _) = r.shard_for(4.0, 4.0);
        r.shards[a]
            .rate
            .store(CAP_RATE_PER_WINDOW, Ordering::Relaxed);
        let (b, _) = r.shard_for(4.0, 12.0); // adjacent cell
        assert_ne!(a, b, "rate gate spills growth to the lighter shard");
        // Feeders in cells the busy shard already owns still land in it.
        let (again, _) = r.shard_for(4.5, 4.5);
        assert_eq!(again, a);
    }
}
