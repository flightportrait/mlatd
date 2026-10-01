//! Per-shard server state. Each shard owns one State and processes its
//! messages in one task; there are no locks. Results leave through a
//! channel to the output task.

use crate::clocksync::PairModel;
use crate::solve::{self, Observation};
use crate::track::TrackFilter;
use mb_core::{Ecef, Geodetic, Icao, C_MPS};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub struct ReceiverInfo {
    pub user: String,
    /// Process-wide serial, mlat-server's `uid`: what aircraft.json's
    /// tracking_receivers lists.
    pub uid: u64,
    pub uuid: Option<String>,
    pub privacy: bool,
    /// mlat-server's connection_info string, for clients.json.
    pub connection_info: String,
    pub source_ip: String,
    pub source_port: u16,
    pub geo: Geodetic,
    pub ecef: Ecef,
    pub freq_hz: f64,
    pub gps: bool,
    /// Expected timing error fed to the weighted solve, seconds (1σ).
    /// Covers clock jitter plus pair-model slack; per clock type.
    pub jitter_s: f64,
    /// When the connection last read anything from its feeder (scaled
    /// output clock, f64 bits), heartbeats included. Written by the
    /// connection, read when another connection claims the same user.
    pub last_read: Arc<AtomicU64>,
}

/// A connection that has read from its feeder this recently is live; a
/// same-user connection is refused rather than let in over it. Real
/// clients send at least a heartbeat every 30 s: two missed, plus slack.
const CONNECTION_LIVE_S: f64 = 65.0;

/// A receiver slot plus the generation it was issued with. Slot indexes
/// are reused across reconnects; the generation tells stale holders apart.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct RxRef {
    pub idx: usize,
    pub gen: u32,
}

/// Per-aircraft publication state. Ports of mlat-server's tail-control
/// heuristics (mlattrack.py): warm starts, solve backoff, and an
/// accuracy-scaled output rate.
#[derive(Clone, Copy, Default)]
struct Track {
    last_pos: Option<Geodetic>,
    last_time_scaled: f64,
    last_attempt_scaled: f64,
    /// Last attempt at a track-gated 4-receiver solve (four_rx_consistent).
    /// Kept apart from `last_attempt_scaled` so a refused 4-receiver fix
    /// does not hold back the next frame, which more receivers may have
    /// heard.
    last_gated_attempt_scaled: f64,
    /// Consecutive speed-gate rejections. A long run means the track itself
    /// is wrong (locked onto an early bad fix); the gate then resets the
    /// track instead of suppressing a correct stream.
    speed_rejects: u32,
}

struct SyncPoint {
    created: Instant,
    /// (receiver idx, corrected even transmit time, corrected odd transmit time)
    reporters: Vec<(usize, f64, f64)>,
}

struct Group {
    created: Instant,
    icao: Icao,
    df17: bool,
    /// (receiver idx, arrival time in that receiver's clock s, output clock
    /// at insertion). The insertion stamp is stored because one content key
    /// can hold several distinct transmissions; see solve_group.
    entries: Vec<(usize, f64, f64)>,
}

/// Learned per-receiver systematic timing bias (not an mlat-server port).
/// Wrong reported coordinates, cable/processing delay, and altitude error
/// all appear as a stable signed solve residual for that receiver. A slow
/// EMA over well-observed solves learns the bias; the solver subtracts it.
#[derive(Clone, Copy, Default)]
struct RxBias {
    bias_s: f64,
    /// EMA of |residual − bias| — the receiver's non-correctable scatter.
    /// Above QUARANTINE_MAD_S the receiver is excluded from solves (but its
    /// bias keeps training, so a recovered sensor re-admits itself). An
    /// adaptive replacement for mlat-server's manual blacklist. Measured on
    /// LocaRDS: sensor os-495 appeared in ghost fixes at 13 %, the other
    /// sensors at ~0.5 %.
    mad_s: f64,
    n: u32,
}

const QUARANTINE_MAD_S: f64 = 1.5e-6;
const QUARANTINE_MIN_N: u32 = 30;

/// An aircraft not heard for this long leaves every per-aircraft map:
/// the export entry, its track, altitude, own-position reference and
/// smoothing filter. Nothing reads older state (warm start 60 s, dof rule
/// 30 s, INTEREST_S 60 s), and mlat-server's aircraft.json lists live
/// aircraft only. At 3600 s a busy network held 16,000+ aircraft per
/// status tick; the solver-side maps had no expiry at all and grew for
/// the life of the process.
const AC_EXPIRE_S: f64 = 300.0;
/// How often a shard's sweep runs the expiry pass (real seconds).
const EXPIRE_EVERY_S: f64 = 10.0;
/// A receiver counts as hearing / syncing on an aircraft for this long
/// after its last message from it.
const INTEREST_S: f64 = 60.0;
/// A group member farther than this from the fix did not hear that
/// aircraft: the radio horizon from FL450 to a 300 m mast is ~555 km
/// (4/3-earth). Groups key on the raw message, so a short frame (DF4,
/// DF11) that two aircraft happen to share inside the window joins
/// receivers on both; each cluster solves on its own and its fix goes
/// only to the members in range of it.
const RESULT_RANGE_M: f64 = 600_000.0;

/// Per-receiver export bookkeeping (clients.json): what it sent since
/// the last export and which aircraft it is contributing to.
#[derive(Default)]
struct RxLog {
    /// Messages since the last export (mlat-server: message_counter).
    msgs: u64,
    /// icao → last scaled time this receiver reported a sync pair on it.
    sync: HashMap<Icao, f64>,
    /// icao → last scaled time this receiver reported an mlat frame on it.
    mlat: HashMap<Icao, f64>,
    /// Fudged map position (None under privacy), mlat-server's mapLat/Lon.
    map_pos: Option<(f64, f64)>,
}

/// Per-aircraft export bookkeeping (aircraft.json).
#[derive(Default)]
struct AcLog {
    seen: f64,
    /// Sync observations accepted / gate-rejected, decayed 0.8 per export
    /// like mlat-server's sync_good / sync_bad.
    sync_good: f64,
    sync_bad: f64,
    mlat_msgs: u64,
    results: u64,
    /// Published fixes, oldest first: those within FIT_WINDOW_S of the
    /// newest, and never fewer than the last two (heading and speed). The
    /// 4-receiver gate fits the aircraft's motion to them.
    fixes: VecDeque<Fix>,
    /// receiver slot → last scaled time it reported an mlat frame.
    rx_mlat: HashMap<usize, f64>,
    /// receiver slot → last scaled time it reported a sync pair.
    rx_sync: HashMap<usize, f64>,
}

/// A published fix as the track remembers it.
#[derive(Clone, Copy)]
struct Fix {
    /// Scaled time it was published.
    t: f64,
    pos: Geodetic,
    /// The fix's error, m: the larger of the solver's two estimates.
    err_m: f64,
}

/// Where the track expects the aircraft, for the 4-receiver gate.
struct Prediction {
    pos: Geodetic,
    /// Radius the fix may land in, m, before its own error is added.
    reach_m: f64,
    /// 1σ of the prediction itself, m (per axis).
    sigma_m: f64,
}

/// Fixes older than this behind the newest are not fitted: the fit is a
/// straight line, and a turning aircraft leaves a long line behind.
const FIT_WINDOW_S: f64 = 10.0;
/// The shortest span of fixes a velocity is read from; under it the timing
/// noise of two fixes is the velocity.
const FIT_MIN_SPAN_S: f64 = 2.0;
/// Fastest an aircraft is taken to fly, m/s (the speed gate's limit).
const MAX_SPEED_MPS: f64 = 400.0;
/// Hardest an aircraft is taken to turn or change speed, m/s²: 1.5 g, a
/// 55° bank. A straight-line fit misses a turning aircraft by ½·a·τ².
const MAX_ACCEL_MPS2: f64 = 15.0;
/// A track this long without a fix has starved; nothing gates against it.
const TRACK_STALE_S: f64 = 30.0;

/// A fix's error for the track, m. The rescaled estimate alone can come
/// out near zero on a 4-receiver fit; the geometry's bounds it.
fn fix_err_m(sol: &solve::Solution) -> f64 {
    sol.err_est_m.max(sol.err_geom_m)
}

/// A published fix, fanned out to CSV + SBS + subscribed clients.
pub struct Published {
    pub sbs_line: String,
    pub result_line: String,
    /// Connection uids that get result_line: every receiver that heard the
    /// transmission the fix was solved from, as mlat-server's
    /// forward_results does (group.receivers, not only the solving
    /// cluster), within RESULT_RANGE_M of the fix. A broadcast to every
    /// client put foreign traffic on each feeder's local map.
    pub recipients: Arc<[u64]>,
}

impl Published {
    /// True when the connection with this uid should get result_line.
    pub fn is_for(&self, uid: u64) -> bool {
        self.recipients.contains(&uid)
    }
}

pub struct State {
    /// This shard's index, carried on every emitted fix for the output
    /// task's territory gate.
    shard_id: usize,
    /// Where finished rows go (the output task owns all writers and fan-out;
    /// shards never touch files). Lossy try_send: the solver never blocks.
    out: Option<tokio::sync::mpsc::Sender<crate::shard::OutMsg>>,
    /// Last CPR-decoded position per ADS-B aircraft + output-clock stamp —
    /// the self-truth reference (the aircraft's own broadcast position).
    adsb_pos: HashMap<Icao, (Geodetic, f64)>,
    pub mlat_adsb: bool,
    /// Min-tracked offset between the output clock and the reference
    /// receiver's clock. Results are stamped from the solved reference time
    /// plus this offset; arrival-time stamping broke under zlib2's ~1 s batching
    /// (bench: flat 148 m error = 0.7 s × ground speed), and real clients
    /// batch exactly like that. Min over many messages converges to true
    /// transport latency; rises slowly to follow reference-clock drift.
    stamp_offset: HashMap<usize, f64>,
    rx_bias: Vec<RxBias>,
    /// Rolling per-receiver sync counters for the stats push: (accepted,
    /// gate-rejected). Decayed by 0.25 at each stats read, as mlat-server
    /// decays its equivalents.
    rx_sync: Vec<(f64, f64)>,
    rx_log: Vec<RxLog>,
    ac_log: HashMap<Icao, AcLog>,
    pub receivers: Vec<ReceiverInfo>,
    /// Slot generation, bumped on every reuse. Messages from a connection
    /// that lost its slot (reconnect dedupe, disconnect race) carry a stale
    /// generation and are ignored.
    gens: Vec<u32>,
    alive: Vec<bool>,
    free_slots: Vec<usize>,
    by_user: HashMap<String, usize>,
    reference: Option<usize>,
    pairs: HashMap<(usize, usize), PairModel>,
    syncpoints: HashMap<(String, String), SyncPoint>,
    groups: HashMap<String, Group>,
    alts_ft: HashMap<Icao, i32>,
    tracks: HashMap<Icao, Track>,
    filters: HashMap<Icao, TrackFilter>,
    /// Emit alpha-beta-smoothed twins of each row (experimental; benched
    /// losing on real data — kept opt-in).
    emit_filtered: bool,
    // Scaled output clock for accelerated replay (the bench's scoring
    // anchor expects it). At time_scale 1 this is real time.
    t0_real: Instant,
    t0_unix: f64,
    time_scale: f64,
    last_expire: Instant,
    pub stats_solved: u64,
    pub stats_rejected: u64,
    pub stats_sync_obs: u64,
}

impl State {
    pub fn new(
        shard_id: usize,
        time_scale: f64,
        mlat_adsb: bool,
        emit_filtered: bool,
        epoch: (f64, Instant),
    ) -> Self {
        State {
            shard_id,
            out: None,
            adsb_pos: HashMap::new(),
            stamp_offset: HashMap::new(),
            mlat_adsb,
            emit_filtered,
            rx_bias: Vec::new(),
            rx_sync: Vec::new(),
            rx_log: Vec::new(),
            ac_log: HashMap::new(),
            receivers: Vec::new(),
            gens: Vec::new(),
            alive: Vec::new(),
            free_slots: Vec::new(),
            by_user: HashMap::new(),
            reference: None,
            pairs: HashMap::new(),
            syncpoints: HashMap::new(),
            groups: HashMap::new(),
            alts_ft: HashMap::new(),
            tracks: HashMap::new(),
            filters: HashMap::new(),
            // One scaled-clock epoch for the whole process (created in main).
            // Per-shard epochs diverge by (k−1)·startup-gap at speed k;
            // measured as a flat 3 km error at 4×.
            t0_unix: epoch.0,
            t0_real: epoch.1,
            time_scale,
            last_expire: Instant::now(),
            stats_solved: 0,
            stats_rejected: 0,
            stats_sync_obs: 0,
        }
    }

    pub fn set_output(&mut self, tx: tokio::sync::mpsc::Sender<crate::shard::OutMsg>) {
        self.out = Some(tx);
    }

    fn emit(&self, msg: crate::shard::OutMsg) {
        if let Some(tx) = &self.out {
            let _ = tx.try_send(msg); // lossy by design under output pressure
        }
    }

    /// The server's output clock: real time, scaled. At time_scale 1 this is
    /// plain unix time.
    pub fn scaled_now(&self) -> f64 {
        self.t0_unix + self.t0_real.elapsed().as_secs_f64() * self.time_scale
    }

    /// A new receiver's slot; None when the user is already connected and
    /// that connection is live.
    pub fn add_receiver(&mut self, info: ReceiverInfo) -> Option<RxRef> {
        // A reconnect of the same user replaces the old slot when the old
        // connection has gone quiet: real feeders leave half-open sockets
        // behind, and the old connection's late messages die on the
        // generation check. A live one is a second feeder under the same
        // name, and it keeps its slot, as mlat-server keeps it ("User is
        // already connected"). Replacing it made the two take turns: each
        // replaced connection is closed (0.4.7), its feeder reconnects and
        // replaces the other, and every swap threw away that receiver's
        // clock sync.
        if let Some(&old) = self.by_user.get(&info.user) {
            let heard = f64::from_bits(self.receivers[old].last_read.load(Ordering::Relaxed));
            if self.alive[old] && self.scaled_now() - heard < CONNECTION_LIVE_S * self.time_scale {
                return None;
            }
            self.remove_receiver(RxRef {
                idx: old,
                gen: self.gens[old],
            });
        }
        let gps = info.gps;
        let user = info.user.clone();
        let log = RxLog {
            map_pos: map_position(&info),
            ..Default::default()
        };
        let idx = match self.free_slots.pop() {
            Some(i) => {
                self.receivers[i] = info;
                self.rx_bias[i] = RxBias::default();
                self.rx_sync[i] = (0.0, 0.0);
                self.rx_log[i] = log;
                self.gens[i] = self.gens[i].wrapping_add(1);
                self.alive[i] = true;
                i
            }
            None => {
                self.receivers.push(info);
                self.rx_bias.push(RxBias::default());
                self.rx_sync.push((0.0, 0.0));
                self.rx_log.push(log);
                self.gens.push(0);
                self.alive.push(true);
                self.receivers.len() - 1
            }
        };
        self.by_user.insert(user, idx);
        match self.reference {
            None => self.reference = Some(idx),
            Some(r) if gps && !self.receivers[r].gps => self.reference = Some(idx),
            _ => {}
        }
        Some(RxRef {
            idx,
            gen: self.gens[idx],
        })
    }

    pub fn remove_receiver(&mut self, r: RxRef) {
        if !self.live(r) {
            return;
        }
        let rx = r.idx;
        self.alive[rx] = false;
        self.pairs.retain(|(a, b), _| *a != rx && *b != rx);
        self.forget_pending(rx);
        for a in self.ac_log.values_mut() {
            a.rx_mlat.remove(&rx);
            a.rx_sync.remove(&rx);
        }
        let log = &mut self.rx_log[rx];
        log.sync = HashMap::new();
        log.mlat = HashMap::new();
        self.stamp_offset.remove(&rx);
        if self.by_user.get(&self.receivers[rx].user) == Some(&rx) {
            self.by_user.remove(&self.receivers[rx].user);
        }
        self.free_slots.push(rx);
    }

    fn live(&self, r: RxRef) -> bool {
        r.idx < self.receivers.len() && self.alive[r.idx] && self.gens[r.idx] == r.gen
    }

    pub fn clock_reset(&mut self, r: RxRef) {
        if !self.live(r) {
            return;
        }
        self.pairs.retain(|(a, b), _| *a != r.idx && *b != r.idx);
        self.forget_pending(r.idx);
    }

    /// Drop a slot's in-flight timestamps (sync points up to 4 s, groups
    /// for one window). They are in a clock that no longer holds: after a
    /// clock reset, or after a disconnect, when a reconnect of the same
    /// user takes the slot straight back and the liveness check alone
    /// would pair the old times with the new clock.
    fn forget_pending(&mut self, rx: usize) {
        for sp in self.syncpoints.values_mut() {
            sp.reporters.retain(|r| r.0 != rx);
        }
        for g in self.groups.values_mut() {
            g.entries.retain(|e| e.0 != rx);
        }
    }

    /// A sync message from receiver `rx`: the same DF17 even/odd pair seen by
    /// several receivers is the shared event that trains pair clocks.
    pub fn on_sync(
        &mut self,
        r: RxRef,
        et: f64,
        ot: f64,
        em_hex: &str,
        om_hex: &str,
        at_scaled: f64,
    ) {
        if !self.live(r) {
            return;
        }
        let rx = r.idx;
        let (Ok(em), Ok(om)) = (hex::decode(em_hex), hex::decode(om_hex)) else {
            return;
        };
        let (Some(de), Some(do_)) = (
            mb_modes::decode::parse_df17_airborne(&em),
            mb_modes::decode::parse_df17_airborne(&om),
        ) else {
            return;
        };
        if de.icao != do_.icao || de.odd || !do_.odd {
            return;
        }
        self.rx_log[rx].msgs += 1;
        self.rx_log[rx].sync.insert(de.icao, at_scaled);
        {
            let a = self.ac_log.entry(de.icao).or_default();
            a.seen = at_scaled;
            a.rx_sync.insert(rx, at_scaled);
        }
        // Both decodes of the pair — each message gets its own position for
        // the propagation correction (aircraft move ~150 m between them).
        let even = (de.cpr_lat, de.cpr_lon);
        let odd = (do_.cpr_lat, do_.cpr_lon);
        let (Some(pe), Some(po)) = (
            mb_modes::cpr::global_decode_airborne(even, odd, false),
            mb_modes::cpr::global_decode_airborne(even, odd, true),
        ) else {
            return;
        };
        let alt_m = de.alt_ft.unwrap_or(0) as f64 * 0.3048;
        let pos_e = Geodetic {
            lat_deg: pe.0,
            lon_deg: pe.1,
            alt_m,
        }
        .to_ecef();
        let pos_o = Geodetic {
            lat_deg: po.0,
            lon_deg: po.1,
            alt_m,
        }
        .to_ecef();

        let freq = self.receivers[rx].freq_hz;
        let rxe = self.receivers[rx].ecef;
        let te = et / freq - dist(&rxe, &pos_e) / C_MPS;
        let to = ot / freq - dist(&rxe, &pos_o) / C_MPS;

        if let Some(alt) = de.alt_ft {
            self.alts_ft.insert(de.icao, alt);
        }
        // Self-truth reference: what the aircraft itself claims.
        let stamp = at_scaled;
        self.adsb_pos.insert(
            de.icao,
            (
                Geodetic {
                    lat_deg: pe.0,
                    lon_deg: pe.1,
                    alt_m,
                },
                stamp,
            ),
        );
        if self.mlat_adsb {
            // Feed the even DF17 into the mlat grouping path as well: its
            // per-receiver timestamps make ADS-B aircraft multilateratable,
            // and their broadcast position scores the solve (selftruth.csv).
            self.on_mlat(r, et, em_hex, at_scaled);
        }

        let sp = self
            .syncpoints
            .entry((em_hex.to_string(), om_hex.to_string()))
            .or_insert_with(|| SyncPoint {
                created: Instant::now(),
                reporters: Vec::new(),
            });
        // Train every pair this receiver now shares the event with, capped
        // at 15 reporters per syncpoint (mlat-server's MAX_SYNC_AC). Uncapped,
        // pair training grows as k² and dominated CPU at 60 co-hearing
        // receivers; 15 reporters give the models more observations than
        // they need.
        if sp.reporters.len() >= 15 {
            return;
        }
        let others: Vec<(usize, f64, f64)> = sp.reporters.clone();
        sp.reporters.push((rx, te, to));
        for (rx2, te2, to2) in others {
            if rx2 == rx || !self.alive[rx2] {
                continue;
            }
            for (a, b, ta, tb) in [(rx, rx2, te, te2), (rx, rx2, to, to2)] {
                let ok = self.pairs.entry((a, b)).or_default().push(ta, tb);
                self.pairs.entry((b, a)).or_default().push(tb, ta);
                let slot = &mut self.rx_sync[rx];
                let a = self.ac_log.entry(de.icao).or_default();
                if ok {
                    slot.0 += 1.0;
                    a.sync_good += 1.0;
                } else {
                    slot.1 += 1.0;
                    a.sync_bad += 1.0;
                }
                self.stats_sync_obs += 1;
            }
        }
    }

    /// An mlat message: group identical frames across receivers.
    pub fn on_mlat(&mut self, r: RxRef, t_counts: f64, m_hex: &str, at_scaled: f64) {
        if !self.live(r) {
            return;
        }
        let rx = r.idx;
        let Ok(m) = hex::decode(m_hex) else { return };
        let icao = match mb_modes::decode::df_of(&m) {
            Some(17) => {
                if !self.mlat_adsb {
                    return;
                }
                match mb_modes::decode::parse_df17_airborne(&m) {
                    Some(d) => d.icao,
                    None => return,
                }
            }
            Some(4) => {
                let Some((icao, alt)) = mb_modes::decode::parse_df4(&m) else {
                    return;
                };
                if let Some(a) = alt {
                    self.alts_ft.insert(icao, a);
                }
                icao
            }
            Some(11) => match mb_modes::decode::parse_df11(&m) {
                Some(i) => i,
                None => return,
            },
            _ => return,
        };
        self.rx_log[rx].msgs += 1;
        self.rx_log[rx].mlat.insert(icao, at_scaled);
        {
            let a = self.ac_log.entry(icao).or_default();
            a.seen = at_scaled;
            a.mlat_msgs += 1;
            a.rx_mlat.insert(rx, at_scaled);
        }
        let t_s = t_counts / self.receivers[rx].freq_hz;
        let g = self
            .groups
            .entry(m_hex.to_string())
            .or_insert_with(|| Group {
                created: Instant::now(),
                icao,
                df17: m.first().map(|b| b >> 3) == Some(17),
                entries: Vec::new(),
            });
        g.entries.push((rx, t_s, at_scaled));
    }

    /// One receiver's stats-push fields: (peer_count, outlier_percent,
    /// quarantined). Reading decays the rolling counters.
    pub fn receiver_stats(&mut self, r: RxRef) -> Option<(usize, f64, bool)> {
        if !self.live(r) {
            return None;
        }
        let rx = r.idx;
        let peers = self
            .pairs
            .keys()
            .filter(|(a, b)| *a == rx && self.alive[*b])
            .count();
        let (acc, rej) = self.rx_sync[rx];
        let outlier_percent = if acc + rej > 0.0 {
            100.0 * rej / (acc + rej)
        } else {
            0.0
        };
        self.rx_sync[rx] = (acc * 0.25, rej * 0.25);
        let b = self.rx_bias[rx];
        let quarantined = b.n >= QUARANTINE_MIN_N && b.mad_s > QUARANTINE_MAD_S;
        Some((peers, outlier_percent, quarantined))
    }

    pub fn live_receivers(&self) -> usize {
        self.alive.iter().filter(|a| **a).count()
    }

    /// Sweep: solve groups older than the window, expire stale sync points,
    /// and every EXPIRE_EVERY_S drop aircraft not heard for AC_EXPIRE_S.
    pub fn sweep(&mut self, window: std::time::Duration) {
        let now = Instant::now();
        self.syncpoints
            .retain(|_, sp| now.duration_since(sp.created).as_secs_f64() < 4.0);
        if now.duration_since(self.last_expire).as_secs_f64() >= EXPIRE_EVERY_S {
            self.last_expire = now;
            let scaled = self.scaled_now();
            self.expire_at(scaled);
        }

        let ready: Vec<String> = self
            .groups
            .iter()
            .filter(|(_, g)| now.duration_since(g.created) >= window)
            .map(|(k, _)| k.clone())
            .collect();
        for key in ready {
            let g = self.groups.remove(&key).expect("just listed");
            self.solve_group(&g);
        }
    }

    /// Publication gates, ported from mlat-server: solve backoff per
    /// aircraft, a covariance error ceiling, and the accuracy-scaled rate
    /// rule `elapsed/20 < err/max_err → skip`.
    const RESOLVE_BACKOFF_S: f64 = 0.4; // mlat-server uses 0.7; 0.4 keeps more update rate
    const MAX_ERR_M: f64 = 10_000.0;
    /// Throttle scale. mlat-server throttles with err/10 km, but its error
    /// estimates run ~9× above the true error (measured on the lab
    /// scenario), so its effective strictness is ~err_true/1.1 km. The
    /// estimates here are calibrated; this scale matches the effective
    /// strictness, not the written constant.
    const THROTTLE_SCALE_M: f64 = 1_500.0;

    fn solve_group(&mut self, g: &Group) {
        // The reference receiver is elected per group, not globally. A
        // single global reference only serves receivers that co-hear
        // aircraft with it; on continental geometry (LocaRDS, 316 receivers)
        // 2.17 M sync observations produced zero solves. The receivers of
        // one group heard the same transmission, so they are neighbors; the
        // member with the most usable direct pair models to the other
        // members becomes the reference.
        const CLUSTER_SPAN_S: f64 = 2.5e-3;

        let mut members: Vec<usize> = Vec::new();
        for &(rx, _, _) in &g.entries {
            if !members.contains(&rx) {
                members.push(rx);
            }
        }
        if members.len() < 4 {
            return;
        }
        let local_ref = *members
            .iter()
            .max_by_key(|&&cand| {
                members
                    .iter()
                    .filter(|&&other| {
                        other != cand && self.pairs.get(&(other, cand)).is_some_and(|p| p.usable())
                    })
                    .count()
            })
            .expect("nonempty");

        let mut conv: Vec<(usize, f64, f64, f64)> = Vec::new(); // (rx, t_ref, sigma, at_scaled)
        for &(rx, t_s, at_scaled) in &g.entries {
            let t_ref = if rx == local_ref {
                Some((t_s, self.receivers[rx].jitter_s))
            } else if let Some(direct) = self
                .pairs
                .get_mut(&(rx, local_ref))
                .and_then(|p| p.convert(t_s))
            {
                Some(direct)
            } else {
                // Two-hop: route through a cluster member that pairs with
                // both ends. The sigmas add in quadrature, so the detour's
                // extra uncertainty flows into the solve weights.
                let mut best: Option<(f64, f64)> = None;
                for &h in &members {
                    if h == rx || h == local_ref {
                        continue;
                    }
                    let hop1 = self.pairs.get_mut(&(rx, h)).and_then(|p| p.convert(t_s));
                    let Some((t1, s1)) = hop1 else { continue };
                    let hop2 = self
                        .pairs
                        .get_mut(&(h, local_ref))
                        .and_then(|p| p.convert(t1));
                    let Some((t2, s2)) = hop2 else { continue };
                    let sig = (s1 * s1 + s2 * s2).sqrt();
                    if best.is_none_or(|(_, bs)| sig < bs) {
                        best = Some((t2, sig));
                    }
                }
                best
            };
            if let Some((t, sigma)) = t_ref {
                conv.push((rx, t, sigma.max(self.receivers[rx].jitter_s), at_scaled));
            }
        }
        if conv.len() < 4 {
            return;
        }
        conv.sort_by(|a, b| a.1.total_cmp(&b.1));

        let mut i = 0;
        while i < conv.len() {
            let start_t = conv[i].1;
            let mut j = i;
            while j < conv.len() && conv[j].1 - start_t <= CLUSTER_SPAN_S {
                j += 1;
            }
            self.solve_cluster(g.icao, g.df17, local_ref, &conv[i..j], &members);
            i = j;
        }
    }

    /// A 4-receiver fix, with no spare equation to catch a bad receiver, is
    /// published only where the track can put the aircraft: inside the
    /// prediction's own uncertainty and the fix's (3σ, at least 500 m) plus
    /// the reach the motion model leaves open.
    fn four_rx_consistent(&self, pred: &Prediction, sol: &solve::Solution) -> bool {
        const GATE_SIGMAS: f64 = 3.0;
        const GATE_FLOOR_M: f64 = 500.0;
        let sigma = fix_err_m(sol).hypot(pred.sigma_m);
        sol.pos.haversine_m(&pred.pos) <= (GATE_SIGMAS * sigma).max(GATE_FLOOR_M) + pred.reach_m
    }

    /// Where the published fixes put the aircraft at `now`; None when the
    /// track has none, or has starved (TRACK_STALE_S), and a 4-receiver fix
    /// has nothing to agree with.
    ///
    /// A weighted straight-line fit over the last FIT_WINDOW_S of fixes
    /// gives position and velocity together, with the prediction interval
    /// of a line fit: it widens as the forecast reaches past the fixes, so
    /// a refused fix makes the next gate wider, never narrower. A turn is
    /// covered by the acceleration bound, ½·a·τ² from the fit's centre.
    /// Two fixes alone, a few hundred ms apart, carry their timing noise as
    /// a velocity (0.4.6 and 0.4.7 extrapolated those, and refused every
    /// 4-receiver fix after them until the track starved). With no velocity yet, the
    /// fix only has to be reachable from the last one.
    fn track_prediction(&self, icao: Icao, alt_m: f64, now: f64) -> Option<Prediction> {
        let fixes = &self.ac_log.get(&icao)?.fixes;
        let last = *fixes.back()?;
        let horizon = now - last.t;
        if !(0.0..TRACK_STALE_S).contains(&horizon) {
            return None;
        }
        // Local east/north metres around the last fix; longitude taken the
        // short way round, so a track across the antimeridian stays whole.
        let m_lat = 111_320.0;
        let m_lon = m_lat * last.pos.lat_deg.to_radians().cos().max(0.05);
        let local = |p: &Geodetic| {
            let dlon = (p.lon_deg - last.pos.lon_deg + 540.0).rem_euclid(360.0) - 180.0;
            (dlon * m_lon, (p.lat_deg - last.pos.lat_deg) * m_lat)
        };
        let at = |e: f64, n: f64| Geodetic {
            lat_deg: last.pos.lat_deg + n / m_lat,
            lon_deg: (last.pos.lon_deg + e / m_lon + 540.0).rem_euclid(360.0) - 180.0,
            alt_m,
        };
        let reachable = Prediction {
            pos: Geodetic { alt_m, ..last.pos },
            reach_m: MAX_SPEED_MPS * horizon,
            sigma_m: last.err_m,
        };
        let window: Vec<&Fix> = fixes
            .iter()
            .filter(|f| last.t - f.t <= FIT_WINDOW_S)
            .collect();
        if last.t - window[0].t < FIT_MIN_SPAN_S {
            return Some(reachable);
        }
        // Weights 1/σ², σ floored: a 4-receiver solve's own estimate rests
        // on one spare equation and can come out near zero.
        let w = |f: &Fix| 1.0 / f.err_m.max(100.0).powi(2);
        let sw: f64 = window.iter().map(|f| w(f)).sum();
        let t_mid = window.iter().map(|f| w(f) * f.t).sum::<f64>() / sw;
        let sxx: f64 = window.iter().map(|f| w(f) * (f.t - t_mid).powi(2)).sum();
        let (mut e_mid, mut n_mid, mut ve, mut vn) = (0.0, 0.0, 0.0, 0.0);
        for f in &window {
            let (e, n) = local(&f.pos);
            e_mid += w(f) * e / sw;
            n_mid += w(f) * n / sw;
            ve += w(f) * (f.t - t_mid) * e / sxx;
            vn += w(f) * (f.t - t_mid) * n / sxx;
        }
        // Faster than any aircraft: a bad fix is in the window.
        if ve.hypot(vn) > MAX_SPEED_MPS {
            return Some(reachable);
        }
        let tau = now - t_mid;
        Some(Prediction {
            pos: at(e_mid + ve * tau, n_mid + vn * tau),
            reach_m: 0.5 * MAX_ACCEL_MPS2 * tau * tau,
            sigma_m: (1.0 / sw + tau * tau / sxx).sqrt(),
        })
    }

    /// Connection uids of a group's live receivers in range of the fix:
    /// who gets the result.
    fn recipients(&self, members: &[usize], fix: &Geodetic) -> Arc<[u64]> {
        members
            .iter()
            .filter(|&&rx| {
                self.alive[rx] && self.receivers[rx].geo.haversine_m(fix) <= RESULT_RANGE_M
            })
            .map(|&rx| self.receivers[rx].uid)
            .collect()
    }

    /// Drop every trace of aircraft not heard for AC_EXPIRE_S, and the
    /// per-receiver interest older than INTEREST_S. Every write to
    /// alts_ft, adsb_pos, tracks and filters follows a touch of the
    /// aircraft's ac_log entry, so ac_log membership is the one rule. Ran
    /// only from the work-dir export before, so a hub without --work-dir
    /// never expired anything, and the four solver maps never expired at
    /// all.
    fn expire_at(&mut self, now: f64) {
        let fresh = |t: &f64| now - *t < INTEREST_S;
        self.ac_log.retain(|_, a| now - a.seen < AC_EXPIRE_S);
        let alive = &self.alive;
        for a in self.ac_log.values_mut() {
            a.rx_mlat.retain(|rx, t| fresh(t) && alive[*rx]);
            a.rx_sync.retain(|rx, t| fresh(t) && alive[*rx]);
        }
        let live = &self.ac_log;
        self.alts_ft.retain(|k, _| live.contains_key(k));
        self.adsb_pos.retain(|k, _| live.contains_key(k));
        self.tracks.retain(|k, _| live.contains_key(k));
        self.filters.retain(|k, _| live.contains_key(k));
        for (i, log) in self.rx_log.iter_mut().enumerate() {
            if !alive[i] {
                continue;
            }
            log.sync.retain(|_, t| fresh(t));
            log.mlat.retain(|_, t| fresh(t));
        }
    }

    fn solve_cluster(
        &mut self,
        icao: Icao,
        cluster_is_df17: bool,
        local_ref: usize,
        cluster: &[(usize, f64, f64, f64)],
        members: &[usize],
    ) {
        // One observation per receiver: earliest (direct path; any duplicate
        // within a cluster would be multipath in the real world).
        let mut seen = std::collections::HashSet::new();
        let mut obs: Vec<Observation> = Vec::new();
        let mut users: Vec<String> = Vec::new();
        let mut rx_ids: Vec<usize> = Vec::new();
        let mut benched: Vec<(usize, f64)> = Vec::new();
        let mut stamp = f64::INFINITY;
        for &(rx, t, sigma, at_scaled) in cluster {
            if !seen.insert(rx) || !self.alive[rx] {
                continue;
            }
            // Apply the learned systematic bias: residual = predicted −
            // measured, so a positive stable residual means this receiver's
            // effective range is modeled too long — advance its clock reading.
            let b = self.rx_bias[rx];
            if b.n >= QUARANTINE_MIN_N && b.mad_s > QUARANTINE_MAD_S {
                benched.push((rx, t)); // quarantined: scored, not used
                continue;
            }
            obs.push(Observation {
                rx: self.receivers[rx].ecef,
                t_s: t + b.bias_s,
                // Folding the learned residual variance into this weight
                // measured worse on the hostile scenario (109/336/910 vs
                // 105/293/852 m): the pair-model sigma already carries the
                // receiver's scatter, and counting it twice flattens the
                // weights. Scalar bias only.
                err_s: sigma,
            });
            users.push(self.receivers[rx].user.clone());
            rx_ids.push(rx);
            stamp = stamp.min(at_scaled);
        }
        if obs.len() < 4 {
            return;
        }
        // Content-time stamping: solved reference time + min-tracked offset,
        // tracked per reference receiver (each local reference is its own
        // clock domain).
        let t_ref_min = obs.iter().map(|o| o.t_s).fold(f64::INFINITY, f64::min);
        let delta = stamp - t_ref_min;
        let off = match self.stamp_offset.get(&local_ref) {
            None => delta,
            Some(&o) if delta < o => delta, // faster path observed: snap down
            Some(&o) => o + 0.001 * (delta - o), // rise slowly (clock drift)
        };
        self.stamp_offset.insert(local_ref, off);
        let stamp = t_ref_min + off;
        if std::env::var("MB_DEBUG_STAMP").is_ok() && self.stats_solved < 5 {
            let wall = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            eprintln!(
                "STAMP dbg: t_ref_min={t_ref_min:.3} off={off:.3} stamp={stamp:.3} scaled_now={:.3} wall={wall:.3} arrival_at_scaled={:.3}",
                self.scaled_now(),
                cluster.first().map(|c| c.3).unwrap_or(0.0)
            );
        }
        // Cap the solve size (mlat-server MAX_GROUP = 15): beyond ~16
        // receivers the extra observations add little geometry and quadratic
        // solve time. Keep the most precise ones.
        if obs.len() > 16 {
            let mut idx: Vec<usize> = (0..obs.len()).collect();
            idx.sort_by(|&a, &b| obs[a].err_s.total_cmp(&obs[b].err_s));
            idx.truncate(16);
            idx.sort_unstable();
            obs = idx.iter().map(|&i| obs[i]).collect();
            users = idx.iter().map(|&i| users[i].clone()).collect();
            rx_ids = idx.iter().map(|&i| rx_ids[i]).collect();
        }
        let now_scaled = self.scaled_now();
        let track = *self.tracks.entry(icao).or_default();
        let Some(&alt_ft) = self.alts_ft.get(&icao) else {
            return; // no altitude yet (DF11-only so far) — wait for a DF4
        };
        let alt_m = alt_ft as f64 * 0.3048;
        // A 4-receiver solve on a live track is gated by the track (below).
        let gate = if obs.len() == 4 {
            self.track_prediction(icao, alt_m, now_scaled)
        } else {
            None
        };
        let gated = gate.is_some();
        let last_attempt = if gated {
            track
                .last_attempt_scaled
                .max(track.last_gated_attempt_scaled)
        } else {
            track.last_attempt_scaled
        };
        if now_scaled - last_attempt < Self::RESOLVE_BACKOFF_S {
            return;
        }
        // A 4-receiver solve fits 3 unknowns (lat, lon, t_tx; altitude is
        // fixed) to 4 arrivals: one spare equation, too few to leave a bad
        // receiver out (that needs 5). Measured on real data, such solves
        // made most of the ghosts and tail error (74 gross, p99 1.2 km), so
        // they were refused until the track had starved 30 s. On a sparse
        // network most aircraft are heard by exactly 4 receivers, and that
        // refusal froze them to one fix per 30 s.
        // They are solved now and published when they agree with the track
        // (four_rx_consistent, below). An aircraft without a live track
        // starts one from them, as from any fix.
        // A gated attempt stamps its own backoff: were it refused, the
        // shared one would hold back the aircraft's next frame (0.4.6 lost
        // 5-receiver fixes that way).
        {
            let t = self.tracks.get_mut(&icao).expect("entry above");
            if gated {
                t.last_gated_attempt_scaled = now_scaled;
            } else {
                t.last_attempt_scaled = now_scaled;
            }
        }
        // Warm start from the last accepted fix when fresh (< 60 s), as in
        // mlat-server; else start from the receivers' centroid.
        let init = match track.last_pos {
            Some(p) if now_scaled - track.last_time_scaled < 60.0 => Geodetic { alt_m, ..p },
            _ => {
                // Centroid of the observing receivers, by slot. A lookup by
                // user name scanned every slot per member and could land on
                // a freed slot that once held the same name. Averaged in
                // ECEF: a mean of longitudes across the antimeridian lands on
                // the far side of the earth.
                let n = rx_ids.len() as f64;
                let (x, y, z) = rx_ids.iter().fold((0.0, 0.0, 0.0), |(x, y, z), &i| {
                    let e = self.receivers[i].ecef;
                    (x + e.x, y + e.y, z + e.z)
                });
                let c = Ecef {
                    x: x / n,
                    y: y / n,
                    z: z / n,
                }
                .to_geodetic();
                Geodetic { alt_m, ..c }
            }
        };
        let is_df17_group = cluster_is_df17;
        match solve::solve_robust(&obs, alt_m, init) {
            Some(sol) => {
                // Covariance error ceiling + accuracy-scaled output rate:
                // bad fixes only pass after proportionally more silence.
                if sol.err_est_m > Self::MAX_ERR_M {
                    self.stats_rejected += 1;
                    return;
                }
                let elapsed = now_scaled - track.last_time_scaled;
                if track.last_pos.is_some()
                    && elapsed / 20.0 < sol.err_est_m / Self::THROTTLE_SCALE_M
                {
                    self.stats_rejected += 1;
                    return;
                }
                if gate
                    .as_ref()
                    .is_some_and(|g| !self.four_rx_consistent(g, &sol))
                {
                    self.stats_rejected += 1;
                    return;
                }
                // Speed continuity: a fix that implies > 400 m/s against a
                // recent accepted fix is a ghost (the diffuse real-data
                // tail). Five consecutive rejections reset the track, so the
                // gate cannot suppress a correct stream behind one bad early
                // fix.
                if let Some(last) = track.last_pos {
                    if elapsed < 30.0
                        && elapsed > 0.05
                        && sol.pos.haversine_m(&last) / elapsed > 400.0
                    {
                        let t = self.tracks.get_mut(&icao).expect("entry above");
                        t.speed_rejects += 1;
                        if t.speed_rejects >= 5 {
                            t.last_pos = None;
                            t.speed_rejects = 0;
                            // The published fixes the track was built on go
                            // too: the stream disagrees with them, and the
                            // next fix starts the track again.
                            if let Some(a) = self.ac_log.get_mut(&icao) {
                                a.fixes.clear();
                            }
                        }
                        self.stats_rejected += 1;
                        return;
                    }
                }
                // A quarantined receiver keeps training against fixes made
                // without it, so a recovered sensor re-admits itself. Only
                // tight fixes: their own position error (≤ 150 m, 0.5 µs)
                // must stay well under the 1.5 µs quarantine line.
                if rx_ids.len() >= 5 && sol.err_est_m < 150.0 {
                    let tx = Geodetic { alt_m, ..sol.pos }.to_ecef();
                    for &(rx, t) in &benched {
                        let b = &mut self.rx_bias[rx];
                        let r =
                            sol.t_tx + dist(&tx, &self.receivers[rx].ecef) / C_MPS - (t + b.bias_s);
                        b.bias_s += 0.02 * r;
                        b.mad_s += 0.02 * ((r - b.bias_s).abs() - b.mad_s);
                    }
                }
                // DF17 (self-truth) fixes are scored against the aircraft's
                // own broadcast position and stay out of results.csv/SBS, so
                // the bench comparison stays like-for-like. Receiver biases
                // still learn from them: ADS-B traffic is abundant.
                if is_df17_group {
                    if sol.err_est_m > Self::MAX_ERR_M {
                        return; // same ceiling as published fixes
                    }
                    if sol.residuals_s.len() == rx_ids.len()
                        && rx_ids.len() >= 5
                        && sol.err_est_m < 500.0
                    {
                        for (i, &rxi) in rx_ids.iter().enumerate() {
                            let b = &mut self.rx_bias[rxi];
                            let k = if b.n < 50 { 0.10 } else { 0.02 };
                            let r = sol.residuals_s[i];
                            b.bias_s += k * r;
                            b.n += 1;
                        }
                    }
                    if let Some((claimed, at)) = self.adsb_pos.get(&icao).copied() {
                        if (now_scaled - at).abs() < 5.0 {
                            let err = sol.pos.haversine_m(&claimed);
                            self.emit(crate::shard::OutMsg::SelfTruth(format!(
                                "{:.3},{},{:.1},{:.1},{}\n",
                                stamp,
                                icao.to_hex(),
                                err,
                                sol.err_est_m,
                                obs.len()
                            )));
                        }
                    }
                    return;
                }
                self.stats_solved += 1;
                // Learn per-receiver bias only from well-observed, full-set
                // solves (residual order matches rx_ids) with a slow EMA: it
                // must absorb the receiver's systematic error, not the
                // geometry of any single fix.
                if sol.residuals_s.len() == rx_ids.len()
                    && rx_ids.len() >= 5
                    && sol.err_est_m < 500.0
                {
                    for (i, &rx) in rx_ids.iter().enumerate() {
                        let b = &mut self.rx_bias[rx];
                        let k = if b.n < 50 { 0.10 } else { 0.02 };
                        let r = sol.residuals_s[i];
                        b.bias_s += k * r;
                        b.mad_s += k * ((r - b.bias_s).abs() - b.mad_s);
                        b.n += 1;
                    }
                }
                let t = self.tracks.get_mut(&icao).expect("entry above");
                t.last_pos = Some(sol.pos);
                t.last_time_scaled = now_scaled;
                t.last_attempt_scaled = now_scaled; // a published fix is an attempt
                t.speed_rejects = 0;
                {
                    let a = self.ac_log.entry(icao).or_default();
                    a.results += 1;
                    a.fixes.push_back(Fix {
                        t: now_scaled,
                        pos: sol.pos,
                        err_m: fix_err_m(&sol),
                    });
                    while a.fixes.len() > 2 && now_scaled - a.fixes[0].t > FIT_WINDOW_S {
                        a.fixes.pop_front();
                    }
                }
                let err_m = sol.err_est_m;
                let row = format!(
                    "{:.3},{},,,{:.5},{:.5},{},{:.1},{},{},\"{}\",{},\n",
                    stamp,
                    icao.to_hex(),
                    sol.pos.lat_deg,
                    sol.pos.lon_deg,
                    alt_ft,
                    err_m,
                    obs.len(),
                    obs.len(),
                    users.join(","),
                    obs.len().saturating_sub(4),
                );
                // Smoothed twin (experimental): same columns, filtered position.
                let filtered_line = if self.emit_filtered {
                    let sm = match self.filters.get_mut(&icao) {
                        Some(f) => f.update(sol.pos, stamp, sol.err_est_m),
                        None => {
                            self.filters.insert(icao, TrackFilter::new(sol.pos, stamp));
                            sol.pos
                        }
                    };
                    Some(format!(
                        "{:.3},{},,,{:.5},{:.5},{},{:.1},{},{},\"{}\",{},\n",
                        stamp,
                        icao.to_hex(),
                        sm.lat_deg,
                        sm.lon_deg,
                        alt_ft,
                        err_m,
                        obs.len(),
                        obs.len(),
                        users.join(","),
                        obs.len().saturating_sub(4),
                    ))
                } else {
                    None
                };
                // Fan out via the output task: CSV, SBS (readsb ingest) and
                // result messages (mlat-client "old" format, field-for-field
                // mlat-server's report_mlat_position_old).
                let (d, tm) = sbs_datetime(stamp);
                let sbs_line = format!(
                    "MSG,3,1,1,{},1,{d},{tm},{d},{tm},,{alt_ft},,,{:.5},{:.5},,,,,,0\r\n",
                    icao.to_hex().to_uppercase(),
                    sol.pos.lat_deg,
                    sol.pos.lon_deg,
                );
                let result_line = format!(
                    "{{\"result\":{{\"@\":{stamp:.3},\"addr\":\"{}\",\"lat\":{:.5},\"lon\":{:.5},\"alt\":{alt_ft},\"callsign\":null,\"squawk\":null,\"hdop\":0.0,\"vdop\":0.0,\"tdop\":0.0,\"gdop\":0.0,\"nstations\":{}}}}}\n",
                    icao.to_hex(),
                    sol.pos.lat_deg,
                    sol.pos.lon_deg,
                    obs.len()
                );
                self.emit(crate::shard::OutMsg::Fix(crate::shard::OutRow {
                    shard: self.shard_id,
                    lat: sol.pos.lat_deg,
                    lon: sol.pos.lon_deg,
                    icao,
                    stamp,
                    csv_line: row,
                    filtered_line,
                    published: Published {
                        sbs_line,
                        result_line,
                        recipients: self.recipients(members, &sol.pos),
                    },
                }));
            }
            None => {
                self.stats_rejected += 1;
            }
        }
    }
}

impl State {
    /// sync.json in mlat-server's shape, so existing monitoring tools work
    /// unchanged: {user: {peers: {peer: [count, .., ppm, ..]}}}.
    pub fn sync_json(&self) -> serde_json::Value {
        let mut top = serde_json::Map::new();
        for (i, r) in self.receivers.iter().enumerate() {
            if !self.alive[i] {
                continue;
            }
            let mut peers = serde_json::Map::new();
            for ((a, b), pm) in &self.pairs {
                if *a == i {
                    let (n, ppm) = pm.status();
                    peers.insert(
                        self.receivers[*b].user.clone(),
                        serde_json::json!([n, 0.1, ppm, 0, 0, 0.0, 0, 0]),
                    );
                }
            }
            let (lat, lon) = match self.rx_log[i].map_pos {
                Some((a, b)) => (serde_json::json!(a), serde_json::json!(b)),
                None => (serde_json::Value::Null, serde_json::Value::Null),
            };
            top.insert(
                r.user.clone(),
                serde_json::json!({
                    "peers": serde_json::Value::Object(peers),
                    "bad_syncs": self.bad_syncs(i),
                    "lat": lat,
                    "lon": lon,
                }),
            );
        }
        serde_json::Value::Object(top)
    }

    /// mlat-server's bad_syncs score for a receiver, on its 0..6 scale.
    /// mlatd has one verdict, the bias quarantine; it maps to the score
    /// the stats push already reports (bad_sync_timeout 60 = 0.4 × 150).
    fn bad_syncs(&self, rx: usize) -> f64 {
        let b = self.rx_bias[rx];
        if b.n >= QUARANTINE_MIN_N && b.mad_s > QUARANTINE_MAD_S {
            0.4
        } else {
            0.0
        }
    }

    /// clients.json and aircraft.json in mlat-server's shape (its
    /// coordinator._write_state), for this shard. Called once per export
    /// period: it decays the per-aircraft sync counters, resets the
    /// per-receiver message counters, and expires stale entries, as the
    /// original does on its 15 s loop.
    pub fn state_json(&mut self) -> (serde_json::Value, serde_json::Value) {
        let now = self.scaled_now();
        self.expire_at(now);

        let mut clients = serde_json::Map::new();
        for (i, r) in self.receivers.iter().enumerate() {
            if !self.alive[i] {
                continue;
            }
            let bad_syncs = self.bad_syncs(i);
            let log = &mut self.rx_log[i];
            let peers: Vec<usize> = self
                .pairs
                .keys()
                .filter(|(a, b)| *a == i && self.alive[*b])
                .map(|(_, b)| *b)
                .collect();
            let bad_peers: Vec<&str> = peers
                .iter()
                .filter(|&&b| {
                    let bb = self.rx_bias[b];
                    bb.n >= QUARANTINE_MIN_N && bb.mad_s > QUARANTINE_MAD_S
                })
                .map(|&b| self.receivers[b].user.as_str())
                .collect();
            let (acc, rej) = self.rx_sync[i];
            let outlier_percent = if acc + rej > 0.0 {
                100.0 * rej / (acc + rej)
            } else {
                0.0
            };
            let mut sync_interest: Vec<String> = log.sync.keys().map(|k| k.to_hex()).collect();
            let mut mlat_interest: Vec<String> = log.mlat.keys().map(|k| k.to_hex()).collect();
            sync_interest.sort();
            mlat_interest.sort();
            clients.insert(
                r.user.clone(),
                serde_json::json!({
                    "user": r.user,
                    "uid": r.uid,
                    "uuid": r.uuid,
                    "coords": format!("{:.6},{:.6}", r.geo.lat_deg, r.geo.lon_deg),
                    "lat": r.geo.lat_deg,
                    "lon": r.geo.lon_deg,
                    "alt": r.geo.alt_m,
                    "privacy": r.privacy,
                    "connection": r.connection_info,
                    "source_ip": r.source_ip,
                    "source_port": r.source_port,
                    "message_rate": (log.msgs as f64 / 15.0).round() as u64,
                    "peer_count": peers.len(),
                    "bad_sync_timeout": (bad_syncs * 150.0).round() as u64,
                    "outlier_percent": (outlier_percent * 10.0).round() / 10.0,
                    "bad_peer_list": format!("{bad_peers:?}"),
                    "sync_interest": sync_interest,
                    "mlat_interest": mlat_interest,
                }),
            );
            log.msgs = 0;
        }

        let mut aircraft = serde_json::Map::new();
        let receivers = &self.receivers;
        for (icao, a) in self.ac_log.iter_mut() {
            let elapsed_seen = ((now - a.seen) * 10.0).round() / 10.0;
            let sync_count = (a.sync_good + a.sync_bad).round();
            let sync_bad_percent = (1000.0 * a.sync_bad / (sync_count + 0.01)).round() / 10.0;
            a.sync_good *= 0.8;
            a.sync_bad *= 0.8;
            let tracking: std::collections::BTreeSet<usize> =
                a.rx_mlat.keys().chain(a.rx_sync.keys()).copied().collect();
            let mut s = serde_json::json!({
                "icao": icao.to_hex().to_uppercase(),
                "elapsed_seen": elapsed_seen,
                "interesting": u8::from(!a.rx_mlat.is_empty()),
                "allow_mlat": 1,
                "tracking": tracking.len(),
                "sync_interest": a.rx_sync.len(),
                "mlat_interest": a.rx_mlat.len(),
                "adsb_seen": a.rx_sync.len(),
                "mlat_message_count": a.mlat_msgs,
                "mlat_result_count": a.results,
                "mlat_kalman_count": 0,
                "sync_count_1min": sync_count,
                "sync_bad_percent": sync_bad_percent,
            });
            let o = s.as_object_mut().expect("object literal");
            if let Some(&Fix { t, pos, .. }) = a.fixes.back() {
                o.insert(
                    "last_result".into(),
                    serde_json::json!(((now - t) * 10.0).round() / 10.0),
                );
                o.insert(
                    "lat".into(),
                    serde_json::json!((pos.lat_deg * 1e4).round() / 1e4),
                );
                o.insert(
                    "lon".into(),
                    serde_json::json!((pos.lon_deg * 1e4).round() / 1e4),
                );
                if let Some(alt) = self.alts_ft.get(icao) {
                    o.insert("alt".into(), serde_json::json!(alt));
                }
                if let Some(&Fix {
                    t: tp, pos: prev, ..
                }) = a.fixes.len().checked_sub(2).and_then(|i| a.fixes.get(i))
                {
                    let dt = t - tp;
                    if dt > 0.0 && dt < 120.0 {
                        let d = prev.haversine_m(&pos);
                        let speed_kt = d / dt / 0.514_444;
                        o.insert(
                            "heading".into(),
                            serde_json::json!(bearing_deg(&prev, &pos).round()),
                        );
                        o.insert("speed".into(), serde_json::json!(speed_kt.round()));
                    }
                }
            }
            if elapsed_seen > 600.0 {
                let uids: Vec<u64> = tracking.iter().map(|rx| receivers[*rx].uid).collect();
                o.insert("tracking_receivers".into(), serde_json::json!(uids));
            }
            aircraft.insert(icao.to_hex().to_uppercase(), s);
        }
        (
            serde_json::Value::Object(clients),
            serde_json::Value::Object(aircraft),
        )
    }
}

/// mlat-server's map-position fudge: None under privacy; else snapped to a
/// 1/20° grid with an offset inside the cell. The offset is a hash of the
/// user name, not a random draw, so the fudged point survives restarts.
fn map_position(info: &ReceiverInfo) -> Option<(f64, f64)> {
    if info.privacy {
        return None;
    }
    let precision = 20.0;
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in info.user.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let unit = |x: u64| (x % 10_000) as f64 / 10_000.0;
    let off_x = -1.0 / precision + unit(h) / precision;
    let off_y = -1.0 / precision + unit(h >> 20) / precision;
    let snap = |v: f64, off: f64| ((v * precision).round() / precision + off) * 100.0;
    Some((
        snap(info.geo.lat_deg, off_x).round() / 100.0,
        snap(info.geo.lon_deg, off_y).round() / 100.0,
    ))
}

/// Initial bearing from a to b, degrees 0..360.
fn bearing_deg(a: &Geodetic, b: &Geodetic) -> f64 {
    let (la1, la2) = (a.lat_deg.to_radians(), b.lat_deg.to_radians());
    let dlon = (b.lon_deg - a.lon_deg).to_radians();
    let y = dlon.sin() * la2.cos();
    let x = la1.cos() * la2.sin() - la1.sin() * la2.cos() * dlon.cos();
    (y.atan2(x).to_degrees() + 360.0) % 360.0
}

/// SBS rows carry date/time strings; emit UTC derived from the unix stamp.
fn sbs_datetime(unix: f64) -> (String, String) {
    let secs = unix as i64;
    let days = secs / 86400;
    let (mut y, mut rem) = (1970i64, days);
    loop {
        let len = if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
            366
        } else {
            365
        };
        if rem < len {
            break;
        }
        rem -= len;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let ml = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0usize;
    while rem >= ml[m] {
        rem -= ml[m];
        m += 1;
    }
    let tod = secs.rem_euclid(86400);
    let frac = ((unix - secs as f64) * 1000.0) as i64;
    (
        format!("{y:04}/{:02}/{:02}", m + 1, rem + 1),
        format!(
            "{:02}:{:02}:{:02}.{frac:03}",
            tod / 3600,
            (tod / 60) % 60,
            tod % 60
        ),
    )
}

fn dist(a: &Ecef, b: &Ecef) -> f64 {
    ((a.x - b.x).powi(2) + (a.y - b.y).powi(2) + (a.z - b.z).powi(2)).sqrt()
}

#[cfg(test)]
mod tests {
    impl State {
        /// add_receiver for a name nobody holds live (rx_info's
        /// connections have never read, so they never are).
        fn add(&mut self, info: ReceiverInfo) -> RxRef {
            self.add_receiver(info).expect("a free name")
        }
    }

    use super::*;

    fn rx_info(user: &str) -> ReceiverInfo {
        let geo = Geodetic {
            lat_deg: 47.0,
            lon_deg: -1.5,
            alt_m: 40.0,
        };
        ReceiverInfo {
            user: user.into(),
            uid: 0,
            uuid: None,
            privacy: false,
            connection_info: String::new(),
            source_ip: "127.0.0.1".into(),
            source_port: 0,
            ecef: geo.to_ecef(),
            geo,
            freq_hz: 12e6,
            gps: false,
            jitter_s: 150e-9,
            last_read: Arc::new(AtomicU64::new(f64::NEG_INFINITY.to_bits())),
        }
    }

    fn state() -> State {
        State::new(0, 1.0, false, false, (0.0, Instant::now()))
    }

    #[test]
    fn slots_are_reused_after_removal() {
        let mut s = state();
        let a = s.add(rx_info("a"));
        let b = s.add(rx_info("b"));
        assert_eq!((a.idx, b.idx), (0, 1));
        s.remove_receiver(a);
        assert_eq!(s.live_receivers(), 1);
        let c = s.add(rx_info("c"));
        assert_eq!(c.idx, a.idx, "freed slot is reused");
        assert_ne!(c.gen, a.gen, "reuse bumps the generation");
        assert_eq!(s.receivers.len(), 2, "no growth on reconnect churn");
    }

    #[test]
    fn same_user_reconnect_replaces_the_old_slot() {
        let mut s = state();
        let a = s.add(rx_info("stn"));
        let b = s.add(rx_info("stn"));
        assert_eq!(s.live_receivers(), 1);
        assert_eq!(b.idx, a.idx, "same user takes the same slot back");
        assert_ne!(b.gen, a.gen);
        // The zombie connection's teardown must not free the new slot.
        s.remove_receiver(a);
        assert_eq!(s.live_receivers(), 1);
    }

    #[test]
    fn stale_generation_messages_are_ignored() {
        let mut s = state();
        let a = s.add(rx_info("a"));
        s.remove_receiver(a);
        let b = s.add(rx_info("b"));
        assert_eq!(b.idx, a.idx);
        s.clock_reset(a); // stale; must not touch b's pairs
        s.on_mlat(a, 1000.0, "20000f1f10ce93", 0.0);
        s.sweep(std::time::Duration::from_secs(0));
        assert_eq!(s.stats_solved + s.stats_rejected, 0);
        let json = s.sync_json();
        let keys: Vec<&String> = json.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["b"], "sync.json lists only live receivers");
        let _ = b;
    }

    #[test]
    fn state_json_reports_clients_and_aircraft_in_mlat_server_shape() {
        let mut s = state();
        let mut a_info = rx_info("alice");
        a_info.uid = 7;
        a_info.uuid = Some("u-a".into());
        let a = s.add(a_info);
        let _b = s.add(rx_info("bob"));
        // A DF11 all-call from 3c6444, heard by alice only.
        s.on_mlat(a, 1000.0, "5d3c6444aabbcc", s.scaled_now());
        let (clients, aircraft) = s.state_json();
        let alice = &clients["alice"];
        assert_eq!(alice["uid"], 7);
        assert_eq!(alice["uuid"], "u-a");
        assert_eq!(alice["coords"], "47.000000,-1.500000");
        assert_eq!(alice["message_rate"], 0); // 1 message / 15 s rounds down
        assert_eq!(alice["mlat_interest"], serde_json::json!(["3c6444"]));
        assert_eq!(alice["sync_interest"], serde_json::json!([]));
        assert_eq!(alice["bad_sync_timeout"], 0);
        assert!(clients["bob"]["mlat_interest"]
            .as_array()
            .unwrap()
            .is_empty());
        let ac = &aircraft["3C6444"];
        assert_eq!(ac["icao"], "3C6444");
        assert_eq!(ac["tracking"], 1);
        assert_eq!(ac["mlat_interest"], 1);
        assert_eq!(ac["interesting"], 1);
        assert_eq!(ac["mlat_message_count"], 1);
        assert_eq!(ac["mlat_result_count"], 0);
        assert!(ac.get("lat").is_none(), "no fix yet, no position");
        assert!(ac.get("tracking_receivers").is_none(), "fresh aircraft");
    }

    #[test]
    fn expiry_empties_every_per_aircraft_map() {
        let mut s = state();
        let rx = s.add(rx_info("a"));
        let now = s.scaled_now();
        s.on_mlat(rx, 1000.0, "5d3c6444aabbcc", now);
        let icao = *s.ac_log.keys().next().expect("logged");
        s.alts_ft.insert(icao, 35_000);
        s.tracks.entry(icao).or_default();
        let pos = rx_info("a").geo;
        s.adsb_pos.insert(icao, (pos, now));
        s.filters.insert(icao, TrackFilter::new(pos, now));

        s.expire_at(now + INTEREST_S - 1.0);
        assert!(
            s.rx_log[rx.idx].mlat.contains_key(&icao),
            "interest under INTEREST_S"
        );

        s.expire_at(now + AC_EXPIRE_S - 1.0);
        assert_eq!(s.ac_log.len(), 1, "still fresh");
        assert!(s.alts_ft.contains_key(&icao) && s.tracks.contains_key(&icao));
        assert!(
            s.rx_log[rx.idx].mlat.is_empty(),
            "interest gone before the aircraft"
        );

        s.expire_at(now + AC_EXPIRE_S);
        assert!(s.ac_log.is_empty());
        assert!(s.alts_ft.is_empty() && s.adsb_pos.is_empty());
        assert!(s.tracks.is_empty() && s.filters.is_empty());
        assert!(s.rx_log[rx.idx].mlat.is_empty());
    }

    #[test]
    fn results_go_to_the_receivers_that_heard_the_message() {
        let mut s = state();
        let mut refs = Vec::new();
        for (i, u) in ["a", "b", "c", "d", "far", "atlanta"].iter().enumerate() {
            let mut info = rx_info(u);
            info.uid = 100 + i as u64;
            if *u == "atlanta" {
                info.geo = Geodetic {
                    lat_deg: 33.75,
                    lon_deg: -84.39,
                    alt_m: 300.0,
                };
                info.ecef = info.geo.to_ecef();
            }
            refs.push(s.add(info));
        }
        // a..d hear one DF11; "far" hears nothing; "atlanta" hears another
        // aircraft that sent the same frame (a collision across the ocean).
        let now = s.scaled_now();
        for r in refs[..4].iter().chain(&refs[5..]) {
            s.on_mlat(*r, 1000.0, "5d3c6444aabbcc", now);
        }
        s.remove_receiver(refs[3]); // gone before the solve
        let g = &s.groups["5d3c6444aabbcc"];
        let mut members: Vec<usize> = g.entries.iter().map(|e| e.0).collect();
        members.dedup();
        let fix = Geodetic {
            lat_deg: 47.3,
            lon_deg: -1.2,
            alt_m: 10_000.0,
        };
        let p = Published {
            sbs_line: String::new(),
            result_line: String::new(),
            recipients: s.recipients(&members, &fix),
        };
        assert!(p.is_for(100) && p.is_for(101) && p.is_for(102));
        assert!(!p.is_for(103), "disconnected receiver");
        assert!(!p.is_for(104), "a receiver that did not hear it");
        assert!(!p.is_for(105), "same frame, another continent");
    }

    #[test]
    fn sync_json_carries_bad_syncs_and_fudged_position() {
        let mut s = state();
        s.add(rx_info("alice"));
        let mut p = rx_info("private");
        p.privacy = true;
        s.add(p);
        let j = s.sync_json();
        assert_eq!(j["alice"]["bad_syncs"], 0.0);
        let lat = j["alice"]["lat"].as_f64().unwrap();
        let lon = j["alice"]["lon"].as_f64().unwrap();
        assert!(
            (lat - 47.0).abs() <= 0.06 && (lon + 1.5).abs() <= 0.06,
            "{lat} {lon}"
        );
        assert!(j["private"]["lat"].is_null() && j["private"]["lon"].is_null());
        // Deterministic: the same user fudges to the same point every time.
        assert_eq!(
            map_position(&rx_info("alice")),
            map_position(&rx_info("alice"))
        );
    }

    /// An eastbound airliner (230 m/s at 55° N) with two published fixes
    /// 5 s apart; the next 4-receiver fix comes 5 s after the last.
    fn four_rx_track(fixes: usize) -> (State, Icao) {
        let mut st = state();
        let icao = Icao(0x4CAD2A);
        let a = st.ac_log.entry(icao).or_default();
        let at = |t: f64| Geodetic {
            lat_deg: 55.0,
            lon_deg: 15.0 + 0.003602 * (t - 100.0),
            alt_m: 10_973.0,
        };
        for t in [100.0, 105.0].into_iter().skip(2 - fixes) {
            a.fixes.push_back(Fix {
                t,
                pos: at(t),
                err_m: 150.0,
            });
        }
        (st, icao)
    }

    fn gate_passes(st: &State, icao: Icao, fix: &solve::Solution, now: f64) -> bool {
        let pred = st
            .track_prediction(icao, fix.pos.alt_m, now)
            .expect("a live track");
        st.four_rx_consistent(&pred, fix)
    }

    fn fix_at(lat_deg: f64, lon_deg: f64, err_est_m: f64) -> solve::Solution {
        solve::Solution {
            pos: Geodetic {
                lat_deg,
                lon_deg,
                alt_m: 10_973.0,
            },
            rms_s: 0.2e-6,
            err_est_m,
            err_geom_m: 0.0,
            iterations: 4,
            t_tx: 0.0,
            residuals_s: vec![],
        }
    }

    #[test]
    fn a_four_receiver_fix_on_the_track_is_published() {
        let (st, icao) = four_rx_track(2);
        // truth at t=110 is lon 15.03602; 200 m off it to the north
        let fix = fix_at(55.0 + 200.0 / 111_320.0, 15.03602, 150.0);
        assert!(gate_passes(&st, icao, &fix, 110.0));
    }

    #[test]
    fn a_four_receiver_fix_in_a_gentle_turn_is_published() {
        let (st, icao) = four_rx_track(2);
        // 700 m off the straight line 5 s after the last fix
        let fix = fix_at(55.0 + 700.0 / 111_320.0, 15.03602, 150.0);
        assert!(gate_passes(&st, icao, &fix, 110.0));
    }

    #[test]
    fn a_four_receiver_ghost_off_the_track_is_refused() {
        let (st, icao) = four_rx_track(2);
        let fix = fix_at(55.0 + 3_000.0 / 111_320.0, 15.03602, 150.0);
        assert!(!gate_passes(&st, icao, &fix, 110.0));
    }

    #[test]
    fn a_four_receiver_fix_without_a_track_starts_one() {
        let (st, icao) = four_rx_track(0);
        assert!(st.track_prediction(icao, 10_973.0, 110.0).is_none());
    }

    #[test]
    fn a_four_receiver_fix_after_one_fix_only_has_to_be_reachable() {
        let (st, icao) = four_rx_track(1);
        // 5 s after the fix at 15.01801: 1.2 km on is reachable, 4 km is not.
        let near = fix_at(55.0, 15.01801 + 1_200.0 / 63_850.0, 150.0);
        let far = fix_at(55.0, 15.01801 + 4_000.0 / 63_850.0, 150.0);
        assert!(gate_passes(&st, icao, &near, 110.0));
        assert!(!gate_passes(&st, icao, &far, 110.0));
    }

    /// The 20-30 s stall: two fixes 0.5 s apart gave 0.4.6/0.4.7 no
    /// velocity, and every 4-receiver fix after them was refused until the
    /// track starved 30 s.
    #[test]
    fn fixes_close_together_do_not_lock_the_track() {
        let mut st = state();
        let icao = Icao(0x33FFDB);
        let a = st.ac_log.entry(icao).or_default();
        for t in [100.0, 100.5] {
            a.fixes.push_back(Fix {
                t,
                pos: Geodetic {
                    lat_deg: 43.0,
                    lon_deg: 12.0 + 120.0 * (t - 100.0) / 81_400.0,
                    alt_m: 4_572.0,
                },
                err_m: 40.0,
            });
        }
        let next = solve::Solution {
            pos: Geodetic {
                lat_deg: 43.0,
                lon_deg: 12.0 + 120.0 * 3.0 / 81_400.0,
                alt_m: 4_572.0,
            },
            ..fix_at(0.0, 0.0, 40.0)
        };
        assert!(gate_passes(&st, icao, &next, 103.0));
    }

    /// A refused fix widens the next gate: the forecast reaches further
    /// past the fixes, and the prediction interval grows with it.
    #[test]
    fn the_gate_widens_while_fixes_are_refused() {
        let (st, icao) = four_rx_track(2);
        let width = |now: f64| {
            let p = st.track_prediction(icao, 10_973.0, now).unwrap();
            3.0 * p.sigma_m + p.reach_m
        };
        assert!(width(106.0) < width(110.0) && width(110.0) < width(120.0));
    }

    /// A 4-receiver-only aircraft at 40 messages/s, turning, with timing
    /// noise and multipath, on a square and on a ridge of receivers: the
    /// track is never left without a fix for long, and ghosts stay out.
    #[test]
    fn a_four_receiver_aircraft_is_never_starved() {
        let square = [(43.2, 12.3), (43.2, 13.1), (42.6, 12.3), (42.6, 13.1)];
        let ridge = [
            (43.20, 12.40),
            (42.90, 12.80),
            (42.55, 13.20),
            (42.95, 12.55),
        ];
        for spots in [&square, &ridge] {
            for noise_s in [100e-9, 300e-9] {
                let (gaps, errs) = fly_four_rx(spots, noise_s, 7919);
                let worst = gaps.iter().copied().fold(0.0, f64::max);
                assert!(
                    worst < 8.0,
                    "longest gap {worst:.1} s ({noise_s:e} s noise)"
                );
                let p99 = errs[(errs.len() - 1) * 99 / 100];
                assert!(p99 < 2_000.0, "p99 error {p99:.0} m ({noise_s:e} s noise)");
            }
        }
    }

    /// Publish gaps (s) and true errors (m), both sorted, of one aircraft
    /// flown for 10 minutes past four receivers.
    fn fly_four_rx(spots: &[(f64, f64); 4], noise_s: f64, seed: u64) -> (Vec<f64>, Vec<f64>) {
        let mut rng = seed;
        let mut uniform = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng >> 11) as f64 / (1u64 << 53) as f64
        };
        let mut st = State::new(0, 1.0, false, false, (1.8e9, Instant::now()));
        let mut ids = Vec::new();
        for (i, (lat, lon)) in spots.iter().enumerate() {
            let mut info = rx_info(&format!("rx{i}"));
            info.geo = Geodetic {
                lat_deg: *lat,
                lon_deg: *lon,
                alt_m: 300.0,
            };
            info.ecef = info.geo.to_ecef();
            ids.push(st.add(info).idx);
        }
        let icao = Icao(0x33FFDB);
        st.alts_ft.insert(icao, 15_000);
        let (clat, clon) = (42.9, 12.7);
        let (mut x, mut y, mut hdg) = (-20_000.0f64, -10_000.0f64, 0.6f64);
        let t0 = st.t0_unix;
        let (mut published, mut errs) = (Vec::new(), Vec::new());
        let dt = 0.25;
        for step in 0..2400 {
            let t = step as f64 * dt;
            // 60 s straight, then 30 s at 3°/s, turning left and right in turn.
            if t % 90.0 >= 60.0 {
                hdg += if (t / 90.0) as i64 % 2 == 0 {
                    0.0524
                } else {
                    -0.0524
                } * dt;
            }
            x += 120.0 * hdg.sin() * dt;
            y += 120.0 * hdg.cos() * dt;
            if x.abs() > 40_000.0 || y.abs() > 40_000.0 {
                hdg += std::f64::consts::PI;
            }
            let truth = Geodetic {
                lat_deg: clat + y / 111_320.0,
                lon_deg: clon + x / (111_320.0 * clat.to_radians().cos()),
                alt_m: 15_000.0 * 0.3048,
            };
            st.t0_unix = t0 + t;
            let now = st.scaled_now();
            st.ac_log.entry(icao).or_default().seen = now;
            let cluster: Cluster = ids
                .iter()
                .map(|&r| {
                    let range = dist(&truth.to_ecef(), &st.receivers[r].ecef) / C_MPS;
                    // Box-Muller timing noise; 4 % of arrivals take a
                    // 1.5-4.5 µs multipath detour.
                    let (u1, u2) = (uniform().max(1e-300), uniform());
                    let jitter = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
                    let detour = if uniform() < 0.04 {
                        1.5e-6 + 3e-6 * uniform()
                    } else {
                        0.0
                    };
                    (
                        r,
                        1000.0 + t + range + noise_s * jitter + detour,
                        noise_s.max(150e-9),
                        now,
                    )
                })
                .collect();
            let solved = st.stats_solved;
            st.solve_cluster(icao, false, ids[0], &cluster, &ids);
            if st.stats_solved > solved {
                published.push(t);
                errs.push(st.tracks[&icao].last_pos.unwrap().haversine_m(&truth));
            }
        }
        let mut gaps: Vec<f64> = published.windows(2).map(|w| w[1] - w[0]).collect();
        gaps.sort_by(f64::total_cmp);
        errs.sort_by(f64::total_cmp);
        (gaps, errs)
    }

    /// (receiver slot, arrival time, sigma, arrival stamp): solve_cluster's input.
    type Cluster = Vec<(usize, f64, f64, f64)>;

    /// Six receivers around Nantes and an aircraft at FL360 among them; the
    /// cluster tuples carry exact arrival times in one clock.
    fn solvable_sky() -> (State, Icao, Cluster) {
        // A clock well past zero: a fresh track's attempt stamps are 0.
        let mut st = State::new(0, 1.0, false, false, (1.8e9, Instant::now()));
        let spots = [
            (47.0, -1.5),
            (47.5, -1.0),
            (46.6, -0.9),
            (47.3, -2.2),
            (46.7, -2.0),
            (47.6, -1.7),
        ];
        let truth = Geodetic {
            lat_deg: 47.1,
            lon_deg: -1.4,
            alt_m: 36_000.0 * 0.3048,
        };
        let mut cluster = Vec::new();
        for (i, (lat, lon)) in spots.iter().enumerate() {
            let mut info = rx_info(&format!("rx{i}"));
            info.geo = Geodetic {
                lat_deg: *lat,
                lon_deg: *lon,
                alt_m: 40.0,
            };
            info.ecef = info.geo.to_ecef();
            let r = st.add(info);
            let t = 100.0 + dist(&truth.to_ecef(), &st.receivers[r.idx].ecef) / mb_core::C_MPS;
            cluster.push((r.idx, t, 100e-9, st.scaled_now()));
        }
        let icao = Icao(0x3C6444);
        st.alts_ft.insert(icao, 36_000);
        st.ac_log.entry(icao).or_default().seen = st.scaled_now();
        (st, icao, cluster)
    }

    #[test]
    fn a_refused_four_receiver_fix_does_not_hold_back_the_next_frame() {
        let (mut st, icao, cluster) = solvable_sky();
        let now = st.scaled_now();
        let truth = Geodetic {
            lat_deg: 47.1,
            lon_deg: -1.4,
            alt_m: 0.0,
        };
        // A live track whose velocity points 20 km away: the 4-receiver fix
        // at the truth disagrees with it and is refused.
        st.tracks.insert(
            icao,
            Track {
                last_pos: Some(truth),
                last_time_scaled: now - 10.0,
                ..Track::default()
            },
        );
        let off = Geodetic {
            lat_deg: 47.28,
            ..truth
        };
        let a = st.ac_log.get_mut(&icao).unwrap();
        for t in [now - 15.0, now - 10.0] {
            a.fixes.push_back(Fix {
                t,
                pos: off,
                err_m: 150.0,
            });
        }
        let members: Vec<usize> = cluster.iter().map(|c| c.0).collect();
        st.solve_cluster(icao, false, 0, &cluster[..4], &members);
        assert_eq!(st.stats_solved, 0, "the ghost-shaped 4-rx fix is refused");
        // The next frame, heard by all six, comes well inside the backoff.
        st.solve_cluster(icao, false, 0, &cluster, &members);
        assert_eq!(st.stats_solved, 1, "the 6-rx fix is not blocked");
    }

    #[test]
    fn four_receiver_attempts_still_back_off() {
        let (mut st, icao, cluster) = solvable_sky();
        let members: Vec<usize> = cluster.iter().map(|c| c.0).collect();
        // First fix of a new aircraft: no track, the 4-rx fix is published.
        st.solve_cluster(icao, false, 0, &cluster[..4], &members);
        assert_eq!(st.stats_solved, 1);
        // Straight after: gated, inside the backoff, not even attempted.
        let rejected = st.stats_rejected;
        st.solve_cluster(icao, false, 0, &cluster[..4], &members);
        assert_eq!((st.stats_solved, st.stats_rejected), (1, rejected));
    }

    #[test]
    fn a_track_across_the_antimeridian_predicts_along_itself() {
        let mut st = state();
        let icao = Icao(0x7C0001);
        let a = st.ac_log.entry(icao).or_default();
        let at = |lon_deg: f64| Geodetic {
            lat_deg: 0.0,
            lon_deg,
            alt_m: 11_000.0,
        };
        for (t, lon) in [(100.0, 179.98), (105.0, 179.99)] {
            a.fixes.push_back(Fix {
                t,
                pos: at(lon),
                err_m: 150.0,
            });
        }
        let fix = fix_at(0.0, -180.0, 150.0);
        let fix = solve::Solution {
            pos: Geodetic {
                lat_deg: 0.0,
                ..fix.pos
            },
            ..fix
        };
        assert!(gate_passes(&st, icao, &fix, 110.0));
    }

    #[test]
    fn a_quarantined_receiver_that_recovers_is_readmitted() {
        let (mut st, icao, cluster) = solvable_sky();
        let members: Vec<usize> = cluster.iter().map(|c| c.0).collect();
        let bad = cluster[5].0;
        st.rx_bias[bad] = RxBias {
            bias_s: 0.0,
            mad_s: 3e-6,
            n: 100,
        };
        // Its timing is clean again; fixes from the other five score it.
        for _ in 0..100 {
            st.tracks.remove(&icao);
            st.solve_cluster(icao, false, 0, &cluster, &members);
        }
        assert!(
            st.rx_bias[bad].mad_s < QUARANTINE_MAD_S,
            "{}",
            st.rx_bias[bad].mad_s
        );
    }

    #[test]
    fn a_second_feeder_under_a_live_name_is_refused() {
        let mut s = state();
        let first = rx_info("shared");
        // Its connection read a heartbeat just now.
        first
            .last_read
            .store(s.scaled_now().to_bits(), Ordering::Relaxed);
        let a = s.add(first);
        assert!(s.add_receiver(rx_info("shared")).is_none());
        assert!(s.live(a), "the first keeps its slot");
    }

    #[test]
    fn a_reconnect_over_a_quiet_connection_takes_the_slot() {
        let mut s = state();
        let old = rx_info("feeder");
        // Last heard 70 s ago: a link that died without closing.
        old.last_read
            .store((s.scaled_now() - 70.0).to_bits(), Ordering::Relaxed);
        let a = s.add(old);
        let b = s
            .add_receiver(rx_info("feeder"))
            .expect("replaces the quiet one");
        assert!(!s.live(a) && s.live(b));
    }

    #[test]
    fn a_reconnect_forgets_the_old_clocks_pending_times() {
        let mut s = state();
        let a = s.add(rx_info("a"));
        let b = s.add(rx_info("b"));
        s.syncpoints.insert(
            ("e".into(), "o".into()),
            SyncPoint {
                created: Instant::now(),
                reporters: vec![(a.idx, 1.0, 1.1), (b.idx, 2.0, 2.1)],
            },
        );
        let b2 = s.add(rx_info("b"));
        assert_eq!(b2.idx, b.idx, "same slot, new clock");
        let sp = &s.syncpoints[&("e".to_string(), "o".to_string())];
        assert_eq!(sp.reporters, vec![(a.idx, 1.0, 1.1)]);
    }

    #[test]
    fn a_track_faster_than_an_aircraft_only_bounds_reach() {
        let mut st = state();
        let icao = Icao(0x4CAD2B);
        let a = st.ac_log.entry(icao).or_default();
        let at = |lon_deg: f64| Geodetic {
            lat_deg: 55.0,
            lon_deg,
            alt_m: 11_000.0,
        };
        for (t, lon) in [(100.0, 15.0), (105.0, 15.8)] {
            // 51 km in 5 s
            a.fixes.push_back(Fix {
                t,
                pos: at(lon),
                err_m: 150.0,
            });
        }
        let p = st.track_prediction(icao, 11_000.0, 110.0).unwrap();
        assert_eq!((p.pos.lon_deg, p.reach_m), (15.8, MAX_SPEED_MPS * 5.0));
    }
}
