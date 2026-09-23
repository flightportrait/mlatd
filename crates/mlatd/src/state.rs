//! Per-shard server state. Each shard owns one State and processes its
//! messages in one task; there are no locks. Results leave through a
//! channel to the output task.

use crate::clocksync::PairModel;
use crate::solve::{self, Observation};
use crate::track::TrackFilter;
use mb_core::{Ecef, Geodetic, Icao, C_MPS};
use std::collections::HashMap;
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
}

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

/// Aircraft an export entry remembers: not seen this long, it drops out.
const AC_EXPIRE_S: f64 = 3600.0;
/// A receiver counts as hearing / syncing on an aircraft for this long
/// after its last message from it.
const INTEREST_S: f64 = 60.0;

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
    /// (scaled time, position) of the last two published fixes; two give
    /// heading and speed.
    last_fix: Option<(f64, Geodetic)>,
    prev_fix: Option<(f64, Geodetic)>,
    /// receiver slot → last scaled time it reported an mlat frame.
    rx_mlat: HashMap<usize, f64>,
    /// receiver slot → last scaled time it reported a sync pair.
    rx_sync: HashMap<usize, f64>,
}

/// A published fix, fanned out to CSV + SBS + subscribed clients.
pub struct Published {
    pub sbs_line: String,
    pub result_line: String,
    /// Connection uids that get result_line: every receiver that heard the
    /// transmission the fix was solved from, as mlat-server's
    /// forward_results does (group.receivers, not only the solving
    /// cluster). A broadcast to every client put foreign traffic on each
    /// feeder's local map.
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

    pub fn add_receiver(&mut self, info: ReceiverInfo) -> RxRef {
        // A reconnect of the same user replaces the old slot: real feeders
        // leave half-open sockets behind, and the old connection's late
        // messages die on the generation check.
        if let Some(&old) = self.by_user.get(&info.user) {
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
        RxRef {
            idx,
            gen: self.gens[idx],
        }
    }

    pub fn remove_receiver(&mut self, r: RxRef) {
        if !self.live(r) {
            return;
        }
        let rx = r.idx;
        self.alive[rx] = false;
        self.pairs.retain(|(a, b), _| *a != rx && *b != rx);
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

    /// Sweep: solve groups older than the window, expire stale sync points.
    pub fn sweep(&mut self, window: std::time::Duration) {
        let now = Instant::now();
        self.syncpoints
            .retain(|_, sp| now.duration_since(sp.created).as_secs_f64() < 4.0);

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
        let recipients = self.recipients(&members);
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
            self.solve_cluster(g.icao, g.df17, local_ref, &conv[i..j], &recipients);
            i = j;
        }
    }

    /// Connection uids of a group's live receivers: who gets the result.
    fn recipients(&self, members: &[usize]) -> Arc<[u64]> {
        members
            .iter()
            .filter(|&&rx| self.alive[rx])
            .map(|&rx| self.receivers[rx].uid)
            .collect()
    }

    fn solve_cluster(
        &mut self,
        icao: Icao,
        cluster_is_df17: bool,
        local_ref: usize,
        cluster: &[(usize, f64, f64, f64)],
        recipients: &Arc<[u64]>,
    ) {
        // One observation per receiver: earliest (direct path; any duplicate
        // within a cluster would be multipath in the real world).
        let mut seen = std::collections::HashSet::new();
        let mut obs: Vec<Observation> = Vec::new();
        let mut users: Vec<String> = Vec::new();
        let mut rx_ids: Vec<usize> = Vec::new();
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
                continue; // quarantined: residual scatter says untrustworthy
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
        if now_scaled - track.last_attempt_scaled < Self::RESOLVE_BACKOFF_S {
            return;
        }
        // mlat-server's dof rule (mlattrack: `elapsed > 30 and dof == 0:
        // continue`): a 4-receiver fixed-altitude solve has zero redundancy,
        // so no residual can catch a bad observation. Allow it only when the
        // track is starved. Measured on real data: zero-dof solves produced
        // most of the ghosts and tail error (74 gross, p99 1.2 km).
        if obs.len() == 4 && now_scaled - track.last_time_scaled < 30.0 {
            self.stats_rejected += 1;
            return;
        }
        let Some(&alt_ft) = self.alts_ft.get(&icao) else {
            return; // no altitude yet (DF11-only so far) — wait for a DF4
        };
        let alt_m = alt_ft as f64 * 0.3048;
        self.tracks
            .get_mut(&icao)
            .expect("entry above")
            .last_attempt_scaled = now_scaled;
        // Warm start from the last accepted fix when fresh (< 60 s), as in
        // mlat-server; else start from the receivers' centroid.
        let init = match track.last_pos {
            Some(p) if now_scaled - track.last_time_scaled < 60.0 => Geodetic { alt_m, ..p },
            _ => {
                let n = obs.len() as f64;
                Geodetic {
                    lat_deg: users
                        .iter()
                        .filter_map(|u| self.receivers.iter().find(|r| &r.user == u))
                        .map(|r| r.geo.lat_deg)
                        .sum::<f64>()
                        / n,
                    lon_deg: users
                        .iter()
                        .filter_map(|u| self.receivers.iter().find(|r| &r.user == u))
                        .map(|r| r.geo.lon_deg)
                        .sum::<f64>()
                        / n,
                    alt_m,
                }
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
                        }
                        self.stats_rejected += 1;
                        return;
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
                t.speed_rejects = 0;
                {
                    let a = self.ac_log.entry(icao).or_default();
                    a.results += 1;
                    a.prev_fix = a.last_fix;
                    a.last_fix = Some((now_scaled, sol.pos));
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
                        recipients: recipients.clone(),
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
        let fresh = |t: &f64| now - *t < INTEREST_S;

        let mut clients = serde_json::Map::new();
        for (i, r) in self.receivers.iter().enumerate() {
            if !self.alive[i] {
                continue;
            }
            let bad_syncs = self.bad_syncs(i);
            let log = &mut self.rx_log[i];
            log.sync.retain(|_, t| fresh(t));
            log.mlat.retain(|_, t| fresh(t));
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

        self.ac_log.retain(|_, a| now - a.seen < AC_EXPIRE_S);
        let mut aircraft = serde_json::Map::new();
        let alive = &self.alive;
        let receivers = &self.receivers;
        for (icao, a) in self.ac_log.iter_mut() {
            a.rx_mlat.retain(|rx, t| fresh(t) && alive[*rx]);
            a.rx_sync.retain(|rx, t| fresh(t) && alive[*rx]);
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
            if let Some((t, pos)) = a.last_fix {
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
                if let Some((tp, prev)) = a.prev_fix {
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
        }
    }

    fn state() -> State {
        State::new(0, 1.0, false, false, (0.0, Instant::now()))
    }

    #[test]
    fn slots_are_reused_after_removal() {
        let mut s = state();
        let a = s.add_receiver(rx_info("a"));
        let b = s.add_receiver(rx_info("b"));
        assert_eq!((a.idx, b.idx), (0, 1));
        s.remove_receiver(a);
        assert_eq!(s.live_receivers(), 1);
        let c = s.add_receiver(rx_info("c"));
        assert_eq!(c.idx, a.idx, "freed slot is reused");
        assert_ne!(c.gen, a.gen, "reuse bumps the generation");
        assert_eq!(s.receivers.len(), 2, "no growth on reconnect churn");
    }

    #[test]
    fn same_user_reconnect_replaces_the_old_slot() {
        let mut s = state();
        let a = s.add_receiver(rx_info("stn"));
        let b = s.add_receiver(rx_info("stn"));
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
        let a = s.add_receiver(rx_info("a"));
        s.remove_receiver(a);
        let b = s.add_receiver(rx_info("b"));
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
        let a = s.add_receiver(a_info);
        let _b = s.add_receiver(rx_info("bob"));
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
    fn results_go_to_the_receivers_that_heard_the_message() {
        let mut s = state();
        let mut refs = Vec::new();
        for (i, u) in ["a", "b", "c", "d", "far"].iter().enumerate() {
            let mut info = rx_info(u);
            info.uid = 100 + i as u64;
            refs.push(s.add_receiver(info));
        }
        // a..d hear one DF11; "far" hears nothing.
        let now = s.scaled_now();
        for r in &refs[..4] {
            s.on_mlat(*r, 1000.0, "5d3c6444aabbcc", now);
        }
        s.remove_receiver(refs[3]); // gone before the solve
        let g = &s.groups["5d3c6444aabbcc"];
        let mut members: Vec<usize> = g.entries.iter().map(|e| e.0).collect();
        members.dedup();
        let p = Published {
            sbs_line: String::new(),
            result_line: String::new(),
            recipients: s.recipients(&members),
        };
        assert!(p.is_for(100) && p.is_for(101) && p.is_for(102));
        assert!(!p.is_for(103), "disconnected receiver");
        assert!(!p.is_for(104), "a receiver that did not hear it");
    }

    #[test]
    fn sync_json_carries_bad_syncs_and_fudged_position() {
        let mut s = state();
        s.add_receiver(rx_info("alice"));
        let mut p = rx_info("private");
        p.privacy = true;
        s.add_receiver(p);
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
}
