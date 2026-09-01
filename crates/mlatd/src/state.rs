//! Per-shard server state. Each shard owns one State and processes its
//! messages in one task; there are no locks. Results leave through a
//! channel to the output task.

use crate::clocksync::PairModel;
use crate::solve::{self, Observation};
use crate::track::TrackFilter;
use mb_core::{Ecef, Geodetic, Icao, C_MPS};
use std::collections::HashMap;
use std::time::Instant;

pub struct ReceiverInfo {
    pub user: String,
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

/// A published fix, fanned out to CSV + SBS + subscribed clients.
pub struct Published {
    pub sbs_line: String,
    pub result_line: String,
}

pub struct State {
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
        time_scale: f64,
        mlat_adsb: bool,
        emit_filtered: bool,
        epoch: (f64, Instant),
    ) -> Self {
        State {
            out: None,
            adsb_pos: HashMap::new(),
            stamp_offset: HashMap::new(),
            mlat_adsb,
            emit_filtered,
            rx_bias: Vec::new(),
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
        let idx = match self.free_slots.pop() {
            Some(i) => {
                self.receivers[i] = info;
                self.rx_bias[i] = RxBias::default();
                self.gens[i] = self.gens[i].wrapping_add(1);
                self.alive[i] = true;
                i
            }
            None => {
                self.receivers.push(info);
                self.rx_bias.push(RxBias::default());
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
                self.pairs.entry((a, b)).or_default().push(ta, tb);
                self.pairs.entry((b, a)).or_default().push(tb, ta);
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
            self.solve_cluster(g.icao, g.df17, local_ref, &conv[i..j]);
            i = j;
        }
    }

    fn solve_cluster(
        &mut self,
        icao: Icao,
        cluster_is_df17: bool,
        local_ref: usize,
        cluster: &[(usize, f64, f64, f64)],
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
                    icao,
                    stamp,
                    csv_line: row,
                    filtered_line,
                    published: Published {
                        sbs_line,
                        result_line,
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
            top.insert(
                r.user.clone(),
                serde_json::json!({ "peers": serde_json::Value::Object(peers) }),
            );
        }
        serde_json::Value::Object(top)
    }
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
            ecef: geo.to_ecef(),
            geo,
            freq_hz: 12e6,
            gps: false,
            jitter_s: 150e-9,
        }
    }

    fn state() -> State {
        State::new(1.0, false, false, (0.0, Instant::now()))
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
}
