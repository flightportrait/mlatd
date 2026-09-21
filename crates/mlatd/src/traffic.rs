//! Selective traffic per connection: what a receiver is asked to send.
//!
//! A real mlat-client sends nothing until asked. mlatd asks for every
//! aircraft a receiver offers and can cap how many ADS-B aircraft it
//! keeps receiving sync pairs for, as mlat-server does (MAX_SYNC_AC = 15).
//! Off by default: on the 2026-09-21 drill (500 receivers over one
//! country, 390 k sync observations/s uncapped) a cap of 15 cut intake
//! 8× but cost 3 points of coverage and freed no CPU; the sync pairing
//! was not where the time went. Mode-S-only aircraft (the
//! multilateration targets) are never capped.

use std::collections::{HashMap, HashSet};

/// An ADS-B aircraft leaves a receiver's sync set after this long without
/// a sync pair from it.
const SYNC_IDLE_S: f64 = 30.0;
/// A stopped aircraft may be requested again after this long, and only
/// when the sync set has room.
const SUPPRESS_S: f64 = 60.0;

pub struct Traffic {
    cap: usize,
    /// Aircraft this connection has been told to send (start_sending).
    requested: HashSet<String>,
    /// ADS-B aircraft whose sync pairs we keep: icao → last sync time.
    sync: HashMap<String, f64>,
    /// ADS-B aircraft we told the client to stop: icao → when.
    suppressed: HashMap<String, f64>,
}

impl Traffic {
    /// `cap` 0 = unlimited (the pre-0.4 behavior).
    pub fn new(cap: usize) -> Self {
        Traffic {
            cap,
            requested: HashSet::new(),
            sync: HashMap::new(),
            suppressed: HashMap::new(),
        }
    }

    fn has_room(&mut self, now: f64) -> bool {
        if self.cap == 0 {
            return true;
        }
        self.sync.retain(|_, t| now - *t < SYNC_IDLE_S);
        self.sync.len() < self.cap
    }

    /// The client offers an aircraft (seen list or rate report). True when
    /// it should be started now; false if it already is, or if it was
    /// stopped for the cap and there is no room yet.
    pub fn offered(&mut self, icao: &str, now: f64) -> bool {
        if let Some(&since) = self.suppressed.get(icao) {
            if now - since < SUPPRESS_S || !self.has_room(now) {
                return false;
            }
            self.suppressed.remove(icao);
        }
        self.requested.insert(icao.to_string())
    }

    /// A sync pair arrived for an ADS-B aircraft. True when it should be
    /// forwarded; false when the cap is full and the client should be told
    /// to stop sending this aircraft (the caller sends stop_sending).
    pub fn on_sync(&mut self, icao: &str, now: f64) -> bool {
        if self.cap == 0 {
            return true;
        }
        if let Some(t) = self.sync.get_mut(icao) {
            *t = now;
            return true;
        }
        if self.has_room(now) {
            self.sync.insert(icao.to_string(), now);
            return true;
        }
        self.requested.remove(icao);
        self.suppressed.insert(icao.to_string(), now);
        false
    }

    /// The client reports an aircraft gone.
    pub fn lost(&mut self, icao: &str) {
        self.requested.remove(icao);
        self.sync.remove(icao);
    }
}

/// ICAO address of a DF17/DF18 frame given as hex, lowercase; None for
/// anything else. Cheap: no CRC here, the shard validates.
pub fn adsb_icao(hex: &str) -> Option<String> {
    if hex.len() != 28 {
        return None;
    }
    let df = u8::from_str_radix(hex.get(0..2)?, 16).ok()? >> 3;
    if df != 17 && df != 18 {
        return None;
    }
    Some(hex.get(2..8)?.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_sync_aircraft_and_lets_them_rotate() {
        let mut t = Traffic::new(2);
        for a in ["a1", "a2", "a3"] {
            assert!(t.offered(a, 0.0));
        }
        assert!(t.on_sync("a1", 1.0));
        assert!(t.on_sync("a2", 1.0));
        assert!(!t.on_sync("a3", 1.0), "third ADS-B aircraft is stopped");
        assert!(!t.offered("a3", 2.0), "not re-requested while full");
        // a1 goes quiet: room again, but a3 is still in its suppress window.
        assert!(!t.offered("a3", 40.0));
        assert!(t.offered("a3", 61.0), "re-requested once there is room");
        assert!(t.on_sync("a3", 62.0));
    }

    #[test]
    fn mode_s_only_aircraft_are_never_capped() {
        let mut t = Traffic::new(1);
        assert!(t.offered("m1", 0.0));
        assert!(t.offered("m2", 0.0));
        assert!(t.on_sync("a1", 0.0));
        assert!(!t.on_sync("a2", 0.0));
        assert!(
            t.offered("m3", 0.0),
            "no sync ever seen: not an ADS-B cap case"
        );
    }

    #[test]
    fn unlimited_when_cap_is_zero() {
        let mut t = Traffic::new(0);
        for i in 0..100 {
            assert!(t.on_sync(&format!("{i:06x}"), 0.0));
        }
    }

    #[test]
    fn adsb_icao_from_hex() {
        assert_eq!(
            adsb_icao("8d3c6444580f0e0c1e9f2b6a5b1c").as_deref(),
            Some("3c6444")
        );
        assert_eq!(
            adsb_icao("8D3C6444580F0E0C1E9F2B6A5B1C").as_deref(),
            Some("3c6444")
        );
        assert_eq!(adsb_icao("5d3c6444aabbcc"), None);
        assert_eq!(adsb_icao("203c6444580f0e0c1e9f2b6a5b1c"), None);
    }
}
