//! Altitude field encoding. 25-ft (Q=1) paths only — the 100-ft Gillham
//! encoding is for antique transponders our synthetic fleet doesn't fly.

/// AC12: the 12-bit altitude field of DF17 airborne position (TC 9–18).
/// Layout (MSB..LSB): N[10:4], Q=1, N[3:0] where alt_ft = 25·N − 1000.
/// Returns None outside the encodable range.
pub fn ac12(alt_ft: i32) -> Option<u16> {
    // Callers pre-quantize to 25 ft (quantize_25ft); integer division here
    // silently floors any residue, which is fine at MLAT scales.
    let n = (alt_ft + 1000) / 25;
    if !(0..=2047).contains(&n) {
        return None;
    }
    let n = n as u16;
    Some(((n & 0x7F0) << 1) | 0x010 | (n & 0x00F))
}

/// AC13: the 13-bit altitude field of DF0/DF4/DF16/DF20 replies.
/// Layout (MSB..LSB): N[10:5], M=0, N[4], Q=1, N[3:0].
pub fn ac13(alt_ft: i32) -> Option<u16> {
    let n = (alt_ft + 1000) / 25;
    if !(0..=2047).contains(&n) {
        return None;
    }
    let n = n as u16;
    Some(((n & 0x7E0) << 2) | ((n & 0x010) << 1) | 0x010 | (n & 0x00F))
}

/// Quantize a true altitude to the nearest encodable 25-ft step.
pub fn quantize_25ft(alt_ft: f64) -> i32 {
    (((alt_ft + 1000.0) / 25.0).round() as i32) * 25 - 1000
}

/// Decode AC12 (Q-bit path only; Gillham returns None).
pub fn ac12_decode(field: u16) -> Option<i32> {
    if field & 0x010 == 0 {
        return None;
    }
    let n = ((field & 0xFE0) >> 1) | (field & 0x00F);
    Some(25 * n as i32 - 1000)
}

/// Decode AC13 (M=0, Q=1 path only).
pub fn ac13_decode(field: u16) -> Option<i32> {
    if field & 0x040 != 0 || field & 0x010 == 0 {
        return None; // metric (M) or Gillham — not emitted by the bench
    }
    let n = ((field & 0x1F80) >> 2) | ((field & 0x020) >> 1) | (field & 0x00F);
    Some(25 * n as i32 - 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// From the canonical DF17 frame 8D40621D58C382D690C8AC2863A7:
    /// altitude 38000 ft encodes as AC12 = 0xC38.
    #[test]
    fn ac12_golden_38000() {
        assert_eq!(ac12(38000), Some(0xC38));
    }

    #[test]
    fn ac12_edges() {
        assert_eq!(ac12(-1000), Some(0x010)); // N=0, Q only
        assert!(ac12(50175).is_some()); // N=2047
        assert_eq!(ac12(50200), None); // N=2048 overflow
        assert_eq!(ac12(-1025), None);
    }

    #[test]
    fn ac13_layout() {
        // N=1560 (38000 ft): AC13 spreads N around M(=0) and Q(=1).
        // N = 110 0001 1000b → N[10:5]=110000, N[4]=1, N[3:0]=1000
        // field = 110000 0 1 1 1000 = 1 1000 0011 1000 = 0x1838
        assert_eq!(ac13(38000), Some(0x1838));
    }

    #[test]
    fn quantize() {
        assert_eq!(quantize_25ft(38003.2), 38000);
        assert_eq!(quantize_25ft(-987.0), -975);
        assert_eq!(quantize_25ft(12.4), 0);
    }
}
