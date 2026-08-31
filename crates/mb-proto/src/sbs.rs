//! Minimal SBS/BaseStation "MSG" line parser: enough to score a server's
//! --basestation-listen output.

/// One parsed MSG,3 line. Unscored fields stay unparsed.
#[derive(Debug, Clone, PartialEq)]
pub struct SbsPosition {
    pub icao_hex: String,
    pub alt_ft: Option<f64>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

/// Parse one line; returns None for non-MSG lines or messages without an
/// aircraft ID. Tolerant by design: the SBS "spec" is folklore.
pub fn parse_line(line: &str) -> Option<SbsPosition> {
    let f: Vec<&str> = line.trim_end().split(',').collect();
    if f.first() != Some(&"MSG") || f.len() < 5 {
        return None;
    }
    let icao_hex = f.get(4)?.trim().to_lowercase();
    if icao_hex.is_empty() {
        return None;
    }
    let num = |i: usize| f.get(i).and_then(|s| s.trim().parse::<f64>().ok());
    Some(SbsPosition {
        icao_hex,
        alt_ft: num(11),
        lat: num(14),
        lon: num(15),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_msg3() {
        let line = "MSG,3,1,1,3C6444,1,2026/08/31,12:00:00.000,2026/08/31,12:00:00.000,,35000,,,47.2100,-1.5500,,,,,,0";
        let p = parse_line(line).unwrap();
        assert_eq!(p.icao_hex, "3c6444");
        assert_eq!(p.alt_ft, Some(35000.0));
        assert_eq!(p.lat, Some(47.21));
        assert_eq!(p.lon, Some(-1.55));
    }

    #[test]
    fn ignores_junk() {
        assert!(parse_line("SEL,,496,2286,4CA4E5").is_none());
        assert!(parse_line("").is_none());
        assert!(parse_line("MSG,8,1,1,,1").is_none());
    }
}
