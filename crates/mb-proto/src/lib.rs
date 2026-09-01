//! The mlat-server client wire protocol.
//!
//! Everything here is written against the behavior of wiedehopf/mlat-server's
//! `mlat/jsonclient.py` (the fork every open aggregator runs). Where this file
//! and that file disagree, that file wins — and the discrepancy goes into
//! docs/protocol-notes.md with a date.
//!
//! Protocol shape:
//!   1. Client connects over TCP and sends ONE newline-terminated JSON object
//!      (the handshake). No compression yet.
//!   2. Server replies with one JSON line: either an acceptance (carries the
//!      negotiated `compress` method, `motd`, capability flags) or `{"deny": ...}`.
//!   3. All subsequent client→server traffic uses the negotiated framing.
//!      Server→client traffic is newline-delimited JSON regardless.

pub mod framing;
pub mod sbs;

use mb_core::Icao;
use serde::{Deserialize, Serialize};

/// Clock types the server recognizes (mlat/clocktrack.pyx `make_clock`).
/// The strings on the wire are exactly these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClockType {
    #[serde(rename = "radarcape_gps")]
    RadarcapeGps,
    #[serde(rename = "beast")]
    Beast,
    #[serde(rename = "radarcape_12mhz")]
    Radarcape12Mhz,
    #[serde(rename = "sbs")]
    Sbs,
    #[serde(rename = "dump1090")]
    Dump1090,
    #[serde(rename = "unknown")]
    Unknown,
}

impl ClockType {
    /// Counter frequency in Hz, per clocktrack.pyx.
    pub fn freq_hz(self) -> f64 {
        match self {
            ClockType::RadarcapeGps => 1e9,
            ClockType::Beast | ClockType::Radarcape12Mhz => 12e6,
            ClockType::Sbs => 20e6,
            ClockType::Dump1090 | ClockType::Unknown => 12e6,
        }
    }

    /// The server's tolerance assumptions (max_freq_error, jitter) — these are
    /// the bounds a realistic simulated clock must stay inside.
    pub fn server_assumed_max_freq_error(self) -> f64 {
        match self {
            ClockType::RadarcapeGps => 1e-6,
            ClockType::Beast | ClockType::Radarcape12Mhz => 5e-6,
            ClockType::Sbs => 100e-6,
            ClockType::Dump1090 | ClockType::Unknown => 100e-6,
        }
    }

    pub fn server_assumed_jitter_s(self) -> f64 {
        match self {
            ClockType::RadarcapeGps => 15e-9,
            ClockType::Beast | ClockType::Radarcape12Mhz => 83e-9,
            ClockType::Sbs => 500e-9,
            ClockType::Dump1090 | ClockType::Unknown => 500e-9,
        }
    }
}

/// Compression methods. A synthetic client always offers exactly ONE so the
/// server's choice is forced and pre-generated frames stay valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compress {
    None,
    Zlib,
    Zlib2,
}

/// The handshake line. Field set mirrors mlat-client's; optional fields are
/// omitted (not null) when unset, matching CPython's json.dumps of a dict
/// that simply lacks the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    pub version: u8, // 2 or 3; this implementation sends 3
    pub user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    pub compress: Vec<Compress>,
    pub lat: f64,
    pub lon: f64,
    /// Receiver altitude, METERS (server validates −1000..10000).
    pub alt: f64,
    pub clock_type: ClockType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_results: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_result_format: Option<String>, // "old" | "ecef"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
    /// Client opts in to start_sending/stop_sending steering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selective_traffic: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<bool>,
}

impl Handshake {
    pub fn to_line(&self) -> Vec<u8> {
        let mut v = serde_json::to_vec(self).expect("handshake serializes");
        v.push(b'\n');
        v
    }
}

/// Timestamp as sent on the wire. Units depend on clock_type and are
/// settled empirically in docs/protocol-notes.md. The raw JSON number is
/// carried unchanged (serde_json::Number preserves int-vs-float, which
/// matters: CPython emits `12000000` for an int where a float would be
/// `12000000.0`, and byte-exact capture replay must not launder one into the
/// other).
pub type WireTimestamp = serde_json::Number;

/// Hex-encoded Mode S message body, lowercase, no separators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HexMsg(pub String);

impl HexMsg {
    pub fn from_bytes(b: &[u8]) -> Self {
        HexMsg(hex::encode(b))
    }
}

/// Client → server messages. Serialized as single-key JSON objects, one per
/// line (before framing), exactly as mlat-client does.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMsg {
    #[serde(rename = "sync")]
    Sync {
        et: WireTimestamp,
        ot: WireTimestamp,
        em: HexMsg,
        om: HexMsg,
    },
    #[serde(rename = "mlat")]
    Mlat { t: WireTimestamp, m: HexMsg },
    #[serde(rename = "seen")]
    Seen(Vec<String>),
    #[serde(rename = "lost")]
    Lost(Vec<String>),
    #[serde(rename = "input_connected")]
    InputConnected(String),
    #[serde(rename = "input_disconnected")]
    InputDisconnected(String),
    #[serde(rename = "clock_reset")]
    ClockReset(String),
    #[serde(rename = "clock_jump")]
    ClockJump(String),
    #[serde(rename = "heartbeat")]
    Heartbeat(serde_json::Value),
    #[serde(rename = "rate_report")]
    RateReport(serde_json::Map<String, serde_json::Value>),
}

impl ClientMsg {
    /// One JSON line, newline-terminated (pre-framing form).
    pub fn to_line(&self) -> Vec<u8> {
        let mut v = serde_json::to_vec(self).expect("client msg serializes");
        v.push(b'\n');
        v
    }

    pub fn heartbeat_now() -> Self {
        // mlat-client sends {"heartbeat": {}}; server tolerates extra fields.
        ClientMsg::Heartbeat(serde_json::json!({}))
    }

    pub fn seen(icaos: &[Icao]) -> Self {
        ClientMsg::Seen(icaos.iter().map(|i| i.to_hex()).collect())
    }

    pub fn lost(icaos: &[Icao]) -> Self {
        ClientMsg::Lost(icaos.iter().map(|i| i.to_hex()).collect())
    }
}

/// Server → client messages. `Unknown` is the pressure-release valve: an
/// unrecognized message must never kill a run — it gets logged and becomes a
/// protocol-notes entry instead.
#[derive(Debug, Clone)]
pub enum ServerMsg {
    HandshakeAccept {
        compress: Compress,
        motd: Option<String>,
        heartbeat: bool,
        return_results: bool,
        raw: serde_json::Value,
    },
    Deny(serde_json::Value),
    Result(serde_json::Value),
    StartSending(Vec<String>),
    StopSending(Vec<String>),
    Heartbeat {
        server_time: Option<f64>,
    },
    Unknown(serde_json::Value),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("handshake reply missing 'compress': {0}")]
    BadHandshakeReply(String),
    #[error("framing: {0}")]
    Framing(String),
}

impl ServerMsg {
    /// Parse one server line. Call `parse_handshake_reply` for the first line
    /// instead — the acceptance message isn't tagged.
    pub fn parse_line(line: &[u8]) -> Result<ServerMsg, ProtoError> {
        let v: serde_json::Value = serde_json::from_slice(line)?;
        let obj = match v.as_object() {
            Some(o) => o,
            None => return Ok(ServerMsg::Unknown(v)),
        };
        if let Some(r) = obj.get("result") {
            return Ok(ServerMsg::Result(r.clone()));
        }
        if let Some(d) = obj.get("deny") {
            return Ok(ServerMsg::Deny(d.clone()));
        }
        if let Some(h) = obj.get("heartbeat") {
            let server_time = h.get("server_time").and_then(|t| t.as_f64());
            return Ok(ServerMsg::Heartbeat { server_time });
        }
        if let Some(s) = obj.get("start_sending") {
            return Ok(ServerMsg::StartSending(string_list(s)));
        }
        if let Some(s) = obj.get("stop_sending") {
            return Ok(ServerMsg::StopSending(string_list(s)));
        }
        Ok(ServerMsg::Unknown(v))
    }

    /// The first line the server sends after the client handshake.
    pub fn parse_handshake_reply(line: &[u8]) -> Result<ServerMsg, ProtoError> {
        let v: serde_json::Value = serde_json::from_slice(line)?;
        if v.get("deny").is_some() {
            return Ok(ServerMsg::Deny(v));
        }
        let compress = v
            .get("compress")
            .and_then(|c| c.as_str())
            .and_then(|s| match s {
                "none" => Some(Compress::None),
                "zlib" => Some(Compress::Zlib),
                "zlib2" => Some(Compress::Zlib2),
                _ => None,
            })
            .ok_or_else(|| ProtoError::BadHandshakeReply(v.to_string()))?;
        Ok(ServerMsg::HandshakeAccept {
            compress,
            motd: v.get("motd").and_then(|m| m.as_str()).map(String::from),
            heartbeat: v
                .get("heartbeat")
                .and_then(|b| b.as_bool())
                .unwrap_or(false),
            return_results: v
                .get("return_results")
                .and_then(|b| b.as_bool())
                .unwrap_or(false),
            raw: v,
        })
    }
}

fn string_list(v: &serde_json::Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_shape() {
        let h = Handshake {
            version: 3,
            user: "mb-rx-000".into(),
            uuid: None,
            compress: vec![Compress::None],
            lat: 47.21,
            lon: -1.55,
            alt: 40.0,
            clock_type: ClockType::Dump1090,
            return_results: Some(true),
            return_result_format: None,
            client_version: Some("mlat-bench 0.1.0".into()),
            selective_traffic: None,
            heartbeat: None,
        };
        let line = h.to_line();
        assert_eq!(*line.last().unwrap(), b'\n');
        let v: serde_json::Value = serde_json::from_slice(&line[..line.len() - 1]).unwrap();
        assert_eq!(v["version"], 3);
        assert_eq!(v["compress"], serde_json::json!(["none"]));
        assert_eq!(v["clock_type"], "dump1090");
        assert!(
            v.get("uuid").is_none(),
            "unset optionals must be absent, not null"
        );
    }

    #[test]
    fn client_msg_single_key_objects() {
        let m = ClientMsg::Mlat {
            t: serde_json::Number::from_f64(12345.678).unwrap(),
            m: HexMsg("5d3c6444abcdef".into()),
        };
        let v: serde_json::Value = serde_json::from_slice(&m.to_line()).unwrap();
        assert_eq!(v["mlat"]["m"], "5d3c6444abcdef");

        let s = ClientMsg::Sync {
            et: serde_json::Number::from(1200000000u64),
            ot: serde_json::Number::from(1200600000u64),
            em: HexMsg("8d".into()),
            om: HexMsg("8d".into()),
        };
        let line = String::from_utf8(s.to_line()).unwrap();
        // Integer timestamps must serialize without a decimal point.
        assert!(line.contains("\"et\":1200000000"), "{line}");
    }

    #[test]
    fn server_reply_parsing() {
        let ok = br#"{"compress":"none","heartbeat":true,"return_results":true,"motd":"hi"}"#;
        match ServerMsg::parse_handshake_reply(ok).unwrap() {
            ServerMsg::HandshakeAccept {
                compress,
                motd,
                heartbeat,
                ..
            } => {
                assert_eq!(compress, Compress::None);
                assert_eq!(motd.as_deref(), Some("hi"));
                assert!(heartbeat);
            }
            other => panic!("expected accept, got {other:?}"),
        }

        let deny = br#"{"deny":["bad position"],"reconnect_in":300}"#;
        assert!(matches!(
            ServerMsg::parse_handshake_reply(deny).unwrap(),
            ServerMsg::Deny(_)
        ));

        let hb = br#"{"heartbeat":{"server_time":1756600000.5}}"#;
        match ServerMsg::parse_line(hb).unwrap() {
            ServerMsg::Heartbeat { server_time } => {
                assert_eq!(server_time, Some(1756600000.5))
            }
            other => panic!("{other:?}"),
        }

        let mystery = br#"{"totally_new_field":1}"#;
        assert!(matches!(
            ServerMsg::parse_line(mystery).unwrap(),
            ServerMsg::Unknown(_)
        ));
    }
}
