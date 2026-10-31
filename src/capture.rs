//! `shelly-exporter capture <ip>`: a device's responses as a test fixture, without private data.
//!
//! Support for a new device starts with what it actually sends. The output can be pasted into an
//! issue or saved as `tests/fixtures/devices/<name>.json`; see CONTRIBUTING.md. The configuration
//! is reduced to the device name on purpose: it holds far more than a fixture needs (location,
//! accounts, server names), and a list of fields to hide can only ever cover the fields known
//! today.

use std::time::Duration;

use serde_json::{Value, json};

use crate::source::http::{Client, Fetch, describe, paths};

const TIMEOUT: Duration = Duration::from_secs(5);
/// Example network from RFC 5737.
const FAKE_IP: &str = "192.0.2.1";
const REDACTED: &str = "REDACTED";
/// Keys whose string values describe the network or its owner.
const PRIVATE_KEYS: [&str; 14] = [
    "ssid",
    "bssid",
    "sta_ip",
    "sta_ip6",
    "ipv6",
    "gw",
    "netmask",
    "nameserver",
    "server",
    "host",
    "tz",
    "location",
    "ap_client_ip",
    "ip",
];

/// `02…`: a locally administered address, so it belongs to no vendor and no real device.
fn fake_mac(seed: f64) -> String {
    format!("02{:010X}", seed.to_bits() & 0xFF_FFFF_FFFF)
}

/// Dotted quads in free text → the example address.
fn scrub_ipv4(text: &str) -> String {
    let is_octet =
        |s: &str| !s.is_empty() && s.len() <= 3 && s.parse::<u16>().is_ok_and(|n| n < 256);
    let mut out = String::new();
    let mut token = String::new();
    let flush = |token: &mut String, out: &mut String| {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() == 4 && parts.iter().all(|p| is_octet(p)) {
            out.push_str(FAKE_IP);
        } else {
            out.push_str(token);
        }
        token.clear();
    };
    for ch in text.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            token.push(ch);
        } else {
            flush(&mut token, &mut out);
            out.push(ch);
        }
    }
    flush(&mut token, &mut out);
    out
}

/// Replace the real MAC in every spelling (`A0B1C2D3E4F5`, `a0b1c2d3e4f5`), and its last six
/// digits alone: Gen1 hostnames only carry those (`shellyswitch25-D3E4F5`).
fn scrub_mac(text: &str, real: &str, fake: &str) -> String {
    let mut out = text.to_string();
    let tail = |mac: &str| mac.get(6..).unwrap_or("").to_string();
    for (real, fake) in [
        (real.to_string(), fake.to_string()),
        (tail(real), tail(fake)),
    ] {
        if !real.is_empty() {
            out = out
                .replace(&real, &fake)
                .replace(&real.to_lowercase(), &fake.to_lowercase());
        }
    }
    out
}

/// Remove private data from a device response, recursively.
pub fn scrub(value: &mut Value, real_mac: &str, fake: &str) {
    match value {
        Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                if v.is_string() && PRIVATE_KEYS.contains(&key.as_str()) {
                    *v = json!(REDACTED);
                } else {
                    scrub(v, real_mac, fake);
                }
            }
        }
        Value::Array(list) => list.iter_mut().for_each(|v| scrub(v, real_mac, fake)),
        Value::String(s) => *s = scrub_ipv4(&scrub_mac(s, real_mac, fake)),
        _ => {}
    }
}

/// Fail if a fixture still looks like a real device. Run by the tests over every fixture.
pub fn check_clean(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                if let Some(s) = v.as_str() {
                    if PRIVATE_KEYS.contains(&key.as_str()) && s != REDACTED {
                        return Err(format!("{key} is not redacted: {s}"));
                    }
                    if key == "mac" && !s.starts_with("02") {
                        return Err(format!("mac is not a fake one (02…): {s}"));
                    }
                }
                check_clean(v)?;
            }
            Ok(())
        }
        Value::Array(list) => list.iter().try_for_each(check_clean),
        Value::String(s) if scrub_ipv4(s) != *s && !s.contains(FAKE_IP) => {
            Err(format!("contains an IP address: {s}"))
        }
        _ => Ok(()),
    }
}

/// Only what the exporter reads from the configuration.
fn minimal_config(config: &Value) -> Value {
    let mut out = json!({});
    if let Some(v) = config.get("name").filter(|v| !v.is_null()) {
        out["name"] = v.clone();
    }
    if let Some(device) = config.get("device") {
        let mut d = json!({});
        for key in ["name", "hostname", "mac"] {
            if let Some(v) = device.get(key).filter(|v| !v.is_null()) {
                d[key] = v.clone();
            }
        }
        out["device"] = d;
    }
    out
}

/// Assemble a fixture from raw responses. Separate from the network part for the tests.
pub fn fixture(shelly: Value, status: Value, config: Value, fake: &str) -> Value {
    let real_mac = shelly["mac"].as_str().unwrap_or("").to_uppercase();
    let mut out = json!({
        "shelly": shelly,
        "status": status,
        "config": minimal_config(&config),
    });
    if !real_mac.is_empty() {
        scrub(&mut out, &real_mac, fake);
    }
    // User-chosen device names often name a room or a person.
    for pointer in ["/config/name", "/config/device/name", "/shelly/name"] {
        if let Some(name) = out.pointer_mut(pointer).filter(|n| n.is_string()) {
            *name = json!("example");
        }
    }
    out
}

pub async fn run(host: &str, client: Client) -> Result<String, String> {
    let shelly = client.get(host, "/shelly", None, TIMEOUT).await?;
    let found = describe(host, &shelly).ok_or("no Shelly at this address")?;
    let (status, config) = paths(found.generation);
    let generation = Some(found.generation);
    let status = client.get(host, status, generation, TIMEOUT).await?;
    let config = client.get(host, config, generation, TIMEOUT).await?;
    let fake = fake_mac(crate::model::now());
    let fixture = fixture(shelly, status, config, &fake);
    // Rather no output than private data in an issue.
    check_clean(&fixture)?;
    serde_json::to_string_pretty(&fixture).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw() -> (Value, Value, Value) {
        let shelly = json!({"name": "kitchen-window", "id": "shellyplugsg3-a0b1c2d3e4f5",
            "mac": "A0B1C2D3E4F5", "gen": 3, "fw_id": "20261026-151158/2.1.1-g07e9ad2"});
        let status = json!({
            "sys": {"mac": "A0B1C2D3E4F5", "uptime": 5},
            "wifi": {"sta_ip": "192.168.7.7", "ssid": "HomeNet", "rssi": -60, "bssid": "aa:bb"},
            "mqtt": {"server": "broker.home.example:1883"},
            "note": "connected to 10.1.2.3 via 192.168.7.1, fw 1.14.0",
        });
        let config = json!({"name": "room", "lat": 52.5, "lng": 13.4,
            "mqtt": {"user": "someone"}, "login": {"username": "admin"},
            "device": {"name": "kitchen-window", "mac": "A0B1C2D3E4F5", "fw_id": "x"},
            "location": {"tz": "Europe/Berlin"}});
        (shelly, status, config)
    }

    #[test]
    fn nothing_private_survives() {
        let (shelly, status, config) = raw();
        let f = fixture(shelly, status, config, "02AABBCCDDEE");
        let text = f.to_string();
        for private in [
            "A0B1C2D3E4",
            "HomeNet",
            "192.168",
            "10.1.2.3",
            "broker.home",
            "kitchen-window",
            "aa:bb",
            "52.5",
            "someone",
            "login",
            "Europe",
            "room",
        ] {
            assert!(!text.contains(private), "{private} left in {text}");
        }
        check_clean(&f).unwrap();
    }

    #[test]
    fn the_short_mac_in_a_gen1_hostname_is_replaced_too() {
        // Real form, seen in the output of a Shelly 2.5: only the last six digits of the MAC.
        let shelly = json!({"type": "SHSW-25", "mac": "A0B1C2D3E4F5"});
        let config =
            json!({"device": {"hostname": "shellyswitch25-D3E4F5", "mac": "A0B1C2D3E4F5"}});
        let f = fixture(shelly, json!({}), config, "02AABBCCDDEE");
        assert_eq!(f["config"]["device"]["hostname"], "shellyswitch25-CCDDEE");
        assert!(!f.to_string().contains("D3E4F5"), "{f}");
    }

    #[test]
    fn what_the_exporter_needs_stays() {
        let (shelly, status, config) = raw();
        let f = fixture(shelly, status, config, "02AABBCCDDEE");
        assert_eq!(f["shelly"]["mac"], "02AABBCCDDEE");
        assert_eq!(f["shelly"]["id"], "shellyplugsg3-02aabbccddee");
        assert_eq!(f["shelly"]["gen"], 3);
        assert_eq!(f["shelly"]["fw_id"], "20261026-151158/2.1.1-g07e9ad2");
        assert_eq!(f["status"]["wifi"]["rssi"], -60);
        assert_eq!(f["status"]["sys"]["uptime"], 5);
        assert_eq!(f["config"]["device"]["name"], "example");
        // A firmware version is no IP address.
        assert!(f["status"]["note"].as_str().unwrap().contains("fw 1.14.0"));
    }

    #[test]
    fn the_guard_notices_real_data() {
        // Each of these would pass silently if the guard only ever saw clean files.
        assert!(check_clean(&json!({"mac": "A0B1C2D3E4F5"})).is_err());
        assert!(check_clean(&json!({"wifi": {"ssid": "HomeNet"}})).is_err());
        assert!(check_clean(&json!({"sta_ip": "192.168.7.7"})).is_err());
        assert!(check_clean(&json!({"note": "from 192.168.7.7"})).is_err());
        assert!(check_clean(&json!({"mac": "02AABBCCDDEE", "ssid": "REDACTED"})).is_ok());
    }
}
