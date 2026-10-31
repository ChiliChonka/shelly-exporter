//! HTTP status responses → readings, per device generation.
//!
//! Units are unified here: energy is always Wh, although Gen1 reports **watt-minutes** and Gen2+
//! reports Wh. New device types usually only need an entry here, see CONTRIBUTING.md.

use serde_json::Value;

use crate::model::{
    COVER_POSITION, COVER_STATE, CURRENT, ENERGY, LIGHT_BRIGHTNESS, Labels, Metric, OUTPUT_ON,
    OVERTEMPERATURE, POWER, Reading, TEMPERATURE, UPTIME, VOLTAGE, WIFI_RSSI,
};

const GEN1_COVER_STATES: [&str; 3] = ["open", "close", "stop"];
const GEN2_COVER_STATES: [&str; 6] = [
    "open",
    "closed",
    "opening",
    "closing",
    "stopped",
    "calibrating",
];

fn add(out: &mut Vec<Reading>, metric: &'static Metric, labels: &Labels, value: Option<&Value>) {
    let v = match value {
        Some(Value::Bool(b)) => Some(f64::from(u8::from(*b))),
        Some(Value::Number(n)) => n.as_f64(),
        _ => None,
    };
    if let Some(v) = v {
        out.push(Reading::new(metric, labels.clone(), v));
    }
}

fn with(base: &Labels, key: &'static str, value: &str) -> Labels {
    let mut l = base.clone();
    l.push((key, value.to_string()));
    l
}

fn is_true(v: &Value, key: &str) -> bool {
    v.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn is_valid(v: &Value) -> bool {
    v.get("is_valid").and_then(Value::as_bool).unwrap_or(true)
}

fn items(v: &Value, key: &str) -> impl Iterator<Item = (usize, Value)> {
    v.get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .enumerate()
}

fn cover_states(out: &mut Vec<Reading>, labels: &Labels, states: &[&str], current: Option<&str>) {
    for s in states {
        let value = f64::from(u8::from(current == Some(*s)));
        out.push(Reading::new(&COVER_STATE, with(labels, "state", s), value));
    }
}

/// Gen1 `/status` (Shelly 2.5 as relay or roller, Dimmer 2, Plug, …).
pub fn gen1(status: &Value, base: &Labels) -> Vec<Reading> {
    let mut out = Vec::new();
    add(&mut out, &UPTIME, base, status.get("uptime"));
    let wifi = status.get("wifi_sta");
    add(&mut out, &WIFI_RSSI, base, wifi.and_then(|w| w.get("rssi")));
    if let Some(tmp) = status.get("tmp").filter(|t| is_valid(t)) {
        add(&mut out, &TEMPERATURE, base, tmp.get("tC"));
    }
    add(
        &mut out,
        &OVERTEMPERATURE,
        base,
        status.get("overtemperature"),
    );
    add(&mut out, &VOLTAGE, base, status.get("voltage"));
    for (i, m) in items(status, "meters") {
        if !is_valid(&m) {
            continue;
        }
        let ch = with(base, "channel", &i.to_string());
        add(&mut out, &POWER, &ch, m.get("power"));
        if let Some(total) = m.get("total").and_then(Value::as_f64) {
            out.push(Reading::new(&ENERGY, ch, total / 60.0)); // watt-minutes → Wh
        }
    }
    for (i, r) in items(status, "relays") {
        let ch = with(base, "channel", &i.to_string());
        add(&mut out, &OUTPUT_ON, &ch, r.get("ison"));
    }
    for (i, r) in items(status, "rollers") {
        let ch = with(base, "channel", &i.to_string());
        // Only a calibrated roller has a meaningful position.
        if is_true(&r, "positioning") {
            add(&mut out, &COVER_POSITION, &ch, r.get("current_pos"));
        }
        let state = r.get("state").and_then(Value::as_str);
        cover_states(&mut out, &ch, &GEN1_COVER_STATES, state);
    }
    for (i, l) in items(status, "lights") {
        let ch = with(base, "channel", &i.to_string());
        add(&mut out, &OUTPUT_ON, &ch, l.get("ison"));
        add(&mut out, &LIGHT_BRIGHTNESS, &ch, l.get("brightness"));
    }
    out
}

/// Gen2+ `Shelly.GetStatus`: components as `switch:0`, `cover:0`, `light:0`, ….
pub fn gen2(status: &Value, base: &Labels) -> Vec<Reading> {
    let mut out = Vec::new();
    let sys = status.get("sys");
    add(&mut out, &UPTIME, base, sys.and_then(|s| s.get("uptime")));
    let wifi = status.get("wifi");
    add(&mut out, &WIFI_RSSI, base, wifi.and_then(|w| w.get("rssi")));
    let Some(components) = status.as_object() else {
        return out;
    };
    for (key, comp) in components {
        let (kind, index) = key.split_once(':').unwrap_or((key, ""));
        if !matches!(kind, "switch" | "cover" | "light") || !comp.is_object() {
            continue;
        }
        let ch = with(base, "channel", index);
        add(&mut out, &POWER, &ch, comp.get("apower"));
        add(&mut out, &VOLTAGE, &ch, comp.get("voltage"));
        add(&mut out, &CURRENT, &ch, comp.get("current"));
        let energy = comp.get("aenergy").and_then(|a| a.get("total"));
        add(&mut out, &ENERGY, &ch, energy);
        let temperature = comp.get("temperature").and_then(|t| t.get("tC"));
        add(&mut out, &TEMPERATURE, &ch, temperature);
        let overtemp = comp
            .get("errors")
            .and_then(Value::as_array)
            .is_some_and(|e| e.iter().any(|x| x == "overtemp"));
        out.push(Reading::new(
            &OVERTEMPERATURE,
            ch.clone(),
            f64::from(u8::from(overtemp)),
        ));
        if matches!(kind, "switch" | "light") {
            add(&mut out, &OUTPUT_ON, &ch, comp.get("output"));
        }
        if kind == "light" {
            add(&mut out, &LIGHT_BRIGHTNESS, &ch, comp.get("brightness"));
        }
        if kind == "cover" {
            if is_true(comp, "pos_control") {
                add(&mut out, &COVER_POSITION, &ch, comp.get("current_pos"));
            }
            let state = comp.get("state").and_then(Value::as_str);
            cover_states(&mut out, &ch, &GEN2_COVER_STATES, state);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::http::tests::{fixture, value};
    use serde_json::json;

    fn base() -> Labels {
        vec![("device", "test".to_string()), ("mac", "AA".to_string())]
    }

    #[test]
    fn shelly_25_as_roller() {
        let s = gen1(&fixture("shelly-25-roller")["status"], &base());
        // Watt-minutes → Wh: 4355 Wm are 72.583 Wh, not 4355 Wh.
        assert!((value(&s, "energy_wh_total", &[("channel", "0")]) - 72.5833).abs() < 0.001);
        assert!((value(&s, "energy_wh_total", &[("channel", "1")]) - 72.9).abs() < 0.001);
        assert_eq!(value(&s, "power_watts", &[("channel", "0")]), 0.0);
        assert_eq!(
            value(&s, "cover_position_percent", &[("channel", "0")]),
            0.0
        );
        let state = |st| value(&s, "cover_state", &[("channel", "0"), ("state", st)]);
        assert_eq!(state("stop"), 1.0);
        assert_eq!(state("open"), 0.0);
        assert_eq!(value(&s, "temperature_celsius", &[]), 44.58);
        assert_eq!(value(&s, "overtemperature", &[]), 0.0);
        assert_eq!(value(&s, "voltage_volts", &[]), 231.34);
        assert_eq!(value(&s, "wifi_rssi_dbm", &[]), -66.0);
        assert_eq!(value(&s, "uptime_seconds", &[]), 23204272.0);
    }

    #[test]
    fn dimmer_2() {
        let s = gen1(&fixture("dimmer-2")["status"], &base());
        assert_eq!(value(&s, "output_on", &[("channel", "0")]), 0.0);
        assert_eq!(
            value(&s, "light_brightness_percent", &[("channel", "0")]),
            6.0
        );
        assert!((value(&s, "energy_wh_total", &[("channel", "0")]) - 887.1333).abs() < 0.001);
        assert_eq!(value(&s, "temperature_celsius", &[]), 40.61);
    }

    #[test]
    fn uncalibrated_roller_has_no_position() {
        let mut status = fixture("shelly-25-roller")["status"].clone();
        status["rollers"][0]["positioning"] = json!(false);
        let s = gen1(&status, &base());
        assert!(!s.iter().any(|r| r.metric.name == "cover_position_percent"));
        // Control: the roller itself is still read.
        assert!(s.iter().any(|r| r.metric.name == "cover_state"));
    }

    #[test]
    fn empty_response_gives_nothing_instead_of_a_crash() {
        assert!(gen1(&json!({}), &base()).is_empty());
        assert!(gen2(&json!({}), &base()).is_empty());
        assert!(gen2(&json!("broken"), &base()).is_empty());
    }

    #[test]
    fn plug_s_gen3() {
        let s = gen2(&fixture("plug-s-gen3")["status"], &base());
        // Gen3 already reports Wh: no conversion.
        assert!((value(&s, "energy_wh_total", &[("channel", "0")]) - 250581.917).abs() < 0.001);
        assert_eq!(value(&s, "output_on", &[("channel", "0")]), 0.0);
        assert_eq!(value(&s, "voltage_volts", &[("channel", "0")]), 229.8);
        assert_eq!(value(&s, "current_amperes", &[("channel", "0")]), 0.0);
        assert_eq!(value(&s, "temperature_celsius", &[("channel", "0")]), 34.0);
        assert_eq!(value(&s, "overtemperature", &[("channel", "0")]), 0.0);
        assert_eq!(value(&s, "uptime_seconds", &[]), 106212.0);
    }

    #[test]
    fn two_pm_gen3_as_cover() {
        let s = gen2(&fixture("2pm-gen3-cover")["status"], &base());
        let state = |st| value(&s, "cover_state", &[("channel", "0"), ("state", st)]);
        assert_eq!(state("closed"), 1.0);
        assert_eq!(state("opening"), 0.0);
        assert_eq!(
            value(&s, "cover_position_percent", &[("channel", "0")]),
            0.0
        );
        assert!((value(&s, "energy_wh_total", &[("channel", "0")]) - 235.76).abs() < 0.001);
        assert_eq!(value(&s, "temperature_celsius", &[("channel", "0")]), 38.8);
    }

    #[test]
    fn overtemperature_comes_from_the_error_list() {
        let mut status = fixture("plug-s-gen3")["status"].clone();
        status["switch:0"]["errors"] = json!(["overtemp"]);
        let s = gen2(&status, &base());
        assert_eq!(value(&s, "overtemperature", &[("channel", "0")]), 1.0);
    }

    #[test]
    fn unknown_components_are_skipped() {
        let status = json!({"sys": {"uptime": 5}, "em:0": {"a": 1}, "switch:1": "broken"});
        let s = gen2(&status, &base());
        let names: Vec<_> = s.iter().map(|r| r.metric.name).collect();
        assert_eq!(names, ["uptime_seconds"]);
    }
}
