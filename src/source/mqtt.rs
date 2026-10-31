//! Shelly Gen2+ messages via MQTT → device state.
//!
//! A Gen2 Shelly with MQTT enabled reports in two ways (topic prefix = device ID, e.g.
//! `shellyplussmoke-02aabbcc0001`):
//! - `<id>/status/<component>` (setting "status_ntf"): the full status of one component, e.g.
//!   `smoke:0`, `devicepower:0`, `sys`, `wifi`.
//! - `<id>/events/rpc` (setting "rpc_ntf"): JSON-RPC notifications `NotifyStatus` (changed
//!   fields), `NotifyFullStatus` (everything, on wake-up) and `NotifyEvent` (e.g. `alarm`,
//!   `alarm_off`, `alarm_test`, `sleep`).
//! - `<id>/online`: `true`/`false`, retained (Last Will).
//!
//! A Gen1 battery device (Flood) reports under `shellies/<id>/`: one plain value per topic in
//! `sensor/`, for example `sensor/flood` = `false`, plus `online` and an `announce` JSON.
//!
//! Smoke detectors are awake for a few seconds only. The exporter therefore keeps the last state
//! per device and also stores it retained in the broker (`STATE_PREFIX`), so it is back right after
//! a restart.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::model::{
    BATTERY_PERCENT, BATTERY_VOLTS, FLOOD_ALARM, FLOOD_LAST_ALARM, LAST_SEEN, Labels, MQTT_ONLINE,
    Metric, Reading, SENSOR_ERROR, SMOKE_ALARM, SMOKE_LAST_ALARM, SMOKE_LAST_TEST, SMOKE_MUTED,
    TEMPERATURE_SENSOR, UPDATE_AVAILABLE, UPTIME, WIFI_RSSI, now,
};

/// The exporter's own retained state messages: `<STATE_PREFIX>/<device-id>`.
pub const STATE_PREFIX: &str = "shelly-exporter/state";

/// Last known state of a Shelly.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Device {
    /// Device ID = topic prefix, e.g. `shellyplussmoke-02aabbcc0001`.
    pub id: String,
    /// Receive time of the device's last message (Unix seconds). Deliberately the exporter's clock,
    /// not the device's: a detector that has just woken up does not always know the time yet.
    pub last_seen: f64,
    pub online: Option<bool>,
    /// Smoke detector per channel (`smoke:0` → "0").
    pub smoke_alarm: BTreeMap<String, bool>,
    pub smoke_muted: BTreeMap<String, bool>,
    pub battery_percent: Option<f64>,
    pub battery_volts: Option<f64>,
    pub wifi_rssi: Option<f64>,
    pub uptime: Option<f64>,
    pub firmware: Option<String>,
    pub update_available: Option<bool>,
    /// Time of the last alarm event, so a short alarm between two Prometheus scrapes is not lost.
    pub last_alarm_event: Option<f64>,
    pub last_test_event: Option<f64>,
    /// Gen1 sensors. New fields stay `Option`: a state stored by an older version must still load.
    pub flood: Option<bool>,
    pub last_flood_event: Option<f64>,
    pub temperature: Option<f64>,
    pub sensor_error: Option<f64>,
}

/// Store changed states in batches instead of on every message: a smoke detector sends ~20
/// messages within 4 s on wake-up, which would be ~20 retained writes. Anything not yet stored at
/// a crash comes back with the device's next wake-up.
const FLUSH_EVERY: Duration = Duration::from_secs(15);

pub struct Config {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub user: Option<String>,
    pub password: String,
}

pub struct State {
    devices: Mutex<BTreeMap<String, Device>>,
    /// Devices whose state changed since it was last stored (see `FLUSH_EVERY`).
    pending: Mutex<BTreeSet<String>>,
    names: BTreeMap<String, String>,
    connected: AtomicBool,
}

impl State {
    pub fn new(names: BTreeMap<String, String>) -> Self {
        Self {
            devices: Mutex::new(BTreeMap::new()),
            pending: Mutex::new(BTreeSet::new()),
            names,
            connected: AtomicBool::new(false),
        }
    }

    /// Broker connection, number of devices, and their readings.
    pub async fn readings(&self) -> (bool, usize, Vec<Reading>) {
        let devices = self.devices.lock().await;
        (
            self.connected.load(Ordering::Relaxed),
            devices.len(),
            readings(&devices, &self.names),
        )
    }
}

/// Start the MQTT client and the batched state store.
pub fn spawn(state: Arc<State>, config: Config) {
    let mut opts = MqttOptions::new(config.client_id, config.host.clone(), config.port);
    opts.set_keep_alive(Duration::from_secs(30));
    // Receive retained messages (Last Will, our own state) on connect.
    opts.set_clean_session(true);
    if let Some(user) = config.user {
        opts.set_credentials(user, config.password);
    }
    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    // Store the state retained in the broker: back right after a restart, even for detectors that
    // are asleep.
    let flush_state = state.clone();
    let flush_client = client.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(FLUSH_EVERY);
        loop {
            tick.tick().await;
            let ids = std::mem::take(&mut *flush_state.pending.lock().await);
            let devices = flush_state.devices.lock().await;
            for id in ids {
                if let Some(dev) = devices.get(&id)
                    && worth_keeping(dev)
                    && let Ok(json) = serde_json::to_vec(dev)
                {
                    let topic = format!("{STATE_PREFIX}/{id}");
                    if let Err(e) = flush_client.try_publish(topic, QoS::AtLeastOnce, true, json) {
                        warn!(id, error = %e, "state not stored");
                    }
                }
            }
        }
    });

    let host = config.host;
    let port = config.port;
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!(host = %host, port, "connected to broker");
                    state.connected.store(true, Ordering::Relaxed);
                    // Subscribe again after every (re)connect: a clean session forgets them.
                    // Our own state first: the broker delivers retained messages per subscription
                    // in this order, so the stored state arrives before `online false`.
                    for topic in [
                        &format!("{STATE_PREFIX}/#"),
                        "+/status/#",
                        "+/events/rpc",
                        "+/online",
                        "shellies/+/sensor/#",
                        "shellies/+/online",
                        "shellies/+/announce",
                    ] {
                        if let Err(e) = client.subscribe(topic, QoS::AtLeastOnce).await {
                            warn!(topic, error = %e, "subscribe failed");
                        }
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let mut devices = state.devices.lock().await;
                    match apply(&mut devices, &p.topic, &p.payload, p.retain, now()) {
                        Effect::Changed(id) => {
                            state.pending.lock().await.insert(id);
                        }
                        Effect::Restored(dev) => {
                            let id = dev.id.clone();
                            if restore(&mut devices, *dev) {
                                info!(id, "state restored from broker");
                            }
                        }
                        Effect::Ignored => {}
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    if state.connected.swap(false, Ordering::Relaxed) {
                        warn!(error = %e, "connection to broker lost");
                    } else {
                        warn!(error = %e, "broker unreachable");
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });
}

/// Device state → readings. Labels: `device` (name from `SHELLY_NAMES`, otherwise the ID) and
/// `id`, because battery devices do not report their name via MQTT. Unknown values are left out,
/// not reported as 0.
pub fn readings(
    devices: &BTreeMap<String, Device>,
    names: &BTreeMap<String, String>,
) -> Vec<Reading> {
    let mut out = Vec::new();
    for (id, d) in devices {
        let name = names.get(id).unwrap_or(id).clone();
        let base: Labels = vec![("device", name), ("id", id.clone())];
        let mut add = |metric: &'static Metric, v: Option<f64>| {
            if let Some(v) = v {
                out.push(Reading::new(metric, base.clone(), v));
            }
        };
        let flag = |v: Option<bool>| v.map(|b| f64::from(u8::from(b)));
        add(&LAST_SEEN, (d.last_seen > 0.0).then_some(d.last_seen));
        add(&SMOKE_LAST_ALARM, d.last_alarm_event);
        add(&SMOKE_LAST_TEST, d.last_test_event);
        add(&FLOOD_ALARM, flag(d.flood));
        add(&FLOOD_LAST_ALARM, d.last_flood_event);
        add(&TEMPERATURE_SENSOR, d.temperature);
        add(&SENSOR_ERROR, d.sensor_error);
        add(&BATTERY_PERCENT, d.battery_percent);
        add(&BATTERY_VOLTS, d.battery_volts);
        add(&WIFI_RSSI, d.wifi_rssi);
        add(&UPTIME, d.uptime);
        add(&MQTT_ONLINE, flag(d.online));
        add(&UPDATE_AVAILABLE, flag(d.update_available));
        for (metric, channels) in [
            (&SMOKE_ALARM, &d.smoke_alarm),
            (&SMOKE_MUTED, &d.smoke_muted),
        ] {
            for (channel, on) in channels {
                let mut labels = base.clone();
                labels.push(("channel", channel.clone()));
                out.push(Reading::new(metric, labels, f64::from(u8::from(*on))));
            }
        }
    }
    out
}

/// Effect of a message on the state.
#[derive(Debug, PartialEq)]
pub enum Effect {
    /// Device `id` changed.
    Changed(String),
    /// The exporter's retained state (after a restart).
    Restored(Box<Device>),
    /// Not for us, or not understood.
    Ignored,
}

/// Device ID from the topic, if it is a Shelly Gen2 message.
fn device_id(topic: &str) -> Option<(&str, &str)> {
    let (id, rest) = topic.split_once('/')?;
    id.starts_with("shelly").then_some((id, rest))
}

/// Apply one message to the state. `now` = receive time (Unix seconds), `retained` = retain flag of
/// the delivery.
pub fn apply(
    devices: &mut BTreeMap<String, Device>,
    topic: &str,
    payload: &[u8],
    retained: bool,
    now: f64,
) -> Effect {
    if let Some(id) = topic
        .strip_prefix(STATE_PREFIX)
        .and_then(|t| t.strip_prefix('/'))
    {
        // Only the retained state delivered on subscribe counts. During operation, the echo of our
        // own publish arrives here (without retain flag); taking it over would mean overwriting
        // ourselves.
        if !retained {
            return Effect::Ignored;
        }
        return match serde_json::from_slice::<Device>(payload) {
            Ok(d) if d.id == id => Effect::Restored(Box::new(d)),
            _ => Effect::Ignored,
        };
    }
    if let Some(rest) = topic.strip_prefix("shellies/") {
        return apply_gen1(devices, rest, payload, now);
    }
    let Some((id, rest)) = device_id(topic) else {
        return Effect::Ignored;
    };
    let dev = devices.entry(id.to_string()).or_insert_with(|| Device {
        id: id.to_string(),
        ..Default::default()
    });

    if rest == "online" {
        let online = payload.trim_ascii() == b"true";
        if retained {
            // Retained = old news, not a sign of life. A detector disconnects cleanly, so its
            // `online true` stays in the broker forever: counted as a sign of life, every exporter
            // restart would have "seen" every silent detector. Only take it if nothing newer
            // (the stored state) is known.
            if dev.online.is_none() {
                dev.online = Some(online);
            }
            return Effect::Changed(id.to_string());
        }
        dev.online = Some(online);
        // `online false` comes from the broker (Last Will), not the device: no sign of life.
        if online {
            dev.last_seen = now;
        }
        return Effect::Changed(id.to_string());
    }

    let Ok(json) = serde_json::from_slice::<Value>(payload) else {
        return Effect::Ignored;
    };
    if let Some(component) = rest.strip_prefix("status/") {
        merge_component(dev, component, &json);
    } else if rest == "events/rpc" {
        let params = json.get("params").cloned().unwrap_or(Value::Null);
        match json.get("method").and_then(Value::as_str) {
            Some("NotifyStatus") | Some("NotifyFullStatus") => {
                if let Value::Object(map) = &params {
                    for (component, status) in map {
                        merge_component(dev, component, status);
                    }
                }
            }
            Some("NotifyEvent") => {
                for event in params
                    .get("events")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    apply_event(dev, event, now);
                }
            }
            _ => return Effect::Ignored,
        }
    } else {
        return Effect::Ignored;
    }
    dev.last_seen = now;
    Effect::Changed(id.to_string())
}

/// A Gen1 message, `rest` = `<id>/sensor/flood` and so on.
fn apply_gen1(
    devices: &mut BTreeMap<String, Device>,
    rest: &str,
    payload: &[u8],
    now: f64,
) -> Effect {
    let Some((id, field)) = rest.split_once('/') else {
        return Effect::Ignored;
    };
    let text = String::from_utf8_lossy(payload);
    let text = text.trim();
    let number = text.parse::<f64>().ok();
    let flag = text == "true";
    let known = matches!(
        field,
        "online"
            | "announce"
            | "sensor/flood"
            | "sensor/battery"
            | "sensor/temperature"
            | "sensor/error"
            | "sensor/act_reasons"
    );
    if !known || !id.starts_with("shelly") {
        return Effect::Ignored;
    }
    let dev = devices.entry(id.to_string()).or_insert_with(|| Device {
        id: id.to_string(),
        ..Default::default()
    });
    match field {
        "online" => dev.online = Some(flag),
        "announce" => {
            if let Ok(json) = serde_json::from_str::<Value>(text) {
                dev.update_available = json.get("new_fw").and_then(Value::as_bool);
            }
        }
        "sensor/flood" => {
            // A short alarm must not get lost between two scrapes.
            if flag && dev.flood != Some(true) {
                dev.last_flood_event = Some(now);
            }
            dev.flood = Some(flag);
        }
        "sensor/battery" => dev.battery_percent = number.or(dev.battery_percent),
        "sensor/temperature" => dev.temperature = number.or(dev.temperature),
        "sensor/error" => dev.sensor_error = number.or(dev.sensor_error),
        _ => {}
    }
    // `online false` comes without a sign of life, like the Last Will of Gen2.
    if field != "online" || flag {
        dev.last_seen = now;
    }
    Effect::Changed(id.to_string())
}

/// Merge the (partial) status of one component (NotifyStatus sends changed fields only).
fn merge_component(dev: &mut Device, component: &str, s: &Value) {
    let (kind, channel) = component.split_once(':').unwrap_or((component, ""));
    match kind {
        "smoke" => {
            if let Some(a) = s.get("alarm").and_then(Value::as_bool) {
                dev.smoke_alarm.insert(channel.to_string(), a);
            }
            if let Some(m) = s.get("mute").and_then(Value::as_bool) {
                dev.smoke_muted.insert(channel.to_string(), m);
            }
        }
        "devicepower" => {
            if let Some(b) = s.get("battery") {
                if let Some(p) = b.get("percent").and_then(Value::as_f64) {
                    dev.battery_percent = Some(p);
                }
                if let Some(v) = b.get("V").and_then(Value::as_f64) {
                    dev.battery_volts = Some(v);
                }
            }
        }
        "wifi" => {
            if let Some(r) = s.get("rssi").and_then(Value::as_f64) {
                dev.wifi_rssi = Some(r);
            }
        }
        "sys" => {
            if let Some(u) = s.get("uptime").and_then(Value::as_f64) {
                dev.uptime = Some(u);
            }
            if let Some(updates) = s.get("available_updates") {
                dev.update_available = Some(updates.get("stable").is_some());
            }
        }
        _ => {}
    }
}

/// `NotifyEvent`: `alarm`/`alarm_off`/`alarm_test`/`mute` of a smoke detector, `sleep` of a
/// battery device.
fn apply_event(dev: &mut Device, event: &Value, now: f64) {
    let component = event.get("component").and_then(Value::as_str).unwrap_or("");
    // Battery devices disconnect cleanly, so the Last Will (`online false`) never arrives; going to
    // sleep is the only sign of it.
    if component == "sys" && event.get("event").and_then(Value::as_str) == Some("sleep") {
        dev.online = Some(false);
        return;
    }
    let Some(channel) = component.strip_prefix("smoke:") else {
        return;
    };
    match event.get("event").and_then(Value::as_str) {
        Some("alarm") => {
            dev.smoke_alarm.insert(channel.to_string(), true);
            dev.last_alarm_event = Some(now);
        }
        Some("alarm_off") => {
            dev.smoke_alarm.insert(channel.to_string(), false);
        }
        Some("alarm_test") => {
            dev.last_test_event = Some(now);
        }
        Some("mute") => {
            dev.smoke_muted.insert(channel.to_string(), true);
        }
        _ => {}
    }
}

/// Only a state with a real sign of life is stored in the broker. Otherwise, after a restart, the
/// retained `online false` (delivered before anything else) overwrote the stored state with an
/// empty one, and after the next restart everything was gone.
pub fn worth_keeping(dev: &Device) -> bool {
    dev.last_seen > 0.0
}

/// Take over a retained state, but never overwrite a newer one of our own.
pub fn restore(devices: &mut BTreeMap<String, Device>, restored: Device) -> bool {
    match devices.get(&restored.id) {
        Some(d) if d.last_seen >= restored.last_seen => false,
        _ => {
            devices.insert(restored.id.clone(), restored);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "shellyplussmoke-02aabbcc0001";

    fn run(msgs: &[(&str, &str)]) -> BTreeMap<String, Device> {
        let mut d = BTreeMap::new();
        for (i, (topic, payload)) in msgs.iter().enumerate() {
            apply(&mut d, topic, payload.as_bytes(), false, 1000.0 + i as f64);
        }
        d
    }

    #[test]
    fn status_topics_fill_battery_wifi_and_smoke() {
        let d = run(&[
            (
                &format!("{ID}/status/devicepower:0"),
                r#"{"id":0,"battery":{"V":2.98,"percent":91},"external":{"present":false}}"#,
            ),
            (
                &format!("{ID}/status/smoke:0"),
                r#"{"id":0,"alarm":false,"mute":false}"#,
            ),
            (
                &format!("{ID}/status/wifi"),
                r#"{"sta_ip":"192.0.2.1","status":"got ip","rssi":-61}"#,
            ),
        ]);
        let dev = &d[ID];
        assert_eq!(dev.battery_percent, Some(91.0));
        assert_eq!(dev.battery_volts, Some(2.98));
        assert_eq!(dev.wifi_rssi, Some(-61.0));
        assert_eq!(dev.smoke_alarm.get("0"), Some(&false));
        assert_eq!(dev.last_seen, 1002.0, "time of the last message");
    }

    #[test]
    fn notify_status_changes_only_given_fields() {
        let d = run(&[
            (
                &format!("{ID}/status/smoke:0"),
                r#"{"id":0,"alarm":false,"mute":false}"#,
            ),
            (
                &format!("{ID}/events/rpc"),
                r#"{"src":"x","dst":"x/events","method":"NotifyStatus","params":{"ts":1,"smoke:0":{"id":0,"alarm":true}}}"#,
            ),
        ]);
        assert_eq!(d[ID].smoke_alarm.get("0"), Some(&true));
        assert_eq!(
            d[ID].smoke_muted.get("0"),
            Some(&false),
            "mute kept, because it was not sent"
        );
    }

    #[test]
    fn alarm_event_is_remembered_even_if_it_ends_before_the_next_scrape() {
        let d = run(&[
            (
                &format!("{ID}/events/rpc"),
                r#"{"method":"NotifyEvent","params":{"ts":1,"events":[{"component":"smoke:0","event":"alarm","ts":1}]}}"#,
            ),
            (
                &format!("{ID}/events/rpc"),
                r#"{"method":"NotifyEvent","params":{"ts":2,"events":[{"component":"smoke:0","event":"alarm_off","ts":2}]}}"#,
            ),
        ]);
        assert_eq!(d[ID].smoke_alarm.get("0"), Some(&false));
        assert_eq!(d[ID].last_alarm_event, Some(1000.0));
    }

    #[test]
    fn test_button_is_an_event_not_an_alarm() {
        let d = run(&[(
            &format!("{ID}/events/rpc"),
            r#"{"method":"NotifyEvent","params":{"events":[{"component":"smoke:0","event":"alarm_test"}]}}"#,
        )]);
        assert_eq!(d[ID].last_test_event, Some(1000.0));
        assert_eq!(d[ID].last_alarm_event, None);
    }

    #[test]
    fn last_will_false_is_no_sign_of_life() {
        let d = run(&[
            (&format!("{ID}/status/smoke:0"), r#"{"id":0,"alarm":false}"#),
            (&format!("{ID}/online"), "false"),
        ]);
        assert_eq!(d[ID].online, Some(false));
        assert_eq!(
            d[ID].last_seen, 1000.0,
            "'offline' from the broker does not count as seen"
        );
    }

    #[test]
    fn full_status_on_wake_up() {
        let d = run(&[(
            &format!("{ID}/events/rpc"),
            r#"{"method":"NotifyFullStatus","params":{
            "ts":1,"smoke:0":{"id":0,"alarm":false,"mute":false},
            "devicepower:0":{"id":0,"battery":{"V":2.91,"percent":84}},
            "sys":{"uptime":12,"available_updates":{"stable":{"version":"1.5.0"}}},
            "wifi":{"rssi":-70}}}"#,
        )]);
        let dev = &d[ID];
        assert_eq!(dev.battery_percent, Some(84.0));
        assert_eq!(dev.update_available, Some(true));
        assert_eq!(dev.uptime, Some(12.0));
        assert_eq!(dev.wifi_rssi, Some(-70.0));
    }

    #[test]
    fn foreign_and_broken_messages_are_ignored() {
        let mut d = BTreeMap::new();
        assert_eq!(
            apply(&mut d, "homeassistant/status", b"online", false, 1.0),
            Effect::Ignored
        );
        assert_eq!(
            apply(
                &mut d,
                &format!("{ID}/status/smoke:0"),
                b"not json",
                false,
                1.0
            ),
            Effect::Ignored
        );
        assert_eq!(
            apply(
                &mut d,
                &format!("{ID}/events/rpc"),
                br#"{"method":"Unknown"}"#,
                false,
                1.0
            ),
            Effect::Ignored
        );
        assert!(
            d.get(ID).is_none_or(|dev| dev.last_seen == 0.0),
            "none of these is a sign of life"
        );
    }

    #[test]
    fn restored_state_never_overwrites_newer_own_state() {
        let mut d = run(&[(&format!("{ID}/status/smoke:0"), r#"{"id":0,"alarm":true}"#)]);
        let old = Device {
            id: ID.into(),
            last_seen: 500.0,
            ..Default::default()
        };
        assert!(!restore(&mut d, old));
        assert_eq!(d[ID].smoke_alarm.get("0"), Some(&true));
        let newer = Device {
            id: ID.into(),
            last_seen: 2000.0,
            battery_percent: Some(50.0),
            ..Default::default()
        };
        assert!(restore(&mut d, newer));
        assert_eq!(d[ID].battery_percent, Some(50.0));
    }

    #[test]
    fn only_a_sign_of_life_is_worth_keeping() {
        // Order after a restart: Last Will before anything else.
        let mut d = BTreeMap::new();
        apply(&mut d, &format!("{ID}/online"), b"false", true, 5.0);
        assert!(
            !worth_keeping(&d[ID]),
            "empty entry from `online false` must not overwrite anything"
        );
        apply(
            &mut d,
            &format!("{ID}/status/smoke:0"),
            br#"{"id":0,"alarm":false}"#,
            false,
            6.0,
        );
        assert!(worth_keeping(&d[ID]));
    }

    #[test]
    fn state_topic_roundtrip() {
        let dev = Device {
            id: ID.into(),
            last_seen: 1.5,
            battery_percent: Some(77.0),
            ..Default::default()
        };
        let mut d = BTreeMap::new();
        let topic = format!("{STATE_PREFIX}/{ID}");
        let eff = apply(
            &mut d,
            &topic,
            &serde_json::to_vec(&dev).unwrap(),
            true,
            9.0,
        );
        assert_eq!(eff, Effect::Restored(Box::new(dev)));
        // Different ID in the payload → ignore (topic and payload must match).
        let other = Device {
            id: "shellyplussmoke-other".into(),
            ..Default::default()
        };
        assert_eq!(
            apply(
                &mut d,
                &topic,
                &serde_json::to_vec(&other).unwrap(),
                true,
                9.0
            ),
            Effect::Ignored
        );
    }

    // Real messages from a device, anonymized, otherwise verbatim, including the firmware's
    // whitespace and number formats.
    const REAL_TEST: &str = r#"{"src":"shellyplussmoke-02aabbcc0002","dst":"shellyplussmoke-02aabbcc0002/events","method":"NotifyEvent","params":{"ts":1793357873.58,"events":[{"component":"smoke:0", "id":0, "event":"alarm_test", "state":true, "ts":1793357873.58}]}}"#;
    const REAL_SLEEP: &str = r#"{"src":"shellyplussmoke-02aabbcc0002","dst":"shellyplussmoke-02aabbcc0002/events","method":"NotifyEvent","params":{"ts":1793357874.53,"events":[{"component":"sys", "event":"sleep", "ts":1793357874.53}]}}"#;
    const REAL_BATTERY_ERROR: &str =
        r#"{"id": 0,"battery":{"V":"Q", "percent":null},"errors": ["read"]}"#;
    const REAL_BATTERY: &str = r#"{"id": 0,"battery":{"V":2.97, "percent":92}}"#;
    const REAL_FULL: &str = r#"{"src":"shellyplussmoke-02aabbcc0002","dst":"shellyplussmoke-02aabbcc0002/events","method":"NotifyFullStatus","params":{"ts":2.36,"ble":{},"cloud":{"connected":false},"devicepower:0":{"id": 0,"battery":{"V":3.00, "percent":100}},"mqtt":{"connected":true},"smoke:0":{"id":0,"alarm":false, "mute":false},"sys":{"mac":"02AABBCC0002","restart_required":false,"time":null,"unixtime":null,"uptime":2,"ram_size":235824,"ram_free":156236,"fs_size":458752,"fs_free":114688,"cfg_rev":15,"kvs_rev":0,"webhook_rev":0,"available_updates":{},"wakeup_reason":{"boot":"poweron","cause":"undefined"},"wakeup_period":86400},"wifi":{"sta_ip":"192.0.2.1","status":"got ip","ssid":"REDACTED","rssi":-47},"ws":{"connected":true}}}"#;

    #[test]
    fn readings_carry_names_and_ids_and_leave_unknown_values_out() {
        let names = BTreeMap::from([(ID.to_string(), "hall".to_string())]);
        let devices = run(&[(&format!("{ID}/status/devicepower:0"), REAL_BATTERY)]);
        let r = readings(&devices, &names);
        let battery = r
            .iter()
            .find(|x| x.metric.name == "battery_percent")
            .unwrap();
        assert_eq!(battery.value, 92.0);
        assert_eq!(battery.labels[0], ("device", "hall".to_string()));
        assert_eq!(battery.labels[1], ("id", ID.to_string()));
        assert!(
            !r.iter().any(|x| x.metric.name == "wifi_rssi_dbm"),
            "a missing value must not show up as 0: {r:?}"
        );
    }

    #[test]
    fn gen1_flood_wake_up() {
        const ID: &str = "shellyflood-02AABB";
        let t = |f: &str| format!("shellies/{ID}/{f}");
        let announce = r#"{"id":"shellyflood-02AABB","model":"SHWT-1","mac":"020000AABB01","ip":"192.0.2.1","new_fw":false,"fw_ver":"20201124-091757/v1.9.0@57ac4ad8"}"#;
        let mut d = BTreeMap::new();
        // Order and values as recorded from a real Shelly Flood.
        for (field, payload) in [
            ("online", "true"),
            ("announce", announce),
            ("sensor/temperature", "22.75"),
            ("sensor/flood", "false"),
            ("sensor/battery", "89"),
            ("sensor/error", "0"),
            ("sensor/act_reasons", r#"["sensor"]"#),
        ] {
            let effect = apply(&mut d, &t(field), payload.as_bytes(), false, 1000.0);
            assert_eq!(effect, Effect::Changed(ID.to_string()), "{field}");
        }
        let dev = &d[ID];
        assert_eq!(dev.temperature, Some(22.75));
        assert_eq!(dev.flood, Some(false));
        assert_eq!(dev.battery_percent, Some(89.0));
        assert_eq!(dev.sensor_error, Some(0.0));
        assert_eq!(dev.update_available, Some(false));
        assert_eq!(dev.last_seen, 1000.0);
        assert_eq!(dev.last_flood_event, None, "dry: no alarm");

        apply(&mut d, &t("sensor/flood"), b"true", false, 2000.0);
        apply(&mut d, &t("sensor/flood"), b"true", false, 2005.0);
        assert_eq!(d[ID].flood, Some(true));
        assert_eq!(d[ID].last_flood_event, Some(2000.0), "start of the alarm");
        apply(&mut d, &t("sensor/flood"), b"false", false, 2100.0);
        assert_eq!(d[ID].flood, Some(false));
        assert_eq!(d[ID].last_flood_event, Some(2000.0), "survives the end");

        let r = readings(&d, &BTreeMap::new());
        let value = |n: &str| r.iter().find(|x| x.metric.name == n).map(|x| x.value);
        assert_eq!(value("flood_alarm"), Some(0.0));
        assert_eq!(value("flood_last_alarm_timestamp_seconds"), Some(2000.0));
        assert_eq!(value("sensor_temperature_celsius"), Some(22.75));
    }

    #[test]
    fn gen1_online_false_is_no_sign_of_life() {
        let mut d = BTreeMap::new();
        let topic = "shellies/shellyflood-02AABB/online";
        apply(&mut d, topic, b"true", false, 1000.0);
        apply(&mut d, topic, b"false", false, 2000.0);
        let dev = &d["shellyflood-02AABB"];
        assert_eq!(dev.online, Some(false));
        assert_eq!(dev.last_seen, 1000.0);
    }

    #[test]
    fn gen1_foreign_topics_are_ignored() {
        let mut d = BTreeMap::new();
        for topic in [
            "shellies/announce",
            "shellies/shellyflood-x/command",
            "shellies/other/sensor/flood",
        ] {
            assert_eq!(
                apply(&mut d, topic, b"true", false, 1.0),
                Effect::Ignored,
                "{topic}"
            );
        }
        assert!(d.is_empty(), "{d:?}");
    }

    #[test]
    fn a_state_stored_by_the_previous_version_still_loads() {
        // Field list of 0.2.x: no Gen1 fields yet.
        let old = r#"{"id":"shellyplussmoke-02aabbcc0001","last_seen":5.0,"online":false,"smoke_alarm":{},"smoke_muted":{},"battery_percent":91.0,"battery_volts":null,"wifi_rssi":null,"uptime":null,"firmware":null,"update_available":null,"last_alarm_event":null,"last_test_event":null}"#;
        let mut d = BTreeMap::new();
        let topic = format!("{STATE_PREFIX}/shellyplussmoke-02aabbcc0001");
        let effect = apply(&mut d, &topic, old.as_bytes(), true, 1.0);
        assert!(matches!(effect, Effect::Restored(_)), "{effect:?}");
    }

    #[test]
    fn real_wake_up_test_and_sleep() {
        let d = run(&[
            (&format!("{ID}/online"), "true"),
            (&format!("{ID}/events/rpc"), REAL_FULL),
            (&format!("{ID}/events/rpc"), REAL_TEST),
            (&format!("{ID}/events/rpc"), REAL_SLEEP),
        ]);
        let dev = &d[ID];
        assert_eq!(dev.battery_percent, Some(100.0));
        assert_eq!(dev.battery_volts, Some(3.0));
        assert_eq!(dev.wifi_rssi, Some(-47.0));
        assert_eq!(
            dev.smoke_alarm.get("0"),
            Some(&false),
            "a test is not an alarm"
        );
        assert_eq!(dev.last_test_event, Some(1002.0));
        assert_eq!(dev.last_alarm_event, None);
        assert_eq!(dev.online, Some(false), "asleep");
        assert_eq!(dev.last_seen, 1003.0, "going to sleep is a sign of life");
    }

    #[test]
    fn battery_read_error_keeps_last_good_value() {
        let topic = format!("{ID}/status/devicepower:0");
        let d = run(&[(&topic, REAL_BATTERY), (&topic, REAL_BATTERY_ERROR)]);
        assert_eq!(d[ID].battery_percent, Some(92.0));
        assert_eq!(d[ID].battery_volts, Some(2.97));
        // Without a previous value the battery stays unknown, never 0 (that would mean "empty").
        let d = run(&[(&topic, REAL_BATTERY_ERROR)]);
        assert_eq!(d[ID].battery_percent, None);
        assert_eq!(d[ID].battery_volts, None);
    }

    #[test]
    fn retained_online_after_restart_is_no_sign_of_life() {
        // Order on restart: stored state, then the forever-retained `online true`.
        let mut d = BTreeMap::new();
        let saved = Device {
            id: ID.into(),
            last_seen: 500.0,
            online: Some(false),
            ..Default::default()
        };
        let state = serde_json::to_vec(&saved).unwrap();
        let eff = apply(
            &mut d,
            &format!("{STATE_PREFIX}/{ID}"),
            &state,
            true,
            9000.0,
        );
        let Effect::Restored(dev) = eff else {
            panic!("state not restored: {eff:?}")
        };
        assert!(restore(&mut d, *dev));
        apply(&mut d, &format!("{ID}/online"), b"true", true, 9000.0);
        assert_eq!(d[ID].last_seen, 500.0, "a silent detector stays silent");
        assert_eq!(d[ID].online, Some(false), "the stored state is newer");
        // Counter-check: delivered live, it is a sign of life.
        apply(&mut d, &format!("{ID}/online"), b"true", false, 9001.0);
        assert_eq!(d[ID].last_seen, 9001.0);
        assert_eq!(d[ID].online, Some(true));
    }

    #[test]
    fn own_echo_without_retain_is_ignored() {
        let dev = Device {
            id: ID.into(),
            last_seen: 1.5,
            ..Default::default()
        };
        let mut d = BTreeMap::new();
        let topic = format!("{STATE_PREFIX}/{ID}");
        let json = serde_json::to_vec(&dev).unwrap();
        assert_eq!(apply(&mut d, &topic, &json, false, 9.0), Effect::Ignored);
        assert_eq!(
            apply(&mut d, &topic, &json, true, 9.0),
            Effect::Restored(Box::new(dev)),
            "counter-check: the same payload delivered retained is taken over"
        );
    }
}
