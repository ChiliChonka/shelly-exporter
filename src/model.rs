//! The neutral data model between sources and output formats.
//!
//! Sources (`source::mqtt`, `source::http`) turn device messages into [`Reading`]s; a format
//! (`output`) turns readings into text. Neither side knows the other: a new format needs nothing
//! from the sources, and a new device needs nothing from the formats. Metric names here carry no
//! prefix or format conventions of their own; `output::prometheus` adds `shelly_`.

use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    Gauge,
    Counter,
}

#[derive(Debug, PartialEq)]
pub struct Metric {
    pub name: &'static str,
    pub kind: Kind,
    pub help: &'static str,
}

pub type Labels = Vec<(&'static str, String)>;

/// One value of one metric.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub metric: &'static Metric,
    pub labels: Labels,
    pub value: f64,
}

impl Reading {
    pub fn new(metric: &'static Metric, labels: Labels, value: f64) -> Self {
        Self {
            metric,
            labels,
            value,
        }
    }
}

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

macro_rules! metrics {
    ($($id:ident $name:literal $kind:ident $help:literal;)*) => {
        $(pub static $id: Metric = Metric {
            name: $name,
            kind: Kind::$kind,
            help: $help,
        };)*
    };
}

metrics! {
    // Exporter
    EXPORTER_MQTT_CONNECTED "exporter_mqtt_connected" Gauge "1 if the exporter is connected to the broker";
    EXPORTER_DEVICES "exporter_devices" Gauge "Devices known via MQTT";
    DEVICES_DISCOVERED "devices_discovered" Gauge "Devices known via HTTP discovery (remembered by MAC since start)";
    DISCOVERY_LAST_SUCCESS "discovery_last_success_timestamp_seconds" Gauge "End of the last discovery (Unix seconds)";

    // All devices
    WIFI_RSSI "wifi_rssi_dbm" Gauge "Wi-Fi signal strength (battery devices: at the last message)";
    UPTIME "uptime_seconds" Gauge "Uptime since the last restart (battery devices: at the last message)";

    // Polled devices
    UP "up" Gauge "1 if the device answered the last poll";
    INFO "info" Gauge "Model, generation and firmware (value is always 1)";
    POWER "power_watts" Gauge "Current active power";
    ENERGY "energy_wh_total" Counter "Device energy counter in Wh (resets on device restart)";
    VOLTAGE "voltage_volts" Gauge "Mains voltage";
    CURRENT "current_amperes" Gauge "Current";
    TEMPERATURE "temperature_celsius" Gauge "Internal device temperature";
    OVERTEMPERATURE "overtemperature" Gauge "1 if the device shut down because of overtemperature";
    OUTPUT_ON "output_on" Gauge "1 if the relay or light is on";
    COVER_POSITION "cover_position_percent" Gauge "Cover position (0 = closed, 100 = open)";
    COVER_STATE "cover_state" Gauge "Cover state (1 for the current one)";
    LIGHT_BRIGHTNESS "light_brightness_percent" Gauge "Dimmer brightness";

    // Devices reporting via MQTT
    LAST_SEEN "last_seen_timestamp_seconds" Gauge "Time of the device's last message (Unix seconds)";
    MQTT_ONLINE "mqtt_online" Gauge "Device connected via MQTT; battery devices: 0 after the sleep event (normal, they are asleep)";
    UPDATE_AVAILABLE "update_available" Gauge "1 if a new stable firmware is available";
    BATTERY_PERCENT "battery_percent" Gauge "Battery level in percent";
    BATTERY_VOLTS "battery_volts" Gauge "Battery voltage";
    TEMPERATURE_SENSOR "sensor_temperature_celsius" Gauge "Temperature at the last message";
    HUMIDITY_SENSOR "sensor_humidity_percent" Gauge "Relative humidity at the last message";
    SENSOR_ERROR "sensor_error" Gauge "Sensor error code at the last message (0 = none)";
    FLOOD_ALARM "flood_alarm" Gauge "1 if the flood sensor reports water";
    FLOOD_LAST_ALARM "flood_last_alarm_timestamp_seconds" Gauge "Time water was last reported";
    SMOKE_ALARM "smoke_alarm" Gauge "1 if the smoke detector reports an alarm";
    SMOKE_MUTED "smoke_muted" Gauge "1 if the alarm is muted";
    SMOKE_LAST_ALARM "smoke_last_alarm_timestamp_seconds" Gauge "Time of the last alarm event";
    SMOKE_LAST_TEST "smoke_last_test_timestamp_seconds" Gauge "Time of the last test alarm (button)";
}
