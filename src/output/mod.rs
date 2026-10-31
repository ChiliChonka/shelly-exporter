//! Output formats. Prometheus is the only one so far; see CONTRIBUTING.md for adding another.
//!
//! A format is a pure function of a snapshot and knows nothing about MQTT or HTTP polling. The
//! snapshot is taken from memory, so rendering never triggers a device request. A format that is
//! pushed instead of scraped (InfluxDB line protocol, for example) can use the same trait from a
//! timer.

pub mod prometheus;

use crate::model::Reading;

pub trait Format: Send + Sync {
    fn content_type(&self) -> &'static str;
    fn render(&self, readings: &[Reading]) -> String;
}
