//! Prometheus text format: `shelly_<metric>{labels} value`.

use std::collections::BTreeMap;
use std::fmt::Write;

use super::Format;
use crate::model::{Kind, Metric, Reading};

const PREFIX: &str = "shelly_";

pub struct Prometheus;

/// Value without rounding: integers without decimals, otherwise the shortest exact representation.
/// A rounded energy counter loses the consumption of a whole hour once it gets large.
fn fmt_value(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

impl Format for Prometheus {
    fn content_type(&self) -> &'static str {
        "text/plain; version=0.0.4"
    }

    fn render(&self, readings: &[Reading]) -> String {
        // Both sources report some metrics (uptime, Wi-Fi): HELP and TYPE may appear only once.
        let mut families: BTreeMap<&str, (&Metric, Vec<&Reading>)> = BTreeMap::new();
        for r in readings {
            families
                .entry(r.metric.name)
                .or_insert((r.metric, Vec::new()))
                .1
                .push(r);
        }
        let mut out = String::new();
        for (name, (metric, family)) in families {
            let kind = match metric.kind {
                Kind::Gauge => "gauge",
                Kind::Counter => "counter",
            };
            let _ = writeln!(out, "# HELP {PREFIX}{name} {}", metric.help);
            let _ = writeln!(out, "# TYPE {PREFIX}{name} {kind}");
            for r in family {
                let value = fmt_value(r.value);
                if r.labels.is_empty() {
                    let _ = writeln!(out, "{PREFIX}{name} {value}");
                    continue;
                }
                let labels: Vec<String> = r
                    .labels
                    .iter()
                    .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
                    .collect();
                let _ = writeln!(out, "{PREFIX}{name}{{{}}} {value}", labels.join(","));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ENERGY, EXPORTER_DEVICES, UP, UPTIME};

    fn labels(v: &[(&'static str, &str)]) -> Vec<(&'static str, String)> {
        v.iter().map(|(k, v)| (*k, v.to_string())).collect()
    }

    #[test]
    fn help_type_and_escaped_labels() {
        let text = Prometheus.render(&[
            Reading::new(&UP, labels(&[("device", "a\"b\\c"), ("mac", "AA")]), 1.0),
            Reading::new(
                &ENERGY,
                labels(&[("channel", "0"), ("device", "x")]),
                72.5833,
            ),
            Reading::new(&EXPORTER_DEVICES, vec![], 2.0),
        ]);
        assert!(
            text.contains("# TYPE shelly_energy_wh_total counter"),
            "{text}"
        );
        assert!(text.contains("# TYPE shelly_up gauge"), "{text}");
        assert!(
            text.contains(r#"shelly_up{device="a\"b\\c",mac="AA"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"shelly_energy_wh_total{channel="0",device="x"} 72.5833"#),
            "{text}"
        );
        assert!(text.contains("shelly_exporter_devices 2\n"), "{text}");
    }

    #[test]
    fn counters_keep_all_digits() {
        // A real plug: 250581.917 Wh. Rounded to six digits this would read 250582.
        let text = Prometheus.render(&[
            Reading::new(&ENERGY, labels(&[("device", "p")]), 250581.917),
            Reading::new(&UPTIME, labels(&[("device", "p")]), 23204623.0),
        ]);
        assert!(
            text.contains("shelly_energy_wh_total{device=\"p\"} 250581.917\n"),
            "{text}"
        );
        assert!(
            text.contains("shelly_uptime_seconds{device=\"p\"} 23204623\n"),
            "{text}"
        );
    }

    #[test]
    fn a_metric_from_two_sources_has_one_header() {
        let text = Prometheus.render(&[
            Reading::new(&UPTIME, labels(&[("device", "polled")]), 1.0),
            Reading::new(&UP, labels(&[("device", "polled")]), 1.0),
            Reading::new(&UPTIME, labels(&[("device", "smoke")]), 2.0),
        ]);
        assert_eq!(
            text.matches("# TYPE shelly_uptime_seconds").count(),
            1,
            "{text}"
        );
        assert!(text.contains("device=\"smoke\"} 2\n"), "{text}");
    }
}
