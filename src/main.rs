//! shelly-exporter: reads Shellys via MQTT and HTTP and exposes them as metrics.
//!
//! Configuration via environment variables, see README.md. `MQTT_HOST` enables the MQTT source,
//! `SHELLY_NETWORK` the HTTP source; at least one is required.
//!
//! `shelly-exporter capture <ip>` prints a device's responses without private data, for
//! adding support for a new device (CONTRIBUTING.md).

mod capture;
mod model;
mod output;
mod source;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use tracing::{error, info};

use model::{EXPORTER_DEVICES, EXPORTER_MQTT_CONNECTED, Reading};
use output::Format;
use output::prometheus::Prometheus;
use source::http::{Client, Credentials, Poller};
use source::mqtt;

struct App {
    mqtt: Option<Arc<mqtt::State>>,
    http: Option<Arc<Poller<Client>>>,
    format: Box<dyn Format>,
}

impl App {
    async fn snapshot(&self) -> Vec<Reading> {
        let mut out = Vec::new();
        if let Some(state) = &self.mqtt {
            let (connected, devices, readings) = state.readings().await;
            let connected = f64::from(u8::from(connected));
            out.push(Reading::new(
                &EXPORTER_MQTT_CONNECTED,
                Vec::new(),
                connected,
            ));
            out.push(Reading::new(&EXPORTER_DEVICES, Vec::new(), devices as f64));
            out.extend(readings);
        }
        if let Some(poller) = &self.http {
            out.extend(poller.readings().await);
        }
        out
    }
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_seconds(name: &str, default: u64) -> Duration {
    Duration::from_secs(env(name, "").parse().unwrap_or(default).max(1))
}

/// `id=name,id2=name2` → map. Empty or malformed entries are skipped.
fn parse_names(raw: &str) -> BTreeMap<String, String> {
    raw.split(',')
        .filter_map(|e| e.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .filter(|(k, v)| !k.is_empty() && !v.is_empty())
        .collect()
}

fn credentials() -> Option<Credentials> {
    let password = std::env::var("SHELLY_PASSWORD")
        .ok()
        .filter(|p| !p.is_empty())?;
    Some(Credentials {
        user: env("SHELLY_USER", "admin"),
        password,
    })
}

fn fail(message: &str) -> ! {
    error!("{message}");
    std::process::exit(2);
}

async fn metrics_handler(State(app): State<Arc<App>>) -> impl IntoResponse {
    let body = app.format.render(&app.snapshot().await);
    ([(header::CONTENT_TYPE, app.format.content_type())], body)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().expect("filter")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.get(1).is_some_and(|a| a == "capture") {
        let Some(host) = args.get(2) else {
            fail("usage: shelly-exporter capture <ip>");
        };
        let client = Client::new(credentials()).unwrap_or_else(|e| fail(&e));
        match capture::run(host, client).await {
            Ok(text) => println!("{text}"),
            Err(e) => fail(&e),
        }
        return;
    }

    let mqtt_host = std::env::var("MQTT_HOST").ok().filter(|h| !h.is_empty());
    let network = std::env::var("SHELLY_NETWORK")
        .ok()
        .filter(|n| !n.is_empty());
    if mqtt_host.is_none() && network.is_none() {
        fail("nothing to read: set MQTT_HOST, SHELLY_NETWORK or both");
    }

    let mqtt = mqtt_host.map(|host| {
        let state = Arc::new(mqtt::State::new(parse_names(&env("SHELLY_NAMES", ""))));
        let config = mqtt::Config {
            host,
            port: env("MQTT_PORT", "1883").parse().unwrap_or(1883),
            client_id: env("MQTT_CLIENT_ID", "shelly-exporter"),
            user: std::env::var("MQTT_USER").ok(),
            password: std::env::var("MQTT_PASSWORD").unwrap_or_default(),
        };
        mqtt::spawn(state.clone(), config);
        state
    });

    let http = network.map(|network| {
        let hosts = source::http::parse_networks(&network).unwrap_or_else(|e| fail(&e));
        let client = Client::new(credentials()).unwrap_or_else(|e| fail(&e));
        info!(network, hosts = hosts.len(), "HTTP source enabled");
        let poller = Arc::new(Poller::new(client, hosts));
        let poll_every = env_seconds("SHELLY_POLL_SECONDS", 60);
        let discovery_every = env_seconds("SHELLY_DISCOVERY_SECONDS", 600);
        tokio::spawn(poller.clone().run(poll_every, discovery_every));
        poller
    });

    let app = Arc::new(App {
        mqtt,
        http,
        format: Box::new(Prometheus),
    });
    let listen = env("LISTEN", "0.0.0.0:9926");
    let router = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(|| async { "ok\n" }))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|e| fail(&format!("{listen}: {e}")));
    info!(listen, "HTTP ready");
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    if let Err(e) = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
    {
        error!(error = %e, "HTTP server stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_from_env() {
        let n = parse_names("shellyplussmoke-a=hall, shellyplussmoke-b = kitchen,broken,=x,y=");
        assert_eq!(n.len(), 2);
        assert_eq!(n["shellyplussmoke-b"], "kitchen");
    }
}
