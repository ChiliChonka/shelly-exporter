//! Mains-powered Shellys via HTTP: discovery and polling.
//!
//! - Discovery: `GET /shelly` on every address of the configured networks. Devices are remembered
//!   by MAC: a new IP just moves the entry, and a device that stops answering stays known with
//!   `up 0` (otherwise it would silently disappear and an offline alert would have nothing to
//!   count).
//! - Polling runs in the background and keeps the result in memory, so a scrape or any other
//!   output never causes a device request.
//! - Password (optional): Gen1 Basic, Gen2+ Digest with SHA-256. Discovery never sends it: it
//!   talks to every host in the network, and `/shelly` needs no login.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{info, warn};

use super::parse;
use crate::model::{DEVICES_DISCOVERED, DISCOVERY_LAST_SUCCESS, INFO, Labels, Reading, UP, now};

const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
const STATUS_TIMEOUT: Duration = Duration::from_secs(3);
const DISCOVERY_PARALLEL: usize = 64;
const POLL_PARALLEL: usize = 16;
/// Discovery talks to every host in the network; nothing that answers may fill the memory.
const MAX_RESPONSE_BYTES: usize = 1 << 20;
/// Largest network accepted in `SHELLY_NETWORK` (/16 = 65534 hosts).
const MIN_PREFIX: u32 = 16;

/// Battery devices (field `app` in `/shelly`) sleep nearly always. Discovered once while awake,
/// they would stay known with `up 0` forever. They report via MQTT instead.
const BATTERY_APPS: [&str; 1] = ["PlusSmoke"];

/// `192.0.2.0/24,10.0.0.0/28` → host addresses, without network and broadcast address.
pub fn parse_networks(raw: &str) -> Result<Vec<String>, String> {
    let mut hosts = Vec::new();
    for net in raw.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let (addr, prefix) = net.split_once('/').unwrap_or((net, "32"));
        let addr: Ipv4Addr = addr.parse().map_err(|e| format!("{net}: {e}"))?;
        let prefix: u32 = prefix.parse().map_err(|e| format!("{net}: {e}"))?;
        if !(MIN_PREFIX..=32).contains(&prefix) {
            return Err(format!("{net}: prefix must be {MIN_PREFIX}..32"));
        }
        let mask = u32::MAX.checked_shl(32 - prefix).unwrap_or(0);
        let first = u32::from(addr) & mask;
        let last = first | !mask;
        let range = if prefix >= 31 {
            first..=last
        } else {
            first + 1..=last - 1
        };
        hosts.extend(range.map(|ip| Ipv4Addr::from(ip).to_string()));
    }
    Ok(hosts)
}

/// Login of the devices. Gen2+ only accepts the user `admin`.
#[derive(Clone)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

/// How the exporter reaches a device. Separate from the poller so tests can fake the network.
pub trait Fetch: Send + Sync + 'static {
    /// `generation = None`: no login (discovery).
    fn get(
        &self,
        host: &str,
        path: &str,
        generation: Option<u64>,
        timeout: Duration,
    ) -> impl Future<Output = Result<Value, String>> + Send;
}

pub struct Client {
    http: reqwest::Client,
    credentials: Option<Credentials>,
}

impl Client {
    pub fn new(credentials: Option<Credentials>) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .no_proxy()
            // Shellys handle few connections: do not keep any open between two polls.
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self { http, credentials })
    }

    async fn read(response: reqwest::Response) -> Result<Value, String> {
        let status = response.status();
        if !status.is_success() {
            return Err(format!("HTTP {status}"));
        }
        let mut response = response;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            body.extend_from_slice(&chunk);
            if body.len() > MAX_RESPONSE_BYTES {
                return Err("response too large".to_string());
            }
        }
        serde_json::from_slice(&body).map_err(|e| e.to_string())
    }
}

impl Fetch for Client {
    async fn get(
        &self,
        host: &str,
        path: &str,
        generation: Option<u64>,
        timeout: Duration,
    ) -> Result<Value, String> {
        let url = format!("http://{host}{path}");
        let mut request = self.http.get(&url).timeout(timeout);
        let credentials = generation.and(self.credentials.as_ref());
        if let (Some(1), Some(c)) = (generation, credentials) {
            request = request.basic_auth(&c.user, Some(&c.password));
        }
        let response = request.send().await.map_err(|e| e.to_string())?;
        if response.status() == StatusCode::UNAUTHORIZED
            && let Some(c) = credentials
            && let Some(challenge) = response
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|v| v.to_str().ok())
                .filter(|v| v.to_ascii_lowercase().starts_with("digest"))
        {
            let cnonce = format!("{:016x}", now().to_bits());
            let auth = digest_header(challenge, "GET", path, c, &cnonce)?;
            let retry = self.http.get(&url).timeout(timeout);
            let retry = retry.header(AUTHORIZATION, auth);
            return Self::read(retry.send().await.map_err(|e| e.to_string())?).await;
        }
        Self::read(response).await
    }
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// `Authorization` value for Digest with SHA-256 and `qop=auth` (RFC 7616).
fn digest_header(
    challenge: &str,
    method: &str,
    uri: &str,
    c: &Credentials,
    cnonce: &str,
) -> Result<String, String> {
    let param = |name: &str| {
        challenge
            .trim_start_matches(|ch: char| ch.is_alphabetic())
            .split(',')
            .filter_map(|p| p.split_once('='))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().trim_matches('"').to_string())
    };
    let algorithm = param("algorithm").unwrap_or_else(|| "MD5".to_string());
    if !algorithm.eq_ignore_ascii_case("SHA-256") {
        return Err(format!("digest algorithm {algorithm} not supported"));
    }
    let realm = param("realm").ok_or("digest without realm")?;
    let nonce = param("nonce").ok_or("digest without nonce")?;
    let nc = "00000001";
    let ha1 = sha256_hex(&format!("{}:{realm}:{}", c.user, c.password));
    let ha2 = sha256_hex(&format!("{method}:{uri}"));
    let response = sha256_hex(&format!("{ha1}:{nonce}:{nc}:{cnonce}:auth:{ha2}"));
    Ok(format!(
        r#"Digest username="{}", realm="{realm}", nonce="{nonce}", uri="{uri}", algorithm=SHA-256, response="{response}", qop=auth, nc={nc}, cnonce="{cnonce}""#,
        c.user
    ))
}

/// Result of a discovery hit.
#[derive(Debug, Clone, PartialEq)]
pub struct Found {
    pub ip: String,
    pub mac: String,
    pub generation: u64,
    pub model: String,
    pub firmware: String,
}

/// Gen2+ names its app, Gen1 says `sleep_mode` (Flood, H&T).
fn sleeps(info: &Value) -> bool {
    let app = info.get("app").and_then(Value::as_str);
    app.is_some_and(|a| BATTERY_APPS.contains(&a))
        || info.get("sleep_mode").and_then(Value::as_bool) == Some(true)
}

/// `/shelly` response → device that can be polled. `None`: not a Shelly, or it sleeps.
pub fn identify(ip: &str, info: &Value) -> Option<Found> {
    describe(ip, info).filter(|_| !sleeps(info))
}

/// `/shelly` response → device, also a sleeping one that happens to be awake.
pub fn describe(ip: &str, info: &Value) -> Option<Found> {
    let text = |key: &str| info.get(key).and_then(Value::as_str);
    let mac = text("mac").filter(|m| !m.is_empty())?.to_uppercase();
    Some(Found {
        ip: ip.to_string(),
        mac,
        generation: info.get("gen").and_then(Value::as_u64).unwrap_or(1),
        model: text("model")
            .or(text("type"))
            .unwrap_or("unknown")
            .to_string(),
        firmware: text("fw_id").or(text("fw")).unwrap_or("").to_string(),
    })
}

/// Device name from the configuration, otherwise the hostname, otherwise the MAC.
fn name_from_config(found: &Found, config: &Value) -> String {
    let device = config.get("device");
    let field = |v: Option<&Value>, key: &str| {
        v.and_then(|v| v.get(key))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    if found.generation == 1 {
        field(Some(config), "name")
            .or_else(|| field(device, "hostname"))
            .unwrap_or_else(|| found.mac.clone())
    } else {
        field(device, "name").unwrap_or_else(|| format!("shelly-{}", found.mac.to_lowercase()))
    }
}

/// `(status path, config path)` per generation.
pub fn paths(generation: u64) -> (&'static str, &'static str) {
    if generation == 1 {
        ("/status", "/settings")
    } else {
        ("/rpc/Shelly.GetStatus", "/rpc/Sys.GetConfig")
    }
}

#[derive(Debug)]
struct Polled {
    found: Found,
    name: String,
    up: bool,
    readings: Vec<Reading>,
}

impl Polled {
    fn labels(&self) -> Labels {
        vec![
            ("device", self.name.clone()),
            ("mac", self.found.mac.clone()),
        ]
    }

    fn parse(&self, status: &Value) -> Vec<Reading> {
        if self.found.generation == 1 {
            parse::gen1(status, &self.labels())
        } else {
            parse::gen2(status, &self.labels())
        }
    }
}

pub struct Poller<F: Fetch> {
    fetch: Arc<F>,
    hosts: Vec<String>,
    devices: Mutex<BTreeMap<String, Polled>>,
    last_discovery: Mutex<Option<f64>>,
}

impl<F: Fetch> Poller<F> {
    pub fn new(fetch: F, hosts: Vec<String>) -> Self {
        Self {
            fetch: Arc::new(fetch),
            hosts,
            devices: Mutex::new(BTreeMap::new()),
            last_discovery: Mutex::new(None),
        }
    }

    /// Scan the networks. Returns the number of devices that answered.
    pub async fn discover(&self) -> usize {
        let limit = Arc::new(Semaphore::new(DISCOVERY_PARALLEL));
        let mut scan = JoinSet::new();
        for host in self.hosts.clone() {
            let fetch = self.fetch.clone();
            let limit = limit.clone();
            scan.spawn(async move {
                let _slot = limit.acquire().await.ok()?;
                let info = fetch
                    .get(&host, "/shelly", None, PROBE_TIMEOUT)
                    .await
                    .ok()?;
                let found = identify(&host, &info)?;
                let (_, config) = paths(found.generation);
                let name = match fetch
                    .get(&found.ip, config, Some(found.generation), STATUS_TIMEOUT)
                    .await
                {
                    Ok(c) => name_from_config(&found, &c),
                    // A name is no reason to lose the device.
                    Err(error) => {
                        warn!(ip = found.ip, error, "device name not readable");
                        found.mac.clone()
                    }
                };
                Some((found, name))
            });
        }
        let mut answered = 0;
        while let Some(result) = scan.join_next().await {
            let Ok(Some((found, name))) = result else {
                continue;
            };
            answered += 1;
            let mut devices = self.devices.lock().await;
            match devices.get_mut(&found.mac) {
                Some(d) => {
                    d.found = found;
                    d.name = name;
                }
                None => {
                    let polled = Polled {
                        found,
                        name,
                        up: false,
                        readings: Vec::new(),
                    };
                    devices.insert(polled.found.mac.clone(), polled);
                }
            }
        }
        *self.last_discovery.lock().await = Some(now());
        let known = self.devices.lock().await.len();
        info!(answered, known, "discovery finished");
        answered
    }

    /// Ask every known device for its status. One device must not hold up the others.
    pub async fn poll(&self) {
        let targets: Vec<(String, Found)> = {
            let devices = self.devices.lock().await;
            devices
                .iter()
                .map(|(mac, d)| (mac.clone(), d.found.clone()))
                .collect()
        };
        let limit = Arc::new(Semaphore::new(POLL_PARALLEL));
        let mut polls = JoinSet::new();
        for (mac, found) in targets {
            let fetch = self.fetch.clone();
            let limit = limit.clone();
            polls.spawn(async move {
                let _slot = limit.acquire().await;
                let (status, _) = paths(found.generation);
                let result = fetch
                    .get(&found.ip, status, Some(found.generation), STATUS_TIMEOUT)
                    .await;
                (mac, found.ip, result)
            });
        }
        while let Some(joined) = polls.join_next().await {
            let Ok((mac, ip, result)) = joined else {
                continue;
            };
            let mut devices = self.devices.lock().await;
            let Some(device) = devices.get_mut(&mac) else {
                continue;
            };
            match result {
                Ok(status) => {
                    device.readings = device.parse(&status);
                    device.up = true;
                }
                Err(error) => {
                    info!(device = device.name, ip, error, "poll failed");
                    device.readings.clear();
                    device.up = false;
                }
            }
        }
    }

    /// Discovery and polling, forever.
    pub async fn run(self: Arc<Self>, poll_every: Duration, discovery_every: Duration) {
        let mut tick = tokio::time::interval(poll_every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut next_discovery = Instant::now();
        loop {
            tick.tick().await;
            if Instant::now() >= next_discovery {
                self.discover().await;
                next_discovery = Instant::now() + discovery_every;
            }
            self.poll().await;
        }
    }

    pub async fn readings(&self) -> Vec<Reading> {
        let devices = self.devices.lock().await;
        let mut out = vec![Reading::new(
            &DEVICES_DISCOVERED,
            Vec::new(),
            devices.len() as f64,
        )];
        if let Some(t) = *self.last_discovery.lock().await {
            out.push(Reading::new(&DISCOVERY_LAST_SUCCESS, Vec::new(), t));
        }
        for d in devices.values() {
            let base = d.labels();
            let mut info = base.clone();
            info.push(("model", d.found.model.clone()));
            info.push(("gen", d.found.generation.to_string()));
            info.push(("firmware", d.found.firmware.clone()));
            info.push(("ip", d.found.ip.clone()));
            out.push(Reading::new(&UP, base, f64::from(u8::from(d.up))));
            out.push(Reading::new(&INFO, info, 1.0));
            out.extend(d.readings.iter().cloned());
        }
        out
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::capture;
    use axum::Router;
    use axum::http::{HeaderMap, StatusCode as AxumStatus, header};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Mutex as StdMutex;

    /// A device as stored in `tests/fixtures/devices/<name>.json`.
    pub fn fixture(name: &str) -> Value {
        let path = format!(
            "{}/tests/fixtures/devices/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    /// The value of the one reading with this name and (at least) these labels.
    pub fn value(readings: &[Reading], name: &str, labels: &[(&str, &str)]) -> f64 {
        let hits: Vec<f64> = readings
            .iter()
            .filter(|r| r.metric.name == name)
            .filter(|r| {
                labels
                    .iter()
                    .all(|(k, v)| r.labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|r| r.value)
            .collect();
        assert_eq!(hits.len(), 1, "{name}{labels:?}: {hits:?}");
        hits[0]
    }

    /// Fake network: answers from fixtures, and remembers whether a login was offered.
    #[derive(Default)]
    struct FakeNet {
        answers: StdMutex<HashMap<(String, String), Value>>,
        unauthenticated_paths: StdMutex<BTreeSet<String>>,
    }

    impl FakeNet {
        fn device(self, ip: &str, name: &str) -> Self {
            let f = fixture(name);
            let generation = f["shelly"]["gen"].as_u64().unwrap_or(1);
            let (status, config) = paths(generation);
            let mut answers = self.answers.lock().unwrap();
            answers.insert((ip.into(), "/shelly".into()), f["shelly"].clone());
            answers.insert((ip.into(), status.into()), f["status"].clone());
            answers.insert((ip.into(), config.into()), f["config"].clone());
            drop(answers);
            self
        }
    }

    impl Fetch for Arc<FakeNet> {
        async fn get(
            &self,
            host: &str,
            path: &str,
            generation: Option<u64>,
            _timeout: Duration,
        ) -> Result<Value, String> {
            if generation.is_none() {
                self.unauthenticated_paths
                    .lock()
                    .unwrap()
                    .insert(path.to_string());
            }
            self.answers
                .lock()
                .unwrap()
                .get(&(host.to_string(), path.to_string()))
                .cloned()
                .ok_or_else(|| "unreachable".to_string())
        }
    }

    fn hosts() -> Vec<String> {
        parse_networks("192.0.2.0/24").unwrap()
    }

    fn net() -> Arc<FakeNet> {
        Arc::new(
            FakeNet::default()
                .device("192.0.2.164", "shelly-25-roller")
                .device("192.0.2.39", "dimmer-2")
                .device("192.0.2.63", "plug-s-gen3"),
        )
    }

    #[tokio::test]
    async fn discovery_finds_and_names_the_devices() {
        let poller = Poller::new(net(), hosts());
        assert_eq!(poller.discover().await, 3);
        poller.poll().await;
        let r = poller.readings().await;
        assert_eq!(value(&r, "devices_discovered", &[]), 3.0);
        assert_eq!(
            r.iter()
                .filter(|x| x.metric.name == "up")
                .map(|x| x.value)
                .sum::<f64>(),
            3.0
        );
        assert_eq!(
            value(&r, "info", &[("device", "example-plug"), ("gen", "3")]),
            1.0
        );
        assert_eq!(
            value(&r, "info", &[("device", "example-roller"), ("gen", "1")]),
            1.0
        );
        // No name configured: falls back to the hostname.
        let dimmer = r
            .iter()
            .filter(|x| x.metric.name == "info")
            .find(|x| x.labels.iter().any(|(k, v)| *k == "model" && v == "SHDM-2"));
        assert!(
            dimmer.unwrap().labels[0].1.starts_with("shellydimmer2-"),
            "{dimmer:?}"
        );
        assert!(
            (value(
                &r,
                "energy_wh_total",
                &[("device", "example-roller"), ("channel", "0")]
            ) - 72.5833)
                .abs()
                < 0.001
        );
    }

    #[tokio::test]
    async fn a_vanished_device_stays_known_with_up_0() {
        let net = net();
        let poller = Poller::new(net.clone(), hosts());
        poller.discover().await;
        poller.poll().await;
        let r = poller.readings().await;
        assert_eq!(value(&r, "up", &[("device", "example-plug")]), 1.0);
        assert!(
            r.iter()
                .any(|x| x.metric.name == "voltage_volts" && x.labels[0].1 == "example-plug"),
            "control: values before the failure"
        );

        net.answers
            .lock()
            .unwrap()
            .retain(|(ip, _), _| ip != "192.0.2.63");
        assert_eq!(poller.discover().await, 2);
        poller.poll().await;
        let r = poller.readings().await;
        assert_eq!(value(&r, "devices_discovered", &[]), 3.0);
        assert_eq!(value(&r, "up", &[("device", "example-plug")]), 0.0);
        assert_eq!(
            value(&r, "up", &[("device", "example-roller")]),
            1.0,
            "control"
        );
        assert!(
            !r.iter()
                .any(|x| x.metric.name == "voltage_volts" && x.labels[0].1 == "example-plug"),
            "no stale values"
        );
    }

    #[tokio::test]
    async fn a_new_ip_moves_the_entry() {
        let net = net();
        let poller = Poller::new(net.clone(), hosts());
        poller.discover().await;
        {
            let mut a = net.answers.lock().unwrap();
            let moved: Vec<_> = a
                .keys()
                .filter(|(ip, _)| ip == "192.0.2.63")
                .cloned()
                .collect();
            for key in moved {
                let v = a.remove(&key).unwrap();
                a.insert(("192.0.2.64".to_string(), key.1), v);
            }
        }
        poller.discover().await;
        poller.poll().await;
        let r = poller.readings().await;
        assert_eq!(value(&r, "devices_discovered", &[]), 3.0);
        assert_eq!(
            value(
                &r,
                "info",
                &[("device", "example-plug"), ("ip", "192.0.2.64")]
            ),
            1.0
        );
        assert_eq!(value(&r, "up", &[("device", "example-plug")]), 1.0);
    }

    #[tokio::test]
    async fn an_awake_smoke_detector_is_not_added() {
        let net = Arc::new(
            FakeNet::default()
                .device("192.0.2.164", "shelly-25-roller")
                .device("192.0.2.237", "plus-smoke"),
        );
        let poller = Poller::new(net.clone(), hosts());
        poller.discover().await;
        let r = poller.readings().await;
        assert_eq!(value(&r, "devices_discovered", &[]), 1.0);
        // Control: the detector did answer, and only the app field keeps it out.
        let answers = net.answers.lock().unwrap();
        let info = &answers[&("192.0.2.237".to_string(), "/shelly".to_string())];
        assert_eq!(info["app"], "PlusSmoke");
    }

    #[tokio::test]
    async fn a_sleeping_gen1_device_is_not_added() {
        // Gen1 has no app field: it says sleep_mode. Real response of a Shelly Flood.
        let net = Arc::new(
            FakeNet::default()
                .device("192.0.2.164", "shelly-25-roller")
                .device("192.0.2.213", "flood-gen1"),
        );
        let poller = Poller::new(net.clone(), hosts());
        poller.discover().await;
        let r = poller.readings().await;
        assert_eq!(value(&r, "devices_discovered", &[]), 1.0);
        // Control: it did answer, so only sleep_mode keeps it out.
        let answers = net.answers.lock().unwrap();
        let info = &answers[&("192.0.2.213".to_string(), "/shelly".to_string())];
        assert_eq!(info["sleep_mode"], true);
    }

    #[tokio::test]
    async fn discovery_never_offers_the_login() {
        let net = net();
        let poller = Poller::new(net.clone(), hosts());
        poller.discover().await;
        let unauthenticated = net.unauthenticated_paths.lock().unwrap().clone();
        assert_eq!(unauthenticated, BTreeSet::from(["/shelly".to_string()]));
    }

    #[test]
    fn networks() {
        assert_eq!(
            parse_networks("192.0.2.0/30").unwrap(),
            ["192.0.2.1", "192.0.2.2"]
        );
        assert_eq!(parse_networks("192.0.2.7/32").unwrap(), ["192.0.2.7"]);
        assert_eq!(parse_networks("192.0.2.0/24").unwrap().len(), 254);
        assert_eq!(parse_networks("192.0.2.9,192.0.2.0/31").unwrap().len(), 3);
        assert!(parse_networks("10.0.0.0/8").is_err(), "too large");
        assert!(parse_networks("nonsense/24").is_err());
    }

    // Expected value computed separately with Python hashlib, not with this code.
    #[test]
    fn digest_matches_an_independent_calculation() {
        let challenge = r#"Digest realm="http-auth@example.org", qop="auth, auth-int", algorithm=SHA-256, nonce="7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v", opaque="FdR""#;
        let c = Credentials {
            user: "Mufasa".to_string(),
            password: "Circle of Life".to_string(),
        };
        let cnonce = "f2/wE4q74E6zIJEtWaHKaf5wv/H3nzbBjcvLUKHVqjY";
        let auth = digest_header(challenge, "GET", "/dir/index.html", &c, cnonce).unwrap();
        assert!(
            auth.contains(
                r#"response="fec694df953657e57d87b7d8cd341b45529bb6fad5e5603076ddd18494dbaf89""#
            ),
            "{auth}"
        );
    }

    #[test]
    fn digest_with_md5_is_refused_not_guessed() {
        let c = Credentials {
            user: "a".into(),
            password: "b".into(),
        };
        let e = digest_header(r#"Digest realm="r", nonce="n""#, "GET", "/", &c, "x").unwrap_err();
        assert!(e.contains("MD5"), "{e}");
    }

    /// A device behind Digest, as a real HTTP server: it computes the answer itself.
    async fn digest_server() -> String {
        async fn handler(headers: HeaderMap) -> impl IntoResponse {
            let auth = headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let p = |k: &str| {
                auth.trim_start_matches("Digest ")
                    .split(", ")
                    .filter_map(|e| e.split_once('='))
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.trim_matches('"').to_string())
                    .unwrap_or_default()
            };
            let ha1 = sha256_hex(&format!("admin:{}:secret", p("realm")));
            let ha2 = sha256_hex(&format!("GET:{}", p("uri")));
            let expected = sha256_hex(&format!(
                "{ha1}:{}:{}:{}:{}:{ha2}",
                p("nonce"),
                p("nc"),
                p("cnonce"),
                p("qop")
            ));
            if auth.starts_with("Digest ") && p("response") == expected {
                return (AxumStatus::OK, HeaderMap::new(), r#"{"ok":true}"#);
            }
            let mut h = HeaderMap::new();
            h.insert(
                header::WWW_AUTHENTICATE,
                r#"Digest qop="auth", realm="shelly-test", nonce="60dc59c8", algorithm=SHA-256"#
                    .parse()
                    .unwrap(),
            );
            (AxumStatus::UNAUTHORIZED, h, "")
        }
        let router = Router::new().route("/rpc/Shelly.GetStatus", get(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, router).await });
        addr
    }

    async fn fetch_with(password: Option<&str>, host: &str) -> Result<Value, String> {
        let credentials = password.map(|p| Credentials {
            user: "admin".to_string(),
            password: p.to_string(),
        });
        Client::new(credentials)
            .unwrap()
            .get(
                host,
                "/rpc/Shelly.GetStatus",
                Some(3),
                Duration::from_secs(2),
            )
            .await
    }

    #[tokio::test]
    async fn digest_login_against_a_server() {
        let host = digest_server().await;
        assert_eq!(fetch_with(Some("secret"), &host).await.unwrap()["ok"], true);
        let wrong = fetch_with(Some("wrong"), &host).await.unwrap_err();
        assert!(wrong.contains("401"), "{wrong}");
        let none = fetch_with(None, &host).await.unwrap_err();
        assert!(none.contains("401"), "{none}");
    }

    /// Runs over every file, so a contributed device is covered without writing a test.
    #[test]
    fn every_fixture_is_a_usable_device_without_private_data() {
        let dir = format!("{}/tests/fixtures/devices", env!("CARGO_MANIFEST_DIR"));
        let mut checked = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let text = std::fs::read_to_string(&path).unwrap();
            let f: Value = serde_json::from_str(&text).unwrap();
            if let Err(e) = capture::check_clean(&f) {
                panic!("{}: {e}", path.display());
            }
            let found = identify("192.0.2.1", &f["shelly"]);
            if sleeps(&f["shelly"]) {
                assert!(found.is_none(), "{}", path.display());
            } else {
                let found = found.unwrap_or_else(|| panic!("{}: not identified", path.display()));
                let base = vec![("device", "x".to_string())];
                let r = if found.generation == 1 {
                    parse::gen1(&f["status"], &base)
                } else {
                    parse::gen2(&f["status"], &base)
                };
                assert!(!r.is_empty(), "{}: nothing parsed", path.display());
            }
            checked += 1;
        }
        assert!(checked >= 5, "only {checked} fixtures found");
    }
}
