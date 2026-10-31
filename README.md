# shelly-exporter

Prometheus exporter for [Shelly](https://shelly.com) devices. One small binary for your whole
Shelly setup: mains-powered devices are polled via **HTTP**, and battery devices that sleep nearly
all the time report via **MQTT**. It works without the Shelly Cloud.

Both sources are optional. Use whichever your devices need.

## Which device needs what?

| Device | Source | Why | Change on the device |
|---|---|---|---|
| Mains-powered, any generation | HTTP | Always awake, can simply be asked | None |
| Battery-powered (sleeping) | MQTT | Awake for seconds only, **cannot be polled**. It has to report by itself | Enable MQTT, then restart it |

A sleeping device is the reason MQTT exists here. A Shelly Plus Smoke is awake for 2 to 6 seconds
once a day, and on alarm or test. An HTTP request almost always finds nobody home, so the exporter
needs a broker that receives the message while the device is awake.

This has consequences for monitoring battery devices:

- Values are as old as the last wake-up, up to a day for a smoke detector. The exporter keeps the
  last state, including across restarts.
- `shelly_mqtt_online 0` is normal for a sleeping device. To notice a dead detector, alert on
  `shelly_last_seen_timestamp_seconds`, see [`examples/alerts.yml`](examples/alerts.yml).

### Tested devices

| Device | Generation | Source | Values |
|---|---|---|---|
| Shelly 2.5 as roller (`SHSW-25`) | Gen1 | HTTP | Power, energy, voltage, temperature, cover |
| Shelly Dimmer 2 (`SHDM-2`) | Gen1 | HTTP | Power, energy, temperature, output, brightness |
| Shelly Plug S Gen3 (`S3PL`) | Gen3 | HTTP | Power, energy, voltage, current, temperature, output |
| Shelly 2PM Gen3 as cover (`S3SW`) | Gen3 | HTTP | Power, energy, voltage, current, temperature, cover |
| Shelly Plus Smoke (`SNSN-0031Z`) | Gen2 | MQTT | Alarm, test, mute, battery |

Other devices probably work to some degree: any Gen1 device with `meters`, `relays`, `rollers` or
`lights`, and any Gen2+ device with `switch`, `cover` or `light` components. Other battery devices
such as the Plus H&T are **untested**. Missing yours? [CONTRIBUTING.md](CONTRIBUTING.md) explains
what we need. Usually that is one command and no Rust.

## Quick start

Mains-powered devices only:

```bash
docker run --rm -p 9926:9926 -e SHELLY_NETWORK=192.0.2.0/24 \
  ghcr.io/chilichonka/shelly-exporter:0.2.0
curl localhost:9926/metrics
```

The container has to reach the devices. If they are in another network than Docker's, route to
them or use `--network host`.

With battery devices, add a broker:

```bash
docker run --rm -p 9926:9926 \
  -e SHELLY_NETWORK=192.0.2.0/24 \
  -e MQTT_HOST=192.0.2.5 -e MQTT_USER=shelly-exporter -e MQTT_PASSWORD=... \
  -e SHELLY_NAMES='shellyplussmoke-aabbccddeeff=Hallway' \
  ghcr.io/chilichonka/shelly-exporter:0.2.0
```

## Configuration

At least one of `SHELLY_NETWORK` and `MQTT_HOST` is required.

| Variable | Default | |
|---|---|---|
| `LISTEN` | `0.0.0.0:9926` | Address for `/metrics` and `/healthz` |
| `SHELLY_NETWORK` | — | Enables HTTP. Networks to scan, comma-separated, e.g. `192.0.2.0/24` (largest: /16) |
| `SHELLY_POLL_SECONDS` | `60` | How often devices are asked |
| `SHELLY_DISCOVERY_SECONDS` | `600` | How often the networks are scanned |
| `SHELLY_USER`, `SHELLY_PASSWORD` | `admin`, — | Device login, if set. Gen1: Basic, Gen2+: Digest SHA-256 (the user is always `admin`). Never sent during the scan |
| `MQTT_HOST` | — | Enables MQTT |
| `MQTT_PORT` | `1883` | |
| `MQTT_USER`, `MQTT_PASSWORD` | — | Broker account |
| `MQTT_CLIENT_ID` | `shelly-exporter` | |
| `SHELLY_NAMES` | — | `device-id=name,…` for MQTT devices: battery devices do not report their name |
| `RUST_LOG` | `info` | Log level. Logs are JSON |

### Setting up MQTT

On the device (Settings → MQTT): server and account, "RPC status notifications" and "Generic status
update" both enabled, then restart the device. A battery device only applies this while awake, so
put it into setup mode first (Plus Smoke: press the button three times).

The broker must allow retained messages. The exporter stores each device's last state there, so
sleeping devices are not lost when it restarts. For Mosquitto, an ACL could look like this:

```text
user shelly-exporter
topic read +/status/#
topic read +/events/rpc
topic read +/online
topic readwrite shelly-exporter/state/#
```

Traffic is plain MQTT without TLS, so keep the broker on a trusted network.

## Metrics

Polled devices have the labels `device` and `mac`, MQTT devices `device` and `id`. Values with
several outputs also have `channel`. A value that is unknown is left out, never reported as 0.

| Metric | Source | Meaning |
|---|---|---|
| `shelly_up` | HTTP | 1 if the device answered the last poll. A device that stops answering stays known with 0 |
| `shelly_info{model,gen,firmware,ip}` | HTTP | Device information, value is always 1 |
| `shelly_power_watts`, `shelly_voltage_volts`, `shelly_current_amperes` | HTTP | Electrical values |
| `shelly_energy_wh_total` | HTTP | Energy counter in Wh for all generations. Resets when the device restarts |
| `shelly_temperature_celsius`, `shelly_overtemperature` | HTTP | Device temperature, 1 if it shut down because of heat |
| `shelly_output_on`, `shelly_light_brightness_percent` | HTTP | Relay or light, dimmer |
| `shelly_cover_position_percent`, `shelly_cover_state{state}` | HTTP | Cover. Position only when calibrated |
| `shelly_devices_discovered`, `shelly_discovery_last_success_timestamp_seconds` | HTTP | The discovery itself |
| `shelly_last_seen_timestamp_seconds` | MQTT | Last message, by the exporter's clock |
| `shelly_smoke_alarm{channel}`, `shelly_smoke_muted{channel}` | MQTT | Alarm, muted |
| `shelly_smoke_last_alarm_timestamp_seconds`, `shelly_smoke_last_test_timestamp_seconds` | MQTT | Last alarm or test, so an event between two scrapes is not lost |
| `shelly_battery_percent`, `shelly_battery_volts` | MQTT | Battery. A read error keeps the last good value |
| `shelly_update_available` | MQTT | 1 if new stable firmware is available |
| `shelly_mqtt_online` | MQTT | Connected. Battery devices: 0 after going to sleep |
| `shelly_exporter_mqtt_connected`, `shelly_exporter_devices` | MQTT | The exporter's broker connection and device count |
| `shelly_wifi_rssi_dbm`, `shelly_uptime_seconds` | Both | Battery devices: at the last wake-up |

A test alarm (holding the button for 3 s) does not set `shelly_smoke_alarm`. It only updates
`shelly_smoke_last_test_timestamp_seconds`.

## How it works

```text
source::mqtt ─┐                       ┌─ output::prometheus  (/metrics)
              ├─ neutral readings ────┤
source::http ─┘   (src/model.rs)      └─ your format?
```

Sources and output formats only meet at the neutral [`Reading`](src/model.rs). Polling runs in the
background, so a scrape answers from memory and never causes a device request. Prometheus is the
only format so far, and adding another needs no change to the sources:
[CONTRIBUTING.md](CONTRIBUTING.md).

- **Discovery:** `GET /shelly` on every address. Devices are remembered by MAC, so a new IP just
  moves the entry.
- **MQTT:** Subscribes to `+/status/#`, `+/events/rpc` and `+/online`. The last state per device is
  stored retained under `shelly-exporter/state/<id>`, in batches every 15 s.
- **Retained messages are no sign of life.** A detector disconnects cleanly, so its `online true`
  stays in the broker forever. If it counted, every restart would mark every silent detector as
  seen.

## Development

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

Rust version: `rust-toolchain.toml` and `Dockerfile`. Release: tag `x.y.z` → CI builds
`ghcr.io/chilichonka/shelly-exporter:x.y.z` (linux/amd64).

## License

[MIT](LICENSE)
