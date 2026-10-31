# Contributing

Support for more devices and more output formats is very welcome. The exporter is only tested
against devices its authors own, so the most useful contribution is often just what your device
actually sends.

## Add a device

You do not need to write Rust for this. Open a
[new device issue](../../issues/new?template=new-device.yml) with the recording described below.

### Mains-powered device (reachable via HTTP)

```bash
docker run --rm ghcr.io/chilichonka/shelly-exporter:0.2.0 \
  /usr/local/bin/shelly-exporter capture 192.0.2.10 > my-device.json
```

`capture` asks the device for `/shelly` and its status, removes the MAC address, IP addresses,
Wi-Fi and server names, and reduces the configuration to the device name. If something private is
left, it prints an error instead of the data. Please look through the file before you share it
anyway: the list of private fields covers what we have seen so far, and your device may have more.
For a device with a password, set `SHELLY_USER` and `SHELLY_PASSWORD`.

If you want to go further:

1. Save the file as `tests/fixtures/devices/<model>.json`. A test runs over every file in that
   directory, so your device is covered from then on: it has to parse, and it must not look like
   real data.
2. Teach `src/source/parse.rs` the fields that are not read yet (`gen1` or `gen2`).
3. Add a test with the expected values **as numbers**, read from the device's web interface, not
   calculated from the fixture. Otherwise a wrong conversion confirms itself.
4. Add the device to the table in the README, with the mode you tested.

Watch the units. Gen1 energy counters are in watt-minutes, Gen2+ in Wh, and readings are always Wh.

### Battery device (reports via MQTT)

It sleeps, so there is nothing to poll. Enable MQTT on it, then record one wake-up:

```bash
mosquitto_sub -h <broker> -u <user> -P <password> -t '+/status/#' -t '+/events/rpc' -t '+/online' -v \
  | tee recording.txt
```

Wake the device by pressing its button. Replace the MAC address in the device ID with
`02aabbccddee`, and remove your Wi-Fi name and IP address (`ssid`, `sta_ip`) before you share it.

Add the app name from `/shelly` to `BATTERY_APPS` in `src/source/http.rs`, otherwise a device
discovered while awake stays known as `up 0` forever. New components go into `merge_component` in
`src/source/mqtt.rs`.

Battery devices need a real recording. They behave differently from what the API documentation
suggests: a clean disconnect leaves a retained `online true` in the broker forever, and a battery
read error arrives as `"V":"Q"`.

## Add an output format

Formats are independent of devices. `src/model.rs` defines the neutral [`Reading`](src/model.rs):
a metric, labels and a value.

1. Add `src/output/<format>.rs` and implement the `Format` trait: a content type and
   `render(&[Reading]) -> String`. Use `src/output/prometheus.rs` as the example.
2. Rendering must stay a pure function. It reads from memory and never triggers a device request.
3. Wire it up in `src/main.rs`. A format that is scraped gets a route. One that is pushed calls
   `App::snapshot` from a timer.
4. Add a test with a literal expected output.

Metric names in the model have no prefix, because prefix and naming conventions belong to the
format. Prometheus adds `shelly_`.

## Rules for all changes

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

- Tests use real device data, anonymized, so a wrong assumption about the format cannot pass.
- Fixtures contain no real MAC addresses (fake ones start with `02`), IP addresses (use
  `192.0.2.x`), Wi-Fi names or room names.
- A missing value is left out, never reported as 0.
- New dependencies need a reason in the pull request. The image has no TLS or system libraries, so
  check the default features of a new crate.
