# Rust version = rust-toolchain.toml, runtime trixie-slim, fixed non-root user. No TLS libraries:
# plain MQTT on the LAN.
FROM rust:1.99.1-slim-trixie AS builder

WORKDIR /app

# Dependencies first (cache layer)
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:trixie-slim AS runtime

# The base image lags behind trixie-security: upgrade at build time.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app

COPY --from=builder /app/target/release/shelly-exporter /usr/local/bin/shelly-exporter

USER 10001:10001
EXPOSE 9926
CMD ["/usr/local/bin/shelly-exporter"]
