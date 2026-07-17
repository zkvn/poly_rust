# Build image for local perf testing (docker-compose.perf.yml). For Oracle
# (aarch64) deploy: `cross build --release --target aarch64-unknown-linux-gnu`
# then rsync the binary — same posture as trader/Dockerfile.
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin indicator

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/indicator ./
COPY config /app/config
ENTRYPOINT ["./indicator"]
CMD ["--config", "/app/config/indicator.toml", "run"]
