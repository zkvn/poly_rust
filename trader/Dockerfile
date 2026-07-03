# Build image for local testing and cross-compilation reference.
# For Oracle (aarch64) deploy: use `cross build --release --target aarch64-unknown-linux-gnu`
# then rsync the binary — do not build on Oracle.
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin live --bin api_probe --bin telegram_probe

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/live /build/target/release/api_probe /build/target/release/telegram_probe ./
ENTRYPOINT ["./live"]
