# Local build+test image — matches the dev host's architecture (x86_64), used
# to validate the live driver (balance/Telegram/order paths) against
# production before deploying. NOT the Oracle deploy artifact — Oracle is
# aarch64, cross-compiled locally with `cross` (see plan_rust_module.md
# "Build and deploy"), never built on Oracle itself.
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
