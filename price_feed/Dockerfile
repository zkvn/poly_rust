FROM rust:1-bookworm AS builder
WORKDIR /build
# native-tls for tokio-tungstenite (Binance WS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin price_feed

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/price_feed ./
ENTRYPOINT ["./price_feed"]
