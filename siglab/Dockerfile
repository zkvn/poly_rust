# Build image for local testing and Oracle cross-compile reference — same pattern as
# ../trader/Dockerfile. siglab path-depends on ../trader (source only, read-only at build
# time), so this Dockerfile's build context must be the poly_rust repo root, not siglab/
# itself — see siglab/docker-compose.yml's `context: ..`. For Oracle (aarch64) deploy:
# `cross build --release --target aarch64-unknown-linux-gnu` then rsync the binary — do
# not build on Oracle.
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY siglab/Cargo.toml siglab/Cargo.lock ./
COPY siglab/src ./src
COPY trader /trader
RUN cargo build --release --bin siglab

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/siglab ./
ENTRYPOINT ["./siglab"]
