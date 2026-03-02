FROM rust:1.85-slim AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/wesense-iroh-sidecar /usr/local/bin/

ENV IROH_DATA_DIR=/data
ENV IROH_SIDECAR_PORT=4002
EXPOSE 4002

VOLUME /data

ENTRYPOINT ["wesense-iroh-sidecar"]
