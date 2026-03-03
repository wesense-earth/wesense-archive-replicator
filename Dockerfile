FROM rust:slim AS chef

RUN cargo install cargo-chef && \
    apt-get update && apt-get install -y --no-install-recommends \
    cmake gcc g++ make pkg-config perl libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Stage 1: Analyze dependencies (produces recipe.json)
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies only (cached unless Cargo.toml/lock change)
FROM chef AS deps
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build application (only this layer rebuilds on src/ changes)
FROM deps AS builder
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

# Stage 4: Runtime
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/wesense-iroh-sidecar /usr/local/bin/

ENV IROH_DATA_DIR=/data
ENV IROH_SIDECAR_PORT=4400
EXPOSE 4400

VOLUME /data

ENTRYPOINT ["wesense-iroh-sidecar"]
