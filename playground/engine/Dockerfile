# The stream engine (mage-service) with its standard connectors, for running streams from
# the playground's editor (make playground-streams). Built from this repository on the Debian
# of the Mage image (trixie); librdkafka and OpenSSL link statically. The engine service
# copies the binary into a volume the mage service reads it from (MAGE_ENGINE_PATH), so the
# playground without streams builds no Rust. Cargo's registry and target directory live in
# BuildKit cache mounts: a rebuild after an engine change compiles only what changed.
FROM rust:1.98.0-slim-trixie AS build
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make perl \
      pkg-config libcurl4-openssl-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY mage_ai/pipeline_services/service ./service
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,id=playground-engine-$TARGETARCH,target=/build/service/target \
    cargo build --release --locked --manifest-path service/Cargo.toml -p mage-service \
      --features full && \
    cp service/target/release/mage-service /mage-service

FROM debian:trixie-slim
COPY --from=build /mage-service /opt/engine/mage-service
CMD ["sh", "-c", "cp /opt/engine/mage-service /engine/mage-service && /engine/mage-service --version"]
