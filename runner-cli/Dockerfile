# syntax=docker/dockerfile:1
# Build from the repo root: docker build -f runner-cli/Dockerfile .

FROM rust:1.98-trixie AS build
# cmake + clang: librdkafka (rdkafka-sys) and aws-lc-sys are built from source.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Cache mounts keep the registry and compiled dependencies between builds;
# the binary is copied out because the target dir is not part of the layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=serverless-platform-target,sharing=locked \
    cargo build --release --locked -p runner-cli \
    && cp target/release/runner-cli /runner-cli

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home runner \
    && install -d -o runner /var/cache/runner-cli
COPY --from=build /runner-cli /usr/local/bin/runner-cli
USER 10001
ENV RUST_LOG=info \
    CACHE_DIR=/var/cache/runner-cli
VOLUME /var/cache/runner-cli
ENTRYPOINT ["/usr/local/bin/runner-cli"]
