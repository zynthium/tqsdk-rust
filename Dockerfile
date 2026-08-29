# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.89
ARG NODE_VERSION=24
ARG PNPM_VERSION=10

FROM node:${NODE_VERSION}-bookworm-slim AS dashboard
ARG PNPM_VERSION
WORKDIR /dashboard

RUN npm install --global "pnpm@${PNPM_VERSION}"

COPY crates/tqsdk-relay/dashboard-ui/package.json crates/tqsdk-relay/dashboard-ui/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY crates/tqsdk-relay/dashboard-ui/ ./
RUN pnpm run build && pnpm run size-check

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /workspace

COPY . .
COPY --from=dashboard /dashboard/dist/ crates/tqsdk-relay/dashboard-ui/dist/

RUN --mount=type=cache,id=tqsdk-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=tqsdk-cargo-target,target=/workspace/target \
    if [ ! -f Cargo.lock ]; then cargo generate-lockfile; fi \
    && cargo build --locked --release -p tqsdk-relay -p tqsdk-cache \
    && install -D -m 0755 target/release/tqsdk-relay /out/tqsdk-relay \
    && install -D -m 0755 target/release/tqsdk-cache /out/tqsdk-cache

FROM debian:bookworm-slim AS runtime-base

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 tqsdk \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin tqsdk

WORKDIR /app
ENV TZ=Asia/Shanghai
USER 10001:10001

FROM runtime-base AS cache
COPY --from=builder /out/tqsdk-cache /usr/local/bin/tqsdk-cache
ENTRYPOINT ["/usr/local/bin/tqsdk-cache"]
CMD ["--help"]

FROM runtime-base AS relay
COPY --from=builder /out/tqsdk-relay /usr/local/bin/tqsdk-relay
EXPOSE 7788 7789 7790
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/tqsdk-relay"]
