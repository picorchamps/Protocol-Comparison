# ── Stage 1: build ─────────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

WORKDIR /app
COPY . .

# prost-build uses the vendored protoc binary, but it needs to be executable.
# No system protoc required.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp /app/target/release/pico-launcher /app/pico-launcher

# ── Stage 2: minimal runtime ───────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/pico-launcher /usr/local/bin/pico-launcher

ENTRYPOINT ["pico-launcher"]
