# Kraken — pluggable realtime pub/sub proxy
# Multi-stage build for Erlang/OTP
#
# IMPORTANT: the builder's Alpine MUST match the runtime Alpine below, or the
# crypto NIF compiles against one OpenSSL and fails to relocate against the
# other at boot ("EVP_MD_CTX_get_size_ex: symbol not found" → kernel crash).
# `erlang:26-alpine` is unpinned and drifts (it moved 3.19→3.20, which is what
# broke this), and Erlang publishes no per-Alpine tags for OTP 26, so the
# builder is pinned by DIGEST. This digest = erlang:26-alpine on Alpine 3.20.10
# (libcrypto 3.3.x); the runtime is alpine:3.20 to match. Bump them together.

FROM erlang@sha256:457edc1accf34eeac729b1ebb52402e80da830bc6d8da64367a834f0c8f3a6b6 AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY rebar.config rebar.lock* ./
RUN rebar3 compile

COPY config/ config/
COPY src/ src/

RUN rebar3 as prod release

# Runtime stage — Alpine version MUST match the builder's (see note above)
FROM alpine:3.20

RUN apk add --no-cache \
    libstdc++ \
    ncurses-libs \
    openssl \
    curl \
    bash

WORKDIR /app

COPY --from=builder /app/_build/prod/rel/kraken ./
COPY examples/ ./examples/

ENV HOME=/app
ENV RELX_REPLACE_OS_VARS=true

# ---- Defaults: zero-config single node ----
ENV WS_PORT=8080
ENV MQTT_PORT=1883

ENV AUTH_BACKEND=static
ENV BROKER_BACKEND=syn
ENV STORE_BACKEND=ets
ENV CONTROL_BACKEND=noop

ENV AUTH_FILE=/app/examples/auth.json
ENV AUTH_ALLOW_ALL=false
ENV AUTH_HTTP_URL=""
ENV CONTROL_HTTP_URL=""
ENV BACKEND_SECRET=""

ENV MQTT_BROKER_HOST=""
ENV MQTT_BROKER_PORT=1884
ENV MQTT_BROKER_USERNAME=""
ENV MQTT_BROKER_PASSWORD=""

ENV STORE_TTL_SECONDS=3600
ENV STORE_MAX_MESSAGES=10000
ENV RECORD_MESSAGES=true
ENV MAX_MESSAGE_SIZE=921600

ENV INTERNAL_SECRET=change_me

ENV CLUSTER_STRATEGY=standalone
ENV CLUSTER_DNS_NAME=""
ENV CLUSTER_HOSTS=""
ENV CLUSTER_GOSSIP_PORT=45892
ENV CLUSTER_GOSSIP_SECRET=""

ENV ERLANG_NODE_NAME=kraken@127.0.0.1
ENV ERLANG_COOKIE=kraken_dev_cookie

EXPOSE 8080 1883

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["bin/kraken", "foreground"]
