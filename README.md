# Kraken

**A pluggable realtime pub/sub proxy.** Kraken is the simplification layer for
realtime messaging: actor-token auth, topic ACLs, rooms, presence, lobbies,
per-message QoS with acks, replay-on-reconnect, rate limiting and webhooks —
over a compact MessagePack WebSocket protocol (plus an MQTT ingress listener
for devices).

Everything behind that layer is a **plugin**:

| Slot | Default | Built-ins | Swap in |
|------|---------|-----------|---------|
| **Auth** | `static` (token file) | `static`, `http` | your control plane / IdP |
| **Broker** (fan-out) | `syn` (built-in, zero deps) | `syn`, `mqtt` | EMQX, Mosquitto, VerneMQ, ... |
| **Store** (history/replay) | `ets` (in-memory) | `ets`, `noop` | your database |
| **Control** (billing/quota) | `noop` | `noop`, `http` | your billing system |

## Quickstart

```bash
docker compose up
```

That's it: a complete realtime backend on `ws://localhost:8080/ws` — no
external broker, no database. Tokens come from `examples/auth.json`.

```js
import { NoLag } from "@nolag/js-sdk";

const client = NoLag("dev-token-alice", { url: "ws://localhost:8080/ws" });
await client.connect();

client.subscribe("demo/general/messages");
client.on("demo/general/messages", (data) => console.log(data));
client.emit("demo/general/messages", { text: "hello" });
```

## The built-in syn broker (and when to outgrow it)

The default broker fans out messages via Erlang process groups —
single-container quickstart, small clusters via Erlang distribution
(see `docker-compose.cluster.yml` for a 3-node example). It supports
retained messages, shared subscriptions (load balancing) and wildcards.

It is a **starter broker by design**: single region, modest scale. For large
or multi-region deployments, point the broker slot at a real MQTT broker:

```bash
BROKER_BACKEND=mqtt MQTT_BROKER_HOST=my-emqx MQTT_BROKER_PORT=1883 docker compose up
```

(`docker-compose.mqtt.yml` runs this against Mosquitto.) Same wire protocol,
same SDKs — only the fan-out changes.

## Hosted auth, self-hosted data

Run kraken yourself but let NoLag cloud be the control plane: token validation,
ACLs, rooms, presence and quota come from NoLag, while the broker and store stay
on your box, so **NoLag never sees a message payload**. Mint a link key in the
NoLag portal and:

```bash
NOLAG_LINK_KEY=nlg_link_xxxx.<secret> docker compose -f docker-compose.cloud.yml up
```

The middle ground between the `auth.json` quickstart and the fully hosted cloud.
See [docs/CLOUD.md](docs/CLOUD.md) for exactly what NoLag does and does not
receive.

## Features

- **Wire protocol**: MessagePack over WebSocket; see [docs/PROTOCOL.md](docs/PROTOCOL.md)
- **MQTT ingress**: devices can connect over MQTT 3.1.1 (port 1883)
- **Auth**: token validation via static file or HTTP callback, with a 30s
  cache and periodic revalidation; per-token topic ACLs, rate limits,
  message-size limits, connection limits
- **Rooms + presence**: room-scoped presence with join/leave/update events,
  lobby aggregation across rooms
- **QoS + replay**: per-message QoS 0/1/2, delivery tracking, acks, and
  replay of unacknowledged messages on reconnect (store-backed)
- **Echo control, per-subscription filters, load-balanced subscriptions**
- **Webhooks**: hydration + trigger webhooks per topic
- **Clustering**: dns / epmd / gossip discovery (Erlang distribution)
- **Embeddable**: use kraken as a rebar3 dependency and provide your own
  backend modules — see [docs/PLUGINS.md](docs/PLUGINS.md)

## Configuration

All via environment variables; see [docs/CONFIG.md](docs/CONFIG.md).

## Tests

```bash
docker run --rm -v "$PWD:/app" -w /app erlang:26-alpine rebar3 eunit
docker compose up -d && node e2e/run.mjs           # protocol e2e (uses @nolag/js-sdk)
docker compose -f docker-compose.cluster.yml up -d && node e2e/cluster.mjs
```

## License

Apache-2.0. Built by [NoLag](https://nolag.app) — kraken is the open-source
core of the NoLag realtime cloud.
