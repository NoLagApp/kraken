# Kraken

**A pluggable realtime pub/sub proxy.** Kraken is the simplification layer for
realtime messaging: actor-token auth, topic ACLs, rooms, presence, lobbies,
per-message QoS with acks, webhooks and a fixed 50 messages/s per-connection
limit, over a compact MessagePack WebSocket protocol (plus an MQTT ingress
listener for devices).

Everything behind that layer is a **plugin**:

| Slot | Default | Built-ins | Swap in |
|------|---------|-----------|---------|
| **Auth** | `static` (token file) | `static`, `http` | your control plane / IdP |
| **Broker** (fan-out) | `syn` (built-in, zero deps) | `syn`, `mqtt` | EMQX, Mosquitto, VerneMQ, ... |
| **Store** (message recording) | `ets` (in-memory) | `ets`, `noop` | your database |
| **Control** (billing/quota) | `noop` | `noop`, `http` | your billing system |

Kraken only writes to the Store: it records messages, deliveries and
connection events, and never reads them back. There is no history API, and
nothing is replayed from the Store when a client reconnects.

## Quickstart

```bash
docker compose up
```

That's it: a complete realtime backend on `ws://localhost:8080/ws`, with no
external broker and no database. The first run builds kraken from source.
Tokens come from `examples/auth.json`.

```js
import { NoLag } from "@nolag/js-sdk";

const client = NoLag("dev-token-alice", { url: "ws://localhost:8080/ws" });
await client.connect();

client.subscribe("demo/general/messages");
client.on("demo/general/messages", (data) => console.log(data));
client.emit("demo/general/messages", { text: "hello" });
```

## The built-in syn broker (and when to outgrow it)

The default broker fans out messages via Erlang process groups. It runs as a
single container for the quickstart, or as a small cluster via Erlang
distribution (see `docker-compose.cluster.yml` for a 3-node example). It
supports retained messages, shared subscriptions (load balancing) and
wildcards.

It is a **starter broker by design**: single region, modest scale. For large
or multi-region deployments, point the broker slot at a real MQTT broker:

```bash
BROKER_BACKEND=mqtt MQTT_BROKER_HOST=my-emqx MQTT_BROKER_PORT=1883 docker compose up
```

(`docker-compose.mqtt.yml` runs this against Mosquitto.) Same wire protocol,
same SDKs; only the fan-out changes.

## Bring your own auth

The static token file is for trying kraken out. For anything real, set
`AUTH_BACKEND=http` and kraken delegates every authorization decision to a
service you run. Each call is a JSON `POST` with
`Authorization: Bearer <BACKEND_SECRET>`:

| Endpoint | Body | Called |
|----------|------|--------|
| `{AUTH_HTTP_URL}/validate` | `{"accessToken": "..."}` | on connect; the answer is cached for 30 s |
| `{AUTH_HTTP_URL}/revalidate` | `{"actorTokenId": "..."}` | about every 10 minutes for each live WebSocket connection |
| `{AUTH_HTTP_URL}/check-room-access` | `{"actorTokenId": "...", "pattern": "..."}` | when an actor subscribes to a topic it was not granted at connect |

`/validate` answers `{"result": "allow", "client_attrs": {...}}` or
`{"result": "deny"}`. `/revalidate` answers `{"valid": true, ...}` with the
same attributes, or `{"valid": false, "disconnect_reason": "..."}` to close
the connection. `/check-room-access` answers
`{"allow": true, "allowed_topics": [...]}` or `{"allow": false}`; a denial or
an error refuses the subscribe. The attribute shape (identity, apps, topic
ACLs, connection limit) is documented in [docs/PLUGINS.md](docs/PLUGINS.md),
and `src/backends/kraken_auth_http.erl` is the reference client.

For a ready-made implementation, [@nolag/core](https://github.com/NoLagApp/nolag-core)
is an Apache-2.0 library that answers these calls from PostgreSQL (projects,
apps, rooms, actors and tokens). Its repository includes an example host and a
compose stack that runs it in front of kraken.

## Features

- **Wire protocol**: MessagePack over WebSocket; see [docs/PROTOCOL.md](docs/PROTOCOL.md)
- **MQTT ingress**: devices can connect over MQTT 3.1.1 (port 1883)
- **Auth**: token validation via static file or HTTP callback, with a 30s
  cache and periodic revalidation; per-token topic ACLs and a per-organization
  limit on WebSocket connections
- **Limits**: a fixed 50 messages/s per connection and a 921,600-byte payload
  ceiling on WebSocket publishes; see [docs/CONFIG.md](docs/CONFIG.md#fixed-limits)
- **Rooms + presence**: room-scoped presence with join/leave/update events,
  lobby aggregation across rooms
- **QoS + acks**: per-message QoS 0/1/2, with delivery records and acks written
  to the Store while recording is on
- **Echo control, per-subscription filters, load-balanced subscriptions**
- **Webhooks**: hydration + trigger webhooks per topic
- **Clustering**: dns / epmd / gossip discovery (Erlang distribution)
- **Embeddable**: use kraken as a rebar3 dependency and provide your own
  backend modules; see [docs/PLUGINS.md](docs/PLUGINS.md)

## Not included

- **Per-token rate or size limits.** Every connection gets the fixed limits
  above. The `rateLimit` field in `auth.json` and the `MAX_MESSAGE_SIZE`
  variable have no effect.
- **A message history or replay API.** The Store only records.
- **Offline queueing on the default syn broker.** A subscriber that is offline
  when a message is published never receives it. With the `mqtt` backend,
  actors your auth backend marks `persistent_session` get a persistent MQTT 5
  session on the external broker, and queueing is up to that broker.
- **Persistent presence.** Presence belongs to the socket and ends when it
  closes.
- **A published Docker image.** Build from source with `docker compose up`
  (or `docker compose build`).

## Configuration

All via environment variables; see [docs/CONFIG.md](docs/CONFIG.md).

## Tests

```bash
docker run --rm -v "$PWD:/app" -w /app erlang:26-alpine rebar3 eunit
docker compose up -d && node e2e/run.mjs           # protocol e2e (uses @nolag/js-sdk)
docker compose -f docker-compose.cluster.yml up -d && node e2e/cluster.mjs
```

## Status and contributing

NoLag's hosted service was retired in October 2026. Kraken continues as an
open-source project, and contributions are welcome: see
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0. Built by [NoLag](https://nolag.app).
