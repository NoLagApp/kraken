# Kraken Wire Protocol

WebSocket at `/ws`, **binary frames containing MessagePack** (packed with
`str` from binary / unpacked `str` as binary). The protocol is identical to
the NoLag cloud protocol — official NoLag SDKs work against kraken unchanged.

Heartbeat: an **empty binary frame** in either direction; the server echoes
when authenticated.

## Protocol versions

The protocol is versioned via the auth handshake. **Version 1** (the default
when the client sends no `protocolVersion`) preserves legacy semantics
bit-for-bit. **Version 2** adds loud failures and publish acks:

| Behavior | v1 | v2 |
|---|---|---|
| Unknown room that cannot be auto-provisioned | `not_authorized` error | `42940 unknown_topic` error with `hint` |
| Publish acks | none | `published` frame when the client sends `msgRef` |
| Auto-provisioned rooms | yes (version-independent) | yes |

The server replies with `min(clientVersion, serverVersion)`; clients must
treat an absent `protocolVersion` in the auth response as 1.

## Authentication (first message)

```jsonc
// client -> server
{ "type": "auth", "token": "<access token>", "reconnect": false,
  "protocolVersion": 2 }  // optional; absent = 1

// success
{ "type": "auth", "success": true, "actorTokenId": "...", "projectId": "...",
  "actorType": "...", "protocolVersion": 2,
  "restoredSubscriptions": ["pattern", ...] }

// failure
{ "type": "auth", "success": false, "error": "access_denied" | "connection_limit_reached" | "broker_unavailable" }
```

### Reconnect restore

With `"reconnect": true`, kraken restores the connection's earlier
subscriptions before replying, and lists them in `restoredSubscriptions`.
The source is the auth backend's `active_subscriptions` when it returns any
(a control plane that persists the subscription reports). Otherwise kraken
uses its own memory of the subscribe requests made under the same key: the
actor token plus the optional `clientId` from the auth message. Those
requests are replayed through the normal subscribe path, so current ACLs,
scopes, filters (including `setFilters` changes) and load balancing apply;
a topic the actor may no longer subscribe to is dropped.

- Without a `clientId`, all connections of one actor share a key, so a
  reconnect restores the union of their subscriptions. Send a `clientId` per
  client instance to keep them apart.
- A connect without `"reconnect": true` starts empty and drops what an
  earlier, now-closed connection of the same key left behind.
- The memory is held in RAM, cluster-wide: a reconnect that lands on another
  node still finds it. It is kept while any connection holds the key and for
  `resume_retention_ms` (default 3600000) after the last one closes. It does
  not survive a restart of the node that held it: after a kraken restart,
  clients must subscribe again (subscribing in the SDK's `connect` handler
  covers both cases, and a repeated subscribe is harmless).

## Client → server messages

| type | fields |
|------|--------|
| `publish` | `topic`, `data`, `echo` (default true), `qos` (0-2, default 1), `retain` (default false), `filter` / `filters`, `msgRef` (v2, optional — requests a `published` ack) |
| `subscribe` | `topic`, `qos`, `loadBalance`, `loadBalanceGroup`, `filters` |
| `unsubscribe` | `topic` |
| `setFilters` | `topic`, `filters` |
| `presence` | `roomId` (slug), `data` |
| `getPresence` | `roomId` |
| `lobbySubscribe` / `lobbyUnsubscribe` / `getLobbyPresence` | `lobbyId` (slug) |
| `ack` / `batchAck` | `msgId` / `msgIds` |

Responses: `subscribed`, `unsubscribed`, `filtersUpdated`,
`presenceList` (`roomId`, `data`), `lobbyPresenceList`,
`published` (v2: `topic`, `msgRef`).

**Subscribe acks**: the broker always answers a subscribe with either
`{ "type": "subscribed", "topic": ... }` or a topic-tagged error frame —
clients should treat the response (not the act of sending) as confirmation.

**Publish acks (v2)**: when a publish carries a client-generated `msgRef`,
success is acknowledged with `{ "type": "published", "topic": ..., "msgRef": ... }`
and every publish-path error frame echoes the `msgRef`. Publishes without
`msgRef` are never acked (zero overhead for v1 clients and fire-and-forget).

## Server → client messages

```jsonc
// topic message
{ "type": "message", "topic": "...", "data": ..., "msgId": "...",
  "requiresAck": true, "filter": "...", "isReplay": true }

// publish ack (v2, only when the publish carried msgRef)
{ "type": "published", "topic": "...", "msgRef": "..." }

// presence events
{ "type": "presence", "event": "join"|"leave"|"update",
  "data": { "actor_token_id": "...", "presence": {...} } }
{ "type": "lobbyPresence", "event": "...", "lobbyId": "...", "roomId": "...",
  "actorId": "...", "data": {...} }

// hydration (webhook-sourced data on subscribe)
{ "type": "hydration", "topic": "...", "data": {...} }

// replay framing
{ "type": "replayStart", "count": N, "oldestTimestamp": T, "newestTimestamp": T }
{ "type": "replayEnd", "count": N }

// errors
{ "type": "error", "code": C, "error": "...", "topic": "...", "hint": "...", "msgRef": "..." }
```

## Error codes

| Code | Error |
|------|-------|
| 42910 | `rate_limit_exceeded` (per-connection msg/s limit) |
| 42920 | `monthly_quota_exceeded` (control-plane block) |
| 42930 | `message_too_large` (includes `maxSizeBytes`; payload measured as packed msgpack) |
| 42940 | `unknown_topic` (v2 only; the room is not configured and could not be auto-provisioned — `hint` explains why). v1 clients receive `not_authorized` for the same condition |

## Topic resolution

A client pattern (`app/room/topic`, or `app/scope/room/topic` for scoped
actors) resolves to an internal MQTT topic via the connection's
`allowed_topics` rules, in this order:

1. **Exact rule with an internal mapping** — the normal case for control-
   plane (Titus) tokens: every existing room is enumerated as an exact
   pattern mapped to a `room-uuid/topic` internal topic. Exact rules always
   win over wildcard rules.
2. **Wildcard rule** — patterns matched only by a `+`/`#` rule fall back to
   the deterministic app-scoped topic `<app_id>/<effective pattern>`. This
   is the static-auth/OSS path: with wildcard rules on both sides, both
   resolve identically and traffic flows. **Constraint:** rulesets must be
   homogeneous per app — an exact-mapped actor and a wildcard-only actor
   resolve different topics and will not interop (both sides log their
   resolution; mixed configs are an operator error).
3. **No match** — `not_authorized` (v1) / `42940 unknown_topic` (v2). The
   broker **never creates a room implicitly** on the data path.

### Dynamic rooms are provisioned explicitly (never on the data path)

The broker does not create rooms when an unknown slug is touched — that
silently hid typo'd and asymmetric slugs (publisher and subscriber computing
different ids both "succeed" into separate empty rooms, surfacing downstream
as dead realtime). An unknown room is always a **loud** error
(`42940`/`not_authorized`), delivered to the SDK's subscribe/publish callback.

Per-entity rooms (a matter id, a device id) are created by the application
intentionally, at entity-creation time, via the control-plane rooms API
(`NoLagApi.rooms.ensure(appId, { slug, ... })` → Titus
`POST .../apps/:appId/rooms/ensure`): idempotent (returns the existing room on
slug match), gated by the app's `config.autoProvisionRooms` flag (default off,
so static-topology apps reject runtime creation — a typo loop is loud even on
the creator side), and capped per app (`KRAKEN_AUTO_ROOM_CAP`, default 1000).
Everyone else just joins; only the one intentional creator code path makes a
room, so a divergent slug elsewhere is caught loudly.

### Rolling-upgrade shim

Pre-v2 brokers used inconsistent fallback names (`unknown/<pattern>` etc.).
For one release, wildcard-resolved subscriptions also listen on the legacy
fallback topic (broker env `fallback_compat`, default true) so in-flight
old-node publishers still reach upgraded subscribers. Publishing always uses
the new deterministic name. Remove the shim once all nodes are upgraded.

## Filters

Subscribing with `filters` narrows delivery to matching publishes; each
filter maps to an MQTT sub-topic of the base topic. Without filters a
subscription is a wildcard over the base topic — **but wildcard
subscriptions do not receive filtered publishes** (a filtered publish goes
only to its sub-topic). Publishes carry either a single `filter` or a
`filters` array, which is normalized into an AND-composite: lowercased,
sorted, joined with `|`. Subscribe-side AND groups (nested arrays) normalize
the same way. Filter values must not contain `/`, `#`, `+`, or `|`;
max 100 filters per topic.

## Load balancing

A subscription with `loadBalance: true` joins an EMQX shared-subscription
group (`$share/<group>/<topic>`): **each message is delivered to exactly one
member of the group**, not all of them. The group name is scoped
`<projectId>_<appId>_<clientGroup>` (underscore-separated — slashes would be
parsed as topic segments) so identical group names in different apps never
collide; the default group is the actor token id. Load balancing is
per-subscription: the same connection can hold load-balanced and broadcast
subscriptions on different topics. Re-subscribing with a different mode
switches modes (old MQTT subscriptions are replaced).

Load balancing is for **work distribution**. Topics carrying correlated
replies must not be load-balanced — a reply delivered to a random group
member is lost to the requester. See `blueprints/docs/AGENTS-PROTOCOL.md`
for the agents-layer topology built on these primitives.

## Room scoping

Actors with a scope (`scope_slug` in auth) publish/subscribe patterns with
the scope slug injected after the app segment (`app/scope/room/topic`); the
internal topic is prefixed with the scope id. ACL rules are enumerated per
scope by the control plane.

Room presence and lobby presence are partitioned the same way: actors in
different scopes (or a scoped and an unscoped actor) never see each other's
presence, even when their grants point at the same room id.

## Subscription freshness

Connection auth state (allowed_topics) is cached ~30s and revalidated
roughly every 10 minutes. Rooms created out-of-band (control-plane API,
another node's auto-provisioning) become visible to existing connections at
the next revalidation; same-node auto-provisioning is visible immediately
via the node cache. Plan accordingly for "create room, then immediately use
it from a different, already-connected client on another node".

## Echo suppression

`echo: false` publishes wrap the payload as
`{ "data": ..., "_sender": "<connectionId>" }` on the broker; the sender's
own connection drops it on delivery. With delivery tracking active, payloads
carry `{ "_msgId": "...", "_data": ... }` envelopes that the server unwraps
into `msgId`/`requiresAck` before forwarding.

## MQTT ingress

Devices can connect over MQTT 3.1.1 (default port 1883). The CONNECT
password is the access token (the username can be anything; with no
password the username is used as the token), validated by the same auth
backend as a WebSocket `auth`. An unknown token gets CONNACK return code 5
(not authorized). MQTT 5 clients are not supported: the connection is closed
without a CONNACK.

### Topics

An MQTT topic is the same pattern a WebSocket client uses (`app/room/topic`)
and resolves through the same unified resolution, so MQTT and WebSocket
clients on one pattern exchange messages. A delivery carries the topic the
client subscribed with, never the internal topic.

- A filter outside the actor's grants gets a SUBACK failure (`0x80`).
- Wildcard filters (`+`, `#`) work under wildcard grants: `demo/general/#`
  receives everything published below `demo/general/`, and each delivery
  carries the concrete topic it was published on. A topic that resolves
  through an exact rule with an internal mapping (a control-plane room) is
  delivered only to subscriptions on that exact topic, not to wildcard
  filters that would match it.
- A WebSocket publish with a `filter` goes to `<topic>/<filter>`. MQTT
  filters that match that level receive it (`demo/general/#` gets it as
  `demo/general/chat/vip`); a subscription on `<topic>` itself does not.
- UNSUBSCRIBE leaves exactly the broker topic its SUBSCRIBE joined, and
  nothing more is delivered for that filter after the UNSUBACK.

### Payloads

MQTT payloads are opaque bytes, while WebSocket `data` is any MessagePack
value. Between the two:

- **MQTT publish:** the payload bytes are published as one binary.
  WebSocket subscribers receive it as `data`: a string when the bytes are
  valid UTF-8 (a JSON payload arrives as the JSON text, not parsed), binary
  data otherwise (`Uint8Array` in the JS SDK). Messages published over MQTT
  carry no `msgId`, so they are not delivery-tracked.
- **Delivery to an MQTT subscriber:** when the message data is a string or
  binary, the PUBLISH payload is exactly those bytes. Any other value (an
  object, array or number, for example) is sent as its MessagePack
  encoding. The echo-suppression and delivery-tracking envelopes are
  removed first.
- **No echo:** an MQTT client never receives its own publishes, even on a
  topic it is subscribed to.

### QoS and retain

- PUBLISH at QoS 0, 1 and 2 is accepted: QoS 1 gets PUBACK, QoS 2 gets
  PUBREC and, after PUBREL, PUBCOMP.
- Deliveries to MQTT subscribers are QoS 0, and SUBACK grants QoS 0
  whatever QoS was requested.
- The retain flag on an MQTT PUBLISH is not applied. Retained messages
  from WebSocket publishers (`retain: true`) are delivered to an MQTT
  client when it subscribes.

### Limits

MQTT 3.1.1 cannot NACK a publish, so each of these is dropped with a
broker log line and still acknowledged at QoS 1 and 2, which keeps clients
from retrying it forever:

- a publish without a publish grant for its topic;
- a payload over 921600 bytes (the same 900KB ceiling as WebSocket, measured
  on the raw payload);
- publishes over the per-connection rate limit of 50 msg/s (one log line per
  second while the limit applies).

A packet over 1 MiB (1048576 bytes, the WebSocket frame cap) closes the
connection. So does 1.5x the CONNECT keep-alive without any packet from the
client; every packet resets that timer, not only PINGREQ.

## Persistent presence

> Requires a persistence + wake backend (proprietary; see
> `kraken-proxy/docs/PERSISTENT_PRESENCE.md`). On the OSS `syn` build with no
> backend wired, `persistent` advertises behave like ordinary ephemeral presence
> (no durable record, no wake) — the fields below are accepted and ignored.

Ordinary `presence` is socket-bound: the entry is dropped when the connection
closes. **Persistent presence** lets an actor's presence record (identity +
advertised capabilities) survive disconnection so it stays discoverable, and be
**woken on demand** when a message/task is routed to it while offline.

### Client → server (extends `presence`)

| field | meaning |
|-------|---------|
| `persistent` | `true` to write a durable presence record (default `false` = today's ephemeral behavior) |
| `capabilities` | `array<string>` — capability/tool tags, indexed for discovery (`array-contains`) |
| `advertisement` | map returned verbatim to discoverers: `{ tools, baseUrl, meta }` (no secrets) |
| `wake` | map: `{ url, timeoutMs }` — HMAC-signed wake endpoint + reconnect deadline. The wake secret is minted server-side and **never echoed back** |

A persistent `presence` write upserts the durable record (`status=online`) and
also performs the normal live `join`. On socket close the record is **soft-offlined**
(`status=offline`), not removed; a persistent TTL eventually reaps records that
never reconnect.

### Server → client

```jsonc
// presence events gain a status (persistent actors only)
{ "type": "presence", "event": "join"|"leave"|"update"|"waking",
  "data": { "actor_token_id": "...", "status": "online"|"offline"|"waking",
            "presence": {...}, "capabilities": [...], "advertisementVersion": N } }

// presenceList / discovery now includes offline-but-registered actors:
{ "type": "presenceList", "roomId": "...",
  "data": [ { "actor_token_id": "...", "status": "online"|"offline"|"waking",
              "advertisement": {...}, "advertisementVersion": N } ] }
```

Discovery (`getPresence` / the agents-layer `findAgents`) returns persistent actors
regardless of live-connection status, tagged with `status`. Routing a task to an
`offline` persistent actor transitions it to `waking`, queues the task on its
persistent session (QoS ≥ 1), and fires its wake webhook out-of-band; on reconnect
the queued task is delivered. Wake delivery is **not** part of the wire protocol —
it is an outbound server-side webhook (no client frame).

### Errors

| Code | Error |
|------|-------|
| 42950 | `wake_failed` (persistent actor was offline and the wake webhook did not produce a reconnect before `wake.timeoutMs`; the task is routed to the DLQ) |
