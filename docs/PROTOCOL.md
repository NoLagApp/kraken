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

Devices can connect over MQTT 3.1.1 (default port 1883): CONNECT
username/password = anything/`<access token>` flows through the same auth
backend, topics map to ACL patterns via the same unified resolution, QoS 0-2
supported. Caveats: MQTT 3.1.1 cannot NACK a publish — denied publishes are
dropped with a broker-side log (and acked at QoS>0 to prevent retry storms);
subscribe failures surface as SUBACK failure return codes.
