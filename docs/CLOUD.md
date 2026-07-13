# Hosted auth, self-hosted data

Run kraken on your own infrastructure but let NoLag cloud be the control plane.
NoLag handles coordination and security (token validation, topic ACLs, rooms,
presence, quota). The message broker and the history store stay on your box, so
**NoLag never receives a message payload**.

This is the middle ground between the standalone quickstart (a static
`auth.json` file, no cloud) and the fully hosted NoLag cloud (NoLag runs
everything).

## One-command setup

Mint a link key in the NoLag portal under "Connect a self-hosted kraken", then:

```bash
NOLAG_LINK_KEY=nlg_link_xxxx.<secret> docker compose -f docker-compose.cloud.yml up
```

That is the whole setup. Your kraken is now on `ws://localhost:8080/ws`, and the
official NoLag SDKs connect to it with tokens you manage in the portal.

Point at a non-production environment with `NOLAG_CLOUD_URL`:

```bash
NOLAG_LINK_KEY=... NOLAG_CLOUD_URL=https://api.dev.nolag.app/v1/kraken-link \
  docker compose -f docker-compose.cloud.yml up
```

## What the preset sets

`docker-compose.cloud.yml` maps to these variables (see [CONFIG.md](./CONFIG.md)):

| Variable | Value | Why |
|----------|-------|-----|
| `AUTH_BACKEND` | `http` | token validation + ACLs from NoLag cloud |
| `CONTROL_BACKEND` | `http` | quota / usage reporting to NoLag cloud |
| `AUTH_HTTP_URL` / `CONTROL_HTTP_URL` | `https://api.nolag.app/v1/kraken-link` | the cloud control-plane base URL |
| `BACKEND_SECRET` | your `NOLAG_LINK_KEY` | authenticates this instance to your org |
| `BROKER_BACKEND` | `syn` | fan-out stays local (or set `mqtt` for your own broker) |
| `STORE_BACKEND` | `ets` | history/replay stays local (or `noop` to disable) |

## What NoLag sees, and what it does not

NoLag cloud receives only control-plane calls, keyed by your link key:

- token validation and revalidation (is this actor allowed, what are its ACLs)
- room-access checks for rooms created after an actor connected
- message counts for usage stats (a number, not content)
- subscription changes (for reconnection replay) and webhook delivery failures

NoLag cloud never receives:

- **message payloads** (they fan out through your local broker)
- **message history** (it lives in your local store; portal message-log
  dashboards are a cloud-only feature and stay empty for self-hosted instances)

The link key is scoped to your NoLag organisation. Revoke it in the portal at any
time; in-flight connections revalidate and drop within the auth cache window.

## Bring your own broker too

The broker slot is independent. Keep the built-in `syn` fan-out, or point kraken
at an MQTT broker you already run (EMQX, Mosquitto) with
`BROKER_BACKEND=mqtt` and the `MQTT_BROKER_*` vars from [CONFIG.md](./CONFIG.md).
Either way, NoLag stays the control plane and the data stays yours.
