# Contributing to Kraken

Thanks for your interest! Kraken is early — expect rough edges.

## Development

No local Erlang needed; everything runs through Docker:

```bash
# compile + unit tests
docker run --rm -v "$PWD:/app" -w /app erlang:26-alpine rebar3 eunit

# run a node and the protocol e2e suite (needs node.js + the @nolag/js-sdk repo as a sibling)
docker compose up -d --build
node e2e/run.mjs
```

## What makes a good contribution

- **Backend implementations** are the sweet spot: a `kraken_store` for
  Postgres/Redis, a `kraken_broker` for NATS/Redis pub/sub, a `kraken_auth`
  for JWT/JWKS. See `docs/PLUGINS.md` for the contracts; built-ins under
  `src/backends/` are reference implementations.
- Bug fixes with a failing test first.
- Keep the wire protocol (`docs/PROTOCOL.md`) backward compatible — official
  NoLag SDKs must keep working unchanged.

## Style

Match the existing code: OTP conventions, `kraken_` module prefix,
behaviour-first design, no synchronous calls on the message hot path.
