// Characterization tests for topic resolution + fallback behavior (M0 of the
// loud-failures hardening program). These pin CURRENT behavior — including the
// bugs — so the M1 fix (kraken_topics.erl unified resolution + auto-provision
// + 42940) can prove itself by flipping the marked expectations.
//
// Requires the e2e kraken with e2e/auth.e2e.json:
//   docker compose -f e2e/docker-compose.e2e.yml -p kraken-e2e up -d --build
//
// Scenarios:
//   A. wildcard-only tokens, both sides fall back identically -> DELIVERS
//      (this is how the OSS static-auth quickstart works today)
//   B. exact-mapped subscriber + wildcard-fallback publisher -> BLACKHOLE
//      (different MQTT topics: internal UUID topic vs <AppId>/<pattern>;
//       this is the split-brain bug — post-M1 this must either deliver or
//       fail loudly with 42940)
//   C. Titus-style exact-only token, unknown room -> not_authorized error
//      frame, subscribe callback STILL fires cb(null) (the SDK lie — fixed
//      in M2; post-M1b an app-authorized actor gets auto-provisioning instead)
//
// WS Pattern/EffPubPattern asymmetry and WS<->MQTT fallback divergence are
// code-level findings (kraken_ws_handler.erl:574 vs :940; kraken_mqtt_handler
// :233-236) covered by eunit in M6 — not exercised here.
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const { NoLag } = require("../../js-sdk/dist/index.cjs");

const URL = process.argv[2] || "ws://localhost:18080/ws";

let passed = 0, failed = 0;
const ok = (name) => { passed++; console.log(`  PASS  ${name}`); };
const fail = (name, why) => { failed++; console.log(`  FAIL  ${name} — ${why}`); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function client(token) {
  return NoLag(token, { url: URL, reconnect: false, heartbeatInterval: 0, debug: false });
}

console.log(`topic-resolution characterization against ${URL}\n`);

// ---------- A. both-sides wildcard fallback delivers ----------
{
  const w1 = client("tok-wild");
  const w2 = client("tok-wild");
  await w1.connect(); await w2.connect();
  const got = [];
  w2.subscribe("charz/room/events");
  w2.on("charz/room/events", (d) => got.push(d));
  await sleep(300);
  w1.emit("charz/room/events", { a: 1 });
  await sleep(800);
  if (got.length === 1) ok("A: wildcard<->wildcard fallback delivers (OSS quickstart path)");
  else fail("A: wildcard<->wildcard fallback delivers", `got ${got.length}`);
  w1.disconnect(); w2.disconnect();
}

// ---------- B. exact subscriber + wildcard publisher = blackhole ----------
{
  const exact = client("tok-exact");
  const wild = client("tok-wild");
  await exact.connect(); await wild.connect();

  const exactGot = [];
  exact.subscribe("charz/room/messages"); // resolves internal topic room-charz-uuid/messages
  exact.on("charz/room/messages", (d) => exactGot.push(d));
  await sleep(300);

  wild.emit("charz/room/messages", { b: 2 }); // falls back to charz-app/charz/room/messages
  await sleep(800);

  // CURRENT (buggy) behavior: blackhole. Post-M1: flip this expectation —
  // unified resolution must make this deliver (wildcard side resolves the
  // same internal topic via wildcard-aware lookup) or reject loudly.
  if (exactGot.length === 0) ok("B: exact-subscriber misses wildcard-publisher message (CURRENT BUG — must flip post-M1)");
  else fail("B: expected blackhole under current behavior", `delivered ${exactGot.length}`);

  // Sanity: exact<->exact delivers via the internal topic
  const exact2 = client("tok-exact2");
  await exact2.connect();
  const got2 = [];
  exact2.subscribe("charz/room/messages");
  exact2.on("charz/room/messages", (d) => got2.push(d));
  await sleep(300);
  exact.emit("charz/room/messages", { c: 3 });
  await sleep(800);
  if (got2.length === 1) ok("B2: exact<->exact delivers via internal topic");
  else fail("B2: exact<->exact delivers via internal topic", `got ${got2.length}`);

  exact.disconnect(); wild.disconnect(); exact2.disconnect();
}

// ---------- C. unknown room on exact-only token: loud broker, lying SDK ----------
{
  const exact = client("tok-exact");
  const errors = [];
  exact.on("error", (e) => errors.push(e.message || String(e)));
  await exact.connect();

  let subCb = null;
  exact.subscribe("charz/other-room/messages", (err) => { subCb = err ? `err:${err.message}` : "cb(null)"; });
  await sleep(800);

  if (errors.some((m) => m.includes("not_authorized"))) {
    ok("C: unknown room rejected with not_authorized error frame (broker is loud)");
  } else {
    fail("C: unknown room rejected with not_authorized", `errors=[${errors}]`);
  }
  // CURRENT (lying) SDK behavior: callback reports success despite rejection.
  // Post-M2: flip — callback must receive the error.
  if (subCb === "cb(null)") ok("C2: subscribe callback lies with cb(null) despite rejection (CURRENT BUG — must flip post-M2)");
  else fail("C2: expected lying cb(null) under current behavior", String(subCb));

  exact.disconnect();
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
