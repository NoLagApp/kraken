// Kraken e2e: drives a running kraken node with the official @nolag/js-sdk.
// Proves wire-protocol compatibility: auth, pub/sub, echo suppression,
// presence, rate limiting, oversize rejection.
//
// Usage: node e2e/run.mjs [ws://localhost:18080/ws]
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const { NoLag } = require("../../js-sdk/dist/index.cjs");

const URL = process.argv[2] || "ws://localhost:18080/ws";
const TOPIC = "demo/general/messages";

let passed = 0, failed = 0;
const ok = (name) => { passed++; console.log(`  PASS  ${name}`); };
const fail = (name, why) => { failed++; console.log(`  FAIL  ${name} — ${why}`); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const waitFor = (cond, ms = 4000, step = 50) => new Promise(async (resolve) => {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (cond()) return resolve(true);
    await sleep(step);
  }
  resolve(false);
});

function client(token) {
  return NoLag(token, { url: URL, reconnect: false, heartbeatInterval: 0, debug: false });
}

console.log(`kraken e2e against ${URL}\n`);

// ---------- 1. auth ----------
const alice = client("dev-token-alice");
try {
  await alice.connect();
  ok("auth: valid token connects");
} catch (e) {
  fail("auth: valid token connects", e.message);
  process.exit(1);
}

const badClient = client("not-a-real-token");
try {
  await badClient.connect();
  fail("auth: invalid token rejected", "connect resolved");
} catch {
  ok("auth: invalid token rejected");
}

// ---------- 2. pub/sub ----------
const bob = client("dev-token-bob");
await bob.connect();

const bobGot = [];
bob.subscribe(TOPIC);
bob.on(TOPIC, (data) => bobGot.push(data));
await sleep(300);

alice.subscribe(TOPIC);
const aliceGot = [];
alice.on(TOPIC, (data) => aliceGot.push(data));
await sleep(300);

alice.emit(TOPIC, { text: "hello from alice", n: 1 });
(await waitFor(() => bobGot.length >= 1))
  ? ok("pub/sub: bob receives alice's message")
  : fail("pub/sub: bob receives alice's message", "timeout");
if (bobGot[0]?.text === "hello from alice") ok("pub/sub: payload intact");
else fail("pub/sub: payload intact", JSON.stringify(bobGot[0]));

// echo=true default: alice hears herself
(await waitFor(() => aliceGot.length >= 1))
  ? ok("echo: default echo=true delivers to sender")
  : fail("echo: default echo=true delivers to sender", "timeout");

// ---------- 3. echo suppression ----------
const aliceBefore = aliceGot.length;
const bobBefore = bobGot.length;
alice.emit(TOPIC, { text: "no echo", n: 2 }, { echo: false });
await waitFor(() => bobGot.length > bobBefore);
await sleep(400);
if (bobGot.length > bobBefore) ok("echo=false: receiver still gets message");
else fail("echo=false: receiver still gets message", "bob missed it");
if (aliceGot.length === aliceBefore) ok("echo=false: sender does NOT hear own message");
else fail("echo=false: sender does NOT hear own message", "alice echoed");

// ---------- 4. presence (room-scoped via fluent context) ----------
let joinSeen = false;
alice.on("presence:join", () => { joinSeen = true; });
const aliceRoom = alice.setApp("demo").setRoom("general");
const bobRoom = bob.setApp("demo").setRoom("general");
aliceRoom.setPresence({ status: "online", name: "alice" });
await sleep(300);
bobRoom.setPresence({ status: "online", name: "bob" });
(await waitFor(() => joinSeen))
  ? ok("presence: join event broadcast")
  : fail("presence: join event broadcast", "no presence:join at alice");

const list = await bobRoom.fetchPresence().catch(() => []);
(Array.isArray(list) && list.length >= 1)
  ? ok(`presence: fetchPresence returns members (${list.length})`)
  : fail("presence: fetchPresence returns members", JSON.stringify(list));

// ---------- 5. oversize message -> message_too_large ----------
let sizeErr = null;
alice.on("error", (e) => {
  if (/too_large/.test(e?.message ?? "")) sizeErr = e;
});
alice.emit(TOPIC, { blob: "x".repeat(1000_000) });
(await waitFor(() => sizeErr !== null))
  ? ok("limits: >900KB rejected with message_too_large")
  : fail("limits: >900KB rejected with message_too_large", "no error event");

// ---------- 6. rate limit -> rate_limit_exceeded ----------
let rateErr = null;
alice.on("error", (e) => {
  if (/rate_limit/.test(e?.message ?? "")) rateErr = e;
});
for (let i = 0; i < 80; i++) alice.emit(TOPIC, { i });
(await waitFor(() => rateErr !== null))
  ? ok("limits: >50 msg/s rejected with rate_limit_exceeded")
  : fail("limits: >50 msg/s rejected with rate_limit_exceeded", "no error event");

// ---------- done ----------
alice.disconnect();
bob.disconnect();
await sleep(200);
console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
