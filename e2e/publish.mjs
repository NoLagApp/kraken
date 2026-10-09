// Kraken e2e: POST /v1/publish, broker-stamped senders and signed webhooks.
//
// Needs the e2e stack with its control backend pointed at this script's
// receiver (port 18099), so failed webhooks' DLQ reports can be inspected:
//
//   CONTROL_BACKEND=http CONTROL_HTTP_URL=http://host.docker.internal:18099 \
//     docker compose -f e2e/docker-compose.e2e.yml -p kraken-e2e up -d --build
//   node e2e/publish.mjs [http://localhost:18080]
import { createRequire } from "module";
import http from "node:http";
const require = createRequire(import.meta.url);
const { NoLag, verifyWebhookSignature } = require("../../js-sdk/dist/index.cjs");

const BASE = process.argv[2] || "http://localhost:18080";
const WS_URL = BASE.replace(/^http/, "ws") + "/ws";
const TOPIC = "chat/thread/messages"; // scope is injected from the token
const KEY = "nlg_live_e2e.secret";
const SECRET = "whsec_e2e";
const target = (scopeId, extra = {}) => ({ appId: "app-chat", roomId: "room-thread", scopeId, topic: "messages", ...extra });

let passed = 0, failed = 0;
const ok = (name) => { passed++; console.log(`  PASS  ${name}`); };
const fail = (name, why) => { failed++; console.log(`  FAIL  ${name} — ${why}`); };
const check = (name, cond, why = "assertion failed") => (cond ? ok(name) : fail(name, why));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const waitFor = async (cond, ms = 4000, step = 50) => {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (cond()) return true;
    await sleep(step);
  }
  return false;
};

// ---------- local receiver: webhooks + control-plane reports ----------
const hooks = [];
const dlq = [];
let failHooks = false;
const receiver = http.createServer((req, res) => {
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const raw = Buffer.concat(chunks);
    if (req.url === "/hook") {
      hooks.push({ headers: req.headers, raw });
      res.writeHead(failHooks ? 500 : 200).end();
    } else {
      if (req.url === "/webhook-failures") dlq.push(JSON.parse(raw.toString()));
      res.writeHead(200, { "content-type": "application/json" }).end("{}");
    }
  });
});
await new Promise((r) => receiver.listen(18099, "0.0.0.0", r));

async function post(body, { key = KEY, raw = false } = {}) {
  const headers = { "content-type": "application/json" };
  if (key) headers.authorization = `Bearer ${key}`;
  const res = await fetch(`${BASE}/v1/publish`, { method: "POST", headers, body: raw ? body : JSON.stringify(body) });
  let json = null;
  try { json = await res.json(); } catch {}
  return { status: res.status, json, headers: res.headers };
}

function client(token) {
  return NoLag(token, { url: WS_URL, reconnect: false, heartbeatInterval: 0, debug: false });
}

async function listener(token) {
  const c = client(token);
  await c.connect();
  const got = [];
  c.subscribe(TOPIC);
  c.on(TOPIC, (data, meta) => got.push({ data, meta }));
  return { c, got };
}

console.log(`kraken publish e2e against ${BASE}\n`);

const a1 = await listener("tok-pub-a1");
const a2 = await listener("tok-pub-a2");
const b1 = await listener("tok-pub-b1");
await sleep(400);

// ---------- 1. server publish reaches its scope, and only its scope ----------
{
  const res = await post({ messages: [target("scope-a", { data: { text: "hello land a" } })] });
  check("publish: 200 with a message id", res.status === 200 && typeof res.json?.messages?.[0]?.id === "string",
        `status ${res.status} ${JSON.stringify(res.json)}`);
  const got = await waitFor(() => a2.got.some((m) => m.data?.text === "hello land a"));
  check("publish: subscriber in the scope receives it", got);
  const m = a2.got.find((x) => x.data?.text === "hello land a");
  check("publish: stamped fromType server, from = API key id",
        m?.meta?.fromType === "server" && m?.meta?.from === "key-e2e", JSON.stringify(m?.meta));
  check("publish: same envelope as a socket publish (recorded → msgId)", typeof m?.meta?.msgId === "string",
        JSON.stringify(m?.meta));
  await sleep(300);
  check("publish: a subscriber in another scope does NOT receive it",
        !b1.got.some((x) => x.data?.text === "hello land a"));
}
{
  await post({ messages: [target("scope-b", { data: { text: "hello land b" } })] });
  check("publish: scope B delivery", await waitFor(() => b1.got.some((m) => m.data?.text === "hello land b")));
  await sleep(300);
  check("publish: scope A does not see scope B", !a2.got.some((m) => m.data?.text === "hello land b"));
}

// ---------- 2. socket publish carries the actor ----------
{
  a1.c.emit(TOPIC, { text: "from a1" });
  const got = await waitFor(() => a2.got.some((m) => m.data?.text === "from a1"));
  const m = a2.got.find((x) => x.data?.text === "from a1");
  check("ws publish: stamped fromType actor, from = publisher", got && m.meta.fromType === "actor" && m.meta.from === "pub-a1",
        JSON.stringify(m?.meta));
}

// ---------- 3. a publisher cannot pose as the server ----------
{
  const forged = { _from: { type: "server", id: "key-e2e" }, _data: "pretend", text: "forged" };
  a1.c.emit(TOPIC, forged);
  await waitFor(() => a2.got.some((m) => m.data?.text === "forged"));
  const m = a2.got.find((x) => x.data?.text === "forged");
  check("forgery: forged _from arrives as data, sender stays the actor",
        m && m.meta.fromType === "actor" && m.meta.from === "pub-a1" && m.data._from?.type === "server",
        JSON.stringify(m));
}

// ---------- 4. signed webhook ----------
{
  const before = hooks.length;
  a1.c.emit(TOPIC, { text: "webhook me" });
  const arrived = await waitFor(() => hooks.length > before, 5000);
  check("webhook: fired for a socket publish on a configured topic", arrived);
  const hook = hooks[hooks.length - 1];
  const verified = hook && await verifyWebhookSignature(hook.raw, hook.headers["nolag-signature"], SECRET);
  check("webhook: signature verifies with the js-sdk helper", verified, hook?.headers["nolag-signature"]);
  const ev = hook ? JSON.parse(hook.raw.toString()) : {};
  check("webhook: v2 body carries the ids and the broker-attested sender",
        ev.type === "message.published" && ev.appId === "app-chat" && ev.roomId === "room-thread" &&
        ev.scopeId === "scope-a" && ev.topic === "messages" && ev.sender?.type === "actor" &&
        ev.sender?.id === "pub-a1" && ev.data?.text === "webhook me" &&
        hook.headers["nolag-webhook-id"] === ev.id,
        JSON.stringify(ev));
  check("webhook: wrong secret does not verify",
        hook && !(await verifyWebhookSignature(hook.raw, hook.headers["nolag-signature"], "whsec_other")));
}
{
  const before = hooks.length;
  a1.c.emit("chat/thread/_typing", { typing: true });
  await post({ messages: [target("scope-a", { data: { text: "server, no hook" } })] });
  await sleep(1500);
  check("webhook: not fired for a topic outside the webhook's topics, nor for server publishes",
        hooks.length === before, `${hooks.length - before} extra calls`);
}

// ---------- 5. errors ----------
{
  const r1 = await post({ messages: [target("scope-a", { data: 1 })] }, { key: null });
  check("errors: 401 without an API key", r1.status === 401 && r1.json?.error?.code === 40100, JSON.stringify(r1));
  const r2 = await post({ messages: [target("scope-a", { data: 1 })] }, { key: "nlg_live_nope.secret" });
  check("errors: 401 for an unknown key", r2.status === 401, JSON.stringify(r2));
  const r3 = await post({ messages: [target("scope-a", { data: 1 })] }, { key: "nlg_live_other.secret" });
  check("errors: 404 for another project's room", r3.status === 404 && r3.json?.error?.code === 42940, JSON.stringify(r3));
  const r4 = await post({ messages: [target("scope-a", { topic: "_typing", data: 1 })] });
  check("errors: 404 for a topic the key may not publish", r4.status === 404, JSON.stringify(r4));
  const r5 = await post("{not json", { raw: true });
  check("errors: 400 for invalid JSON", r5.status === 400 && r5.json?.error?.code === 40000, JSON.stringify(r5));
  const r6 = await post({ messages: [{ appId: "app-chat", data: 1 }] });
  check("errors: 400 with the index of a malformed message", r6.status === 400 && r6.json?.error?.index === 0, JSON.stringify(r6));
  const big = "x".repeat(950_000);
  const r7 = await post({ messages: [target("scope-a", { data: big })] });
  check("errors: 413 for a message over the size ceiling", r7.status === 413 && r7.json?.error?.code === 42930, JSON.stringify(r7?.json));
}
{
  const r = await post({ messages: [
    target("scope-a", { data: { text: "batch ok" } }),
    target("scope-a", { data: { text: "batch bad" }, filter: "a/b" }),
  ] });
  check("batch: an invalid message rejects the whole batch with its index",
        r.status === 400 && r.json?.error?.code === 42960 && r.json?.error?.index === 1, JSON.stringify(r.json));
  await sleep(400);
  check("batch: nothing from a rejected batch is delivered", !a2.got.some((m) => m.data?.text === "batch ok"));
}
{
  const r = await post({ messages: [1, 2, 3].map((n) => target("scope-a", { data: { seq: n } })) });
  await waitFor(() => a2.got.filter((m) => m.data?.seq).length >= 3);
  const seqs = a2.got.filter((m) => m.data?.seq).map((m) => m.data.seq);
  check("batch: published in order", r.status === 200 && r.json.messages.length === 3 && seqs.join() === "1,2,3",
        `${r.status} ${seqs}`);
}

// ---------- 6. a failed webhook reaches the DLQ as metadata only ----------
if (process.env.SKIP_DLQ) {
  console.log("  SKIP  dlq (SKIP_DLQ set)");
} else {
  failHooks = true;
  const before = dlq.length;
  a1.c.emit(TOPIC, { text: "will fail", secretPayload: "do-not-leak" });
  const got = await waitFor(() => dlq.length > before, 15000, 200);
  failHooks = false;
  const entry = dlq[dlq.length - 1];
  check("dlq: failed webhook reported after retries", got && entry?.attempts === 3 && entry?.responseStatus === 500,
        JSON.stringify(entry));
  check("dlq: report is metadata only (msgId, url, room) — no body, data or headers",
        entry && typeof entry.msgId === "string" && entry.roomId === "room-thread" &&
        entry.requestBody === undefined && entry.requestHeaders === undefined &&
        !JSON.stringify(entry).includes("do-not-leak"),
        JSON.stringify(entry));
}

console.log(`\n${passed} passed, ${failed} failed`);
for (const l of [a1, a2, b1]) l.c.disconnect();
receiver.close();
process.exit(failed ? 1 : 0);
