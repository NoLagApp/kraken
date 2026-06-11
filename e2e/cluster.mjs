// Cluster e2e: alice connects to node 1, bob to node 2. A message
// published on node 1 must arrive at node 2 (syn broker fan-out over
// Erlang distribution), and presence must be visible cross-node.
//
// Usage: node e2e/cluster.mjs  (expects docker-compose.cluster.yml up)
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const { NoLag } = require("../../js-sdk/dist/index.cjs");

const NODE1 = "ws://localhost:8081/ws";
const NODE2 = "ws://localhost:8082/ws";
const TOPIC = "demo/general/messages";

let passed = 0, failed = 0;
const ok = (n) => { passed++; console.log(`  PASS  ${n}`); };
const fail = (n, w) => { failed++; console.log(`  FAIL  ${n} — ${w}`); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const waitFor = async (cond, ms = 5000) => {
  const start = Date.now();
  while (Date.now() - start < ms) {
    if (cond()) return true;
    await sleep(50);
  }
  return false;
};

console.log(`kraken cluster e2e: ${NODE1} <-> ${NODE2}\n`);

const alice = NoLag("dev-token-alice", { url: NODE1, reconnect: false, heartbeatInterval: 0 });
const bob = NoLag("dev-token-bob", { url: NODE2, reconnect: false, heartbeatInterval: 0 });
await alice.connect();
await bob.connect();
ok("both nodes accept connections");

const bobGot = [];
bob.subscribe(TOPIC);
bob.on(TOPIC, (d) => bobGot.push(d));
await sleep(500);

alice.subscribe(TOPIC);
await sleep(300);
alice.emit(TOPIC, { text: "cross-node hello" });
(await waitFor(() => bobGot.length >= 1))
  ? ok("cross-node delivery: node1 publish -> node2 subscriber")
  : fail("cross-node delivery: node1 publish -> node2 subscriber", "timeout");
if (bobGot[0]?.text === "cross-node hello") ok("cross-node payload intact");
else fail("cross-node payload intact", JSON.stringify(bobGot[0]));

// cross-node presence
let joinSeen = false;
alice.on("presence:join", () => { joinSeen = true; });
const aliceRoom = alice.setApp("demo").setRoom("general");
const bobRoom = bob.setApp("demo").setRoom("general");
aliceRoom.setPresence({ name: "alice", node: 1 });
await sleep(300);
bobRoom.setPresence({ name: "bob", node: 2 });
(await waitFor(() => joinSeen))
  ? ok("cross-node presence: join on node2 seen from node1")
  : fail("cross-node presence: join on node2 seen from node1", "timeout");

// syn replicates cross-node asynchronously - poll briefly
let list = [];
for (let i = 0; i < 20 && list.length < 2; i++) {
  list = await aliceRoom.fetchPresence().catch(() => []);
  if (list.length < 2) await sleep(150);
}
(Array.isArray(list) && list.length >= 2)
  ? ok(`cross-node presence: room shows both members (${list.length})`)
  : fail("cross-node presence: room shows both members", JSON.stringify(list));

alice.disconnect();
bob.disconnect();
await sleep(200);
console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
