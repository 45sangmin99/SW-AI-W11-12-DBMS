import assert from "node:assert/strict";
import { once } from "node:events";
import { configuredApi, webServer } from "./server.mjs";

const server = webServer(configuredApi()).listen(0, "127.0.0.1");
await once(server, "listening");
const base = `http://127.0.0.1:${server.address().port}`;
const read = async () => (await fetch(base + "/api/drop")).json();
const order = async () => (await fetch(base + "/api/order", {
  method: "POST", headers: { "X-Drop-Request": "1" },
})).json();

try {
  for (const path of ["/", "/style.css", "/app.mjs", "/engine/component.js",
    "/engine/diff.js", "/engine/patch.js", "/engine/render.js"]) {
    assert.equal((await fetch(base + path)).status, 200, path);
  }
  assert.deepEqual(await read(), { stock: "3", orders: [] }, "Use a fresh demo database");
  assert.equal((await order()).status, "durable");
  assert.equal((await order()).status, "durable");
  const outcomes = await Promise.all([order(), order()]);
  assert.equal(outcomes.filter(result => result.status === "durable").length, 1);
  assert(outcomes.some(result => ["conflict", "sold_out"].includes(result.status)));
  const snapshot = await read();
  assert.equal(snapshot.stock, "0");
  assert.equal(new Set(snapshot.orders).size, 3);
  assert.equal((await order()).status, "sold_out");
  console.log("Verified HTML and engine modules, two orders, concurrent final order, and sold-out state.");
} finally {
  server.close();
}
