import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const engine = new URL("../../../SW-AI-W11-12-REACT/src/engine/", import.meta.url);
const assets = new Map([
  ["/", [new URL("index.html", import.meta.url), "text/html"]],
  ["/app.mjs", [new URL("app.mjs", import.meta.url), "text/javascript"]],
  ["/style.css", [new URL("style.css", import.meta.url), "text/css"]],
  ...["component", "diff", "patch", "render"].map(name =>
    [`/engine/${name}.js`, [new URL(`${name}.js`, engine), "text/javascript"]]),
]);

export function dropApi(requestDb) {
  function requireOk(result) {
    if (!result.ok) throw new Error(result.error?.code || "DBMS request failed");
    return result;
  }

  async function session(operation) {
    const { session: token } = requireOk(await requestDb("POST", "/sessions"));
    const path = `/sessions/${token}`;
    const query = sql => requestDb("POST", `${path}/query`, sql);
    try {
      requireOk(await query("BEGIN;"));
      return await operation(query);
    } finally {
      // Closing the session discards any remaining uncommitted changes.
      await requestDb("DELETE", path).catch(() => {});
    }
  }

  const readStock = async query =>
    requireOk(await query("SELECT stock FROM drops WHERE id = 1;")).rows[0][0];

  return {
    snapshot: () => session(async query => {
      const stock = await readStock(query);
      const { rows } = requireOk(await query(
        "SELECT id FROM orders WHERE drop_id = 1 ORDER BY id DESC LIMIT 3;"));
      return { stock, orders: rows.map(([id]) => id) };
    }),
    order: async () => {
      let commitSent = false;
      let orderId;
      try {
        return await session(async query => {
          const stock = BigInt(await readStock(query));
          if (stock <= 0n) return { status: "sold_out" };
          const update = requireOk(await query(
            `UPDATE drops SET stock = ${stock - 1n} WHERE id = 1;`));
          if (update.affected_rows !== "1") throw new Error("Drop row missing");
          orderId = requireOk(await query("INSERT INTO orders VALUES (1);")).inserted_id;
          commitSent = true;
          const result = await query("COMMIT;");
          if (result.commit === "DURABLE") {
            return { status: "durable", orderId, txid: result.txid,
              recoveryRequired: result.recovery_required };
          }
          if (result.commit !== "NOT_COMMITTED") {
            return { status: "unknown", orderId, txid: result.txid };
          }
          return { status: result.error?.code === "ERR_CONFLICT" ? "conflict" : "failed",
            recoveryRequired: result.recovery_required };
        });
      } catch {
        return commitSent ? { status: "unknown", orderId } : { status: "failed" };
      }
    },
  };
}

export function webServer(api) {
  return createServer(async (req, res) => {
    function json(status, data) {
      res.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(data));
    }

    try {
      if (req.method === "GET" && req.url === "/api/drop") return json(200, await api.snapshot());
      if (req.method === "POST" && req.url === "/api/order" &&
          req.headers["x-drop-request"] === "1") {
        return json(200, await api.order());
      }
      const asset = req.method === "GET" && assets.get(req.url);
      if (!asset) return json(404, { error: "Not found" });
      const [url, type] = asset;
      const bytes = await readFile(url);
      res.writeHead(200, { "Content-Type": `${type}; charset=utf-8` });
      res.end(bytes);
    } catch (error) {
      console.error(`Drop request failed: ${error.message}`, error.cause?.code ?? "");
      json(503, { error: "Drop unavailable" });
    }
  });
}

export function configuredApi() {
  const token = process.env.MINIDB_TOKEN;
  if (!token) throw new Error("Set MINIDB_TOKEN to the DBMS server token");
  const dbUrl = process.env.MINIDB_URL || "http://127.0.0.1:8080";
  return dropApi(async (method, path, sql) => {
    const response = await fetch(new URL(path, dbUrl), {
      method,
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: sql === undefined ? undefined : JSON.stringify({ sql }),
      signal: AbortSignal.timeout(8000),
    });
    return response.json();
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const port = Number(process.env.DROP_PORT || 13000);
  const server = webServer(configuredApi());
  server.on("error", error => {
    console.error(`Cannot start the web server on 127.0.0.1:${port} (${error.code}).`);
    if (["EACCES", "EADDRINUSE"].includes(error.code)) {
      console.error("Set DROP_PORT to another available port and run the command again.");
    }
    process.exitCode = 1;
  });
  server.listen(port, "127.0.0.1", () => {
    console.log(`Limited Drop · http://127.0.0.1:${port}`);
  });
}
