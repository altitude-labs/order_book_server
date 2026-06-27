#!/usr/bin/env node

const args = parseArgs(process.argv.slice(2));

if (!args.a || !args.b || !args.user) {
  usage();
  process.exit(1);
}

const statusFilter = (args.status ?? "filled").toLowerCase();
const graceMs = Number(args["grace-ms"] ?? 5000);
const endpoints = [
  { id: "A", url: args.a },
  { id: "B", url: args.b },
];

const seen = new Map();

for (const endpoint of endpoints) {
  connect(endpoint);
}

function connect(endpoint) {
  const ws = new WebSocket(endpoint.url);

  ws.addEventListener("open", () => {
    console.error(`[${endpoint.id}] connected ${endpoint.url}`);
    ws.send(
      JSON.stringify({
        method: "subscribe",
        subscription: {
          type: "orderUpdates",
          user: args.user,
        },
      }),
    );
  });

  ws.addEventListener("message", async (event) => {
    const text = await messageText(event.data);
    let message;
    try {
      message = JSON.parse(text);
    } catch {
      console.error(`[${endpoint.id}] non-json message: ${text}`);
      return;
    }

    if (message.channel !== "orderUpdates" || !Array.isArray(message.data)) {
      return;
    }

    for (const update of message.data) {
      const orderStatus = update.orderStatus;
      if (!orderStatus || String(orderStatus.status).toLowerCase() !== statusFilter) {
        continue;
      }
      recordUpdate(endpoint.id, update);
    }
  });

  ws.addEventListener("close", (event) => {
    console.error(`[${endpoint.id}] closed code=${event.code} reason=${event.reason || ""}`);
  });

  ws.addEventListener("error", (event) => {
    console.error(`[${endpoint.id}] websocket error`, event.error ?? event.message ?? event);
  });
}

async function messageText(data) {
  if (typeof data === "string") {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return Buffer.from(data).toString("utf8");
  }
  if (ArrayBuffer.isView(data)) {
    return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString("utf8");
  }
  if (data && typeof data.text === "function") {
    return data.text();
  }
  return String(data);
}

function recordUpdate(endpointId, update) {
  const key = updateKey(update);
  const entry = seen.get(key) ?? {
    firstSeenAt: Date.now(),
    endpoints: new Set(),
    samples: {},
    reportedMissing: false,
    timer: null,
  };

  entry.endpoints.add(endpointId);
  entry.samples[endpointId] = update;

  if (!entry.timer) {
    entry.timer = setTimeout(() => reportIfMissing(key), graceMs);
  }

  if (entry.reportedMissing && entry.endpoints.size === endpoints.length) {
    console.log(
      JSON.stringify({
        type: "late_match",
        key,
        receivedOn: endpointId,
        delayMs: Date.now() - entry.firstSeenAt,
      }),
    );
  }

  seen.set(key, entry);
}

function reportIfMissing(key) {
  const entry = seen.get(key);
  if (!entry || entry.endpoints.size === endpoints.length) {
    return;
  }

  entry.reportedMissing = true;
  const seenOn = [...entry.endpoints].sort();
  const missingOn = endpoints.map((endpoint) => endpoint.id).filter((id) => !entry.endpoints.has(id));
  const sample = entry.samples[seenOn[0]];
  const orderStatus = sample.orderStatus;
  const order = orderStatus.order ?? {};

  console.log(
    JSON.stringify({
      type: "missing_order_update",
      key,
      status: orderStatus.status,
      seenOn,
      missingOn,
      waitedMs: graceMs,
      outerTime: sample.time,
      height: sample.height,
      orderStatusTime: orderStatus.time,
      user: orderStatus.user ?? sample.user,
      hash: orderStatus.hash ?? null,
      order: {
        coin: order.coin,
        oid: order.oid,
        side: order.side,
        limitPx: order.limitPx,
        sz: order.sz,
        timestamp: order.timestamp,
        cloid: order.cloid ?? null,
      },
    }),
  );
}

function updateKey(update) {
  const orderStatus = update.orderStatus ?? {};
  const order = orderStatus.order ?? {};
  const hashPart = orderStatus.hash ? `hash:${orderStatus.hash}` : `time:${orderStatus.time ?? ""}`;
  return [
    String(orderStatus.status ?? "").toLowerCase(),
    String(orderStatus.user ?? update.user ?? "").toLowerCase(),
    String(order.coin ?? ""),
    String(order.oid ?? ""),
    hashPart,
    String(order.side ?? ""),
    String(order.limitPx ?? ""),
    String(order.sz ?? ""),
    String(order.timestamp ?? ""),
  ].join("|");
}

function parseArgs(argv) {
  const parsed = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) {
      continue;
    }
    const eq = arg.indexOf("=");
    if (eq !== -1) {
      parsed[arg.slice(2, eq)] = arg.slice(eq + 1);
    } else {
      parsed[arg.slice(2)] = argv[i + 1];
      i += 1;
    }
  }
  return parsed;
}

function usage() {
  console.error(`Usage:
  node scripts/compare_order_updates.mjs \\
    --a ws://node-a.example.com:8081/ws \\
    --b ws://node-b.example.com:8081/ws \\
    --user 0x1234567890abcdef1234567890abcdef12345678

Options:
  --status filled      Order status to compare. Default: filled
  --grace-ms 5000     Wait this long before reporting a missing update.
`);
}
