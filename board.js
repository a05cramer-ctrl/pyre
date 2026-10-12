// Vercel serverless function: GET /api/board            -> top Phoenix traders
//                             GET /api/board?trader=<addr> -> single trader report (wallet or Phoenix account)
// Source 1 (env BITQUERY_TOKEN): Bitquery Solana PerpetualPositions, realized PnL over 30 days (metric "pnl30d").
//   RealizedPnl is in PhUSD (Phoenix's quote currency).
// Source 2 (env HELIUS_API_KEY, used only when BITQUERY_TOKEN is missing): public Phoenix fills from the last 24h,
//   each transaction's signer resolved through Helius RPC, aggregated per wallet. Ranked by Phoenix 30d PnL when
//   Phoenix's /traders/{wallet}/pnl answers publicly (metric "pnl30d"), otherwise by 24h volume (metric "vol24h").
// Neither env var set -> 503.

const ENDPOINT = "https://streaming.bitquery.io/graphql";
const TTL = 5 * 60 * 1000;
const LIMIT = 50;
const cache = new Map(); // in-memory per warm instance

async function gql(query, variables) {
  const token = process.env.BITQUERY_TOKEN;
  if (!token) throw new Error("BITQUERY_TOKEN not set");
  const r = await fetch(ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify({ query, variables }),
  });
  const j = await r.json().catch(() => null);
  if (!r.ok || !j || j.errors) throw new Error("bitquery: " + (j && j.errors ? JSON.stringify(j.errors).slice(0, 300) : r.status));
  return j.data;
}

const num = (v) => (v == null ? 0 : Number(v)) || 0;
const since = () => new Date(Date.now() - 30 * 86400e3).toISOString();
const B58 = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

// 1) Leaderboard: realized PnL per Phoenix trader account (PDA), closed positions, AMM excluded.
const Q_BOARD = `
query Board($since: DateTime, $limit: Int) {
  Solana {
    PerpetualPositions(
      limit: { count: $limit }
      orderBy: { descendingByField: "pnl" }
      where: { Block: { Time: { since: $since } }, Position: { TraderIsAmm: false, Closed: true } }
    ) {
      Position { Trader }
      pnl: sum(of: Position_RealizedPnl)
      closes: count
      wins: count(if: { Position: { RealizedPnl: { gt: 0 } } })
    }
  }
}`;

// Count of distinct traders with a close in the window.
const Q_COUNT = `
query Count($since: DateTime) {
  Solana {
    PerpetualPositions(
      where: { Block: { Time: { since: $since } }, Position: { TraderIsAmm: false, Closed: true } }
    ) {
      traders: uniq(of: Position_Trader)
    }
  }
}`;

// 2) Map Phoenix account (PDA) -> signing wallet (most frequent signer), plus last closes.
const Q_DETAIL = `
query Detail($since: DateTime, $traders: [String!]) {
  Solana {
    signers: PerpetualPositions(
      where: { Block: { Time: { since: $since } }, Position: { Trader: { in: $traders }, TraderIsAmm: false, Liquidation: false } }
    ) {
      Position { Trader }
      Transaction { Signer }
      n: count
    }
    recent: PerpetualPositions(
      limitBy: { by: Position_Trader, count: 6 }
      orderBy: { descending: Block_Time }
      where: { Block: { Time: { since: $since } }, Position: { Trader: { in: $traders }, TraderIsAmm: false, Closed: true } }
    ) {
      Block { Time }
      Position { Trader Type RealizedPnl Asset { Symbol } }
    }
  }
}`;

// 3) Quote volume per trader from fills.
const Q_VOL = `
query Vol($since: DateTime, $traders: [String!]) {
  Solana {
    PerpetualFills(
      where: { Block: { Time: { since: $since } }, Fill: { Trader: { in: $traders } } }
    ) {
      Fill { Trader }
      vol: sum(of: Fill_Amount_Quote)
    }
  }
}`;

// Resolve a wallet (Signer) to its Phoenix trader account(s).
const Q_RESOLVE = `
query Resolve($since: DateTime, $addr: String) {
  Solana {
    PerpetualPositions(
      limit: { count: 5 }
      where: { Block: { Time: { since: $since } }, Transaction: { Signer: { is: $addr } }, Position: { TraderIsAmm: false } }
    ) {
      Position { Trader }
      n: count
    }
  }
}`;

const Q_ONE = `
query One($since: DateTime, $traders: [String!]) {
  Solana {
    PerpetualPositions(
      where: { Block: { Time: { since: $since } }, Position: { Trader: { in: $traders }, TraderIsAmm: false, Closed: true } }
    ) {
      Position { Trader }
      pnl: sum(of: Position_RealizedPnl)
      closes: count
      wins: count(if: { Position: { RealizedPnl: { gt: 0 } } })
    }
  }
}`;

function sym(s) { return String(s || "").replace(/-?PERP$/i, "").replace(/\/.*$/, ""); }

async function enrich(rows, s) {
  const accounts = rows.map((r) => r.account);
  const [det, vol] = await Promise.all([
    gql(Q_DETAIL, { since: s, traders: accounts }).catch(() => null),
    gql(Q_VOL, { since: s, traders: accounts }).catch(() => null),
  ]);
  const signer = {}, recent = {}, volume = {};
  if (det) {
    for (const r of det.Solana.signers || []) {
      const a = r.Position.Trader, w = r.Transaction.Signer, n = num(r.n);
      if (!signer[a] || n > signer[a].n) signer[a] = { w, n };
    }
    for (const r of det.Solana.recent || []) {
      const a = r.Position.Trader;
      (recent[a] = recent[a] || []).push({
        market: sym(r.Position.Asset && r.Position.Asset.Symbol),
        side: r.Position.Type || "",
        pnl: num(r.Position.RealizedPnl),
        time: Date.parse(r.Block.Time),
      });
    }
  }
  if (vol) for (const r of vol.Solana.PerpetualFills || []) volume[r.Fill.Trader] = num(r.vol);
  return rows.map((r) => ({
    ...r,
    wallet: signer[r.account] ? signer[r.account].w : r.account, // fee routing target = signing wallet
    walletResolved: !!signer[r.account],
    volume: volume[r.account] != null ? volume[r.account] : null,
    recent: recent[r.account] || [],
  }));
}

async function board() {
  const s = since();
  const [d, c] = await Promise.all([gql(Q_BOARD, { since: s, limit: LIMIT }), gql(Q_COUNT, { since: s }).catch(() => null)]);
  const rows = (d.Solana.PerpetualPositions || []).map((r) => ({
    account: r.Position.Trader, pnl: num(r.pnl), closes: num(r.closes), wins: num(r.wins),
  })).filter((r) => r.account);
  const traders = await enrich(rows, s);
  const total = c && c.Solana.PerpetualPositions[0] ? num(c.Solana.PerpetualPositions[0].traders) : traders.length;
  return { source: "bitquery", metric: "pnl30d", window: "30d", currency: "PhUSD", updated: new Date().toISOString(), total, traders };
}

async function one(addr) {
  const s = since();
  let accounts = [addr];
  const res = await gql(Q_RESOLVE, { since: s, addr }).catch(() => null);
  if (res) for (const r of res.Solana.PerpetualPositions || []) if (r.Position.Trader) accounts.push(r.Position.Trader);
  accounts = [...new Set(accounts)];
  const d = await gql(Q_ONE, { since: s, traders: accounts });
  const rows = (d.Solana.PerpetualPositions || []).map((r) => ({ account: r.Position.Trader, pnl: num(r.pnl), closes: num(r.closes), wins: num(r.wins) }));
  if (!rows.length) return { trader: null };
  rows.sort((a, b) => b.pnl - a.pnl);
  const [t] = await enrich([rows[0]], s);
  if (accounts[0] !== t.account) t.wallet = addr; // searched by wallet: route to that wallet
  return { metric: "pnl30d", source: "bitquery", trader: t };
}

/* ======================= Source 2: Phoenix public fills + Helius ======================= */
const PHX = "https://perp-api.phoenix.trade";
const FIRST = ["SOL", "BTC", "ETH"];
const MAX_RESOLVE = Number(process.env.PYRE_MAX_RESOLVE) || 800; // Helius getTransaction calls per rebuild (1 credit each)
const MAX_PAGES = 40;      // Phoenix fills pages per rebuild (1000 fills each)
const DEADLINE = 45e3;     // stay inside Vercel's 60s maxDuration

async function getJ(url, opt = {}, ms = 8000) {
  const c = new AbortController(); const tm = setTimeout(() => c.abort(), ms);
  try {
    const r = await fetch(url, { ...opt, signal: c.signal });
    if (!r.ok) { const e = new Error(url.replace(/api-key=[^&]+/, "api-key=***") + " " + r.status); e.status = r.status; throw e; }
    return await r.json();
  } finally { clearTimeout(tm); }
}
async function pool(items, n, fn) {
  const out = new Array(items.length); let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const k = i++; out[k] = await fn(items[k], k).catch(() => null); } }));
  return out;
}

async function activeSymbols() {
  const list = await getJ(PHX + "/v1/view/exchange/markets");
  const act = (Array.isArray(list) ? list : []).filter((m) => m && m.symbol && String(m.marketStatus).toLowerCase() === "active").map((m) => m.symbol);
  return [...FIRST.filter((s) => act.includes(s)), ...act.filter((s) => !FIRST.includes(s))];
}

// All fills of the last 24h across active markets (first page everywhere, then follow cursors on the busiest).
async function fills24h(t0) {
  const start = Date.now() - 24 * 3600e3;
  const syms = await activeSymbols();
  const all = []; let pages = 0;
  const take = (p) => (p && Array.isArray(p.data) ? p.data : []).forEach((f) => {
    const ts = Date.parse(f.timestamp), q = Math.abs(Number(f.quoteQty)), b = Number(f.baseQty);
    if (ts >= start && isFinite(q) && f.transactionSignature) all.push({ market: f.marketSymbol, sig: f.transactionSignature, size: q, side: b >= 0 ? "buy" : "sell", time: ts });
  });
  const first = await pool(syms, 8, (s) => getJ(`${PHX}/v1/trades/${encodeURIComponent(s)}/fills?limit=1000&startTime=${start}`));
  let open = [];
  first.forEach((p, i) => { pages++; take(p); if (p && p.hasMore && p.nextCursor) open.push({ s: syms[i], c: p.nextCursor }); });
  while (open.length && pages < MAX_PAGES && Date.now() - t0 < DEADLINE * 0.45) {
    const batch = open.splice(0, 6);
    const res = await pool(batch, 6, (o) => getJ(`${PHX}/v1/trades/${encodeURIComponent(o.s)}/fills?limit=1000&startTime=${start}&cursor=${encodeURIComponent(o.c)}`));
    res.forEach((p, i) => { pages++; const before = all.length; take(p); if (p && p.hasMore && p.nextCursor && all.length > before) open.push({ s: batch[i].s, c: p.nextCursor }); });
  }
  return { fills: all, markets: syms.length, pages };
}

// signature -> signing wallet via Helius RPC (batched getTransaction, jsonParsed)
async function resolveSigners(sigs, t0) {
  const url = "https://mainnet.helius-rpc.com/?api-key=" + encodeURIComponent(process.env.HELIUS_API_KEY);
  const map = {};
  const chunks = []; for (let i = 0; i < sigs.length; i += 50) chunks.push(sigs.slice(i, i + 50));
  const call = (sig, id) => ({ jsonrpc: "2.0", id, method: "getTransaction", params: [sig, { encoding: "jsonParsed", maxSupportedTransactionVersion: 0, commitment: "confirmed" }] });
  const read = (sig, r) => {
    const keys = r && r.transaction && r.transaction.message && r.transaction.message.accountKeys;
    if (!Array.isArray(keys)) return;
    const signers = keys.filter((k) => k && k.signer).map((k) => k.pubkey || k);
    if (signers.length) map[sig] = signers; // [0] = fee payer
  };
  await pool(chunks, 4, async (ch) => {
    if (Date.now() - t0 > DEADLINE * 0.9) return;
    const body = ch.map((s, i) => call(s, i));
    let res = null;
    try { res = await getJ(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }, 15000); } catch (e) {}
    if (Array.isArray(res)) { res.forEach((x) => { if (x && x.id != null && ch[x.id]) read(ch[x.id], x.result); }); return; }
    // batch not accepted: fall back to single calls
    await pool(ch, 5, async (s) => { const x = await getJ(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(call(s, 1)) }, 10000); read(s, x && x.result); });
  });
  // Relayer guard: if one fee payer pays for >25% of resolved txs that carry a second signer, credit the second signer.
  const vals = Object.values(map), payerN = {};
  vals.forEach((v) => { payerN[v[0]] = (payerN[v[0]] || 0) + 1; });
  const out = {};
  for (const [sig, v] of Object.entries(map)) out[sig] = v.length > 1 && payerN[v[0]] > vals.length * 0.25 ? v[1] : v[0];
  return out;
}

async function phoenixPnl(wallet) {
  const start = Date.now() - 30 * 86400e3;
  const a = await getJ(`${PHX}/v1/traders/${encodeURIComponent(wallet)}/pnl?resolution=1d&limit=30&startTime=${start}`);
  const pts = (Array.isArray(a) ? a : []).filter((p) => p && isFinite(Number(p.cumulativePnl))).sort((x, y) => Number(x.endTime || x.timestamp) - Number(y.endTime || y.timestamp));
  if (!pts.length) return null;
  const last = Number(pts[pts.length - 1].cumulativePnl);
  return pts.length > 1 ? last - Number(pts[0].cumulativePnl) : last;
}

async function phoenixBoard() {
  const t0 = Date.now();
  const { fills, markets, pages } = await fills24h(t0);
  if (!fills.length) throw new Error("no Phoenix fills returned");
  // one row per transaction (a market order can match several makers)
  const tx = new Map();
  for (const f of fills) {
    const x = tx.get(f.sig) || { sig: f.sig, size: 0, time: 0, legs: [] };
    x.size += f.size; x.time = Math.max(x.time, f.time); x.legs.push(f); tx.set(f.sig, x);
  }
  const txs = [...tx.values()].sort((a, b) => b.size - a.size);
  const pick = txs.slice(0, MAX_RESOLVE);
  const signer = await resolveSigners(pick.map((x) => x.sig), t0);
  const W = new Map();
  for (const x of pick) {
    const w = signer[x.sig]; if (!w) continue;
    const r = W.get(w) || { wallet: w, account: w, volume: 0, trades: 0, last: 0, markets: {}, recent: [] };
    r.volume += x.size; r.trades += 1; r.last = Math.max(r.last, x.time);
    for (const l of x.legs) r.markets[l.market] = (r.markets[l.market] || 0) + l.size;
    const lead = x.legs[0];
    r.recent.push({ market: lead.market, side: lead.side, size: x.size, time: x.time, sig: x.sig });
    W.set(w, r);
  }
  let traders = [...W.values()].map((r) => ({
    ...r,
    markets: Object.entries(r.markets).sort((a, b) => b[1] - a[1]).map((e) => e[0]),
    recent: r.recent.sort((a, b) => b.time - a.time).slice(0, 6),
  })).sort((a, b) => b.volume - a.volume);
  if (!traders.length) throw new Error("no signers resolved");
  const total = traders.length;
  traders = traders.slice(0, LIMIT);
  // Phoenix 30d PnL, only if the endpoint answers without auth (probe once).
  let metric = "vol24h";
  try {
    const p0 = await phoenixPnl(traders[0].wallet);
    if (p0 != null) {
      const top = traders.slice(0, 24);
      const ps = await pool(top, 6, (t) => phoenixPnl(t.wallet));
      top.forEach((t, i) => { if (ps[i] != null) t.pnl = ps[i]; });
      const withPnl = top.filter((t) => t.pnl != null);
      if (withPnl.length >= 3) { traders = withPnl.sort((a, b) => b.pnl - a.pnl); metric = "pnl30d"; }
    }
  } catch (e) {}
  return {
    source: "phoenix", metric, window: metric === "pnl30d" ? "30d" : "24h", currency: "USD", updated: new Date().toISOString(),
    total, fills: fills.length, resolved: Object.keys(signer).length, markets, pages, traders,
  };
}

let building = null;
async function cachedBoard(fn) {
  const hit = cache.get("board");
  if (hit && Date.now() - hit.t < TTL) return hit.v;
  if (!building) building = fn().then((v) => { cache.set("board", { t: Date.now(), v }); return v; }).finally(() => { building = null; });
  return building;
}

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate=600");
  const useBitquery = !!process.env.BITQUERY_TOKEN, useHelius = !useBitquery && !!process.env.HELIUS_API_KEY;
  if (!useBitquery && !useHelius) {
    res.setHeader("Cache-Control", "no-store");
    return res.status(503).json({ error: "board not configured: set BITQUERY_TOKEN or HELIUS_API_KEY" });
  }
  try {
    const q = (req.query && req.query.trader) || new URL(req.url, "http://x").searchParams.get("trader");
    if (q) {
      if (!B58.test(q)) return res.status(400).json({ error: "invalid address" });
      if (useHelius) {
        const b = await cachedBoard(phoenixBoard);
        const t = b.traders.find((x) => x.wallet === q) || null;
        return res.status(200).json({ metric: b.metric, source: b.source, trader: t });
      }
      const key = "t:" + q, hit = cache.get(key);
      if (hit && Date.now() - hit.t < TTL) return res.status(200).json(hit.v);
      const v = await one(q);
      cache.set(key, { t: Date.now(), v });
      return res.status(200).json(v);
    }
    const v = await cachedBoard(useBitquery ? board : phoenixBoard);
    return res.status(200).json(v);
  } catch (e) {
    const stale = cache.get("board");
    if (stale && !(req.query && req.query.trader)) return res.status(200).json({ ...stale.v, stale: true });
    return res.status(502).json({ error: String(e.message || e).slice(0, 300) });
  }
};
module.exports.config = { maxDuration: 60 };
