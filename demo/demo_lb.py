"""[T6] Load-balancing demo.

1. HAProxy spreads HTTP load over app1..app3; inside each app, reads go round-robin to the
   Shard A replicas and writes to the primary (read/write splitting).
2. Kill app2 -> HAProxy health checks remove it; traffic redistributes with no failed
   requests (HAProxy retries/redispatches in-flight requests).
3. Restart app2 -> it is re-added automatically.
4. WebSockets are balanced too, and a bid placed via any node is pushed live to clients on
   every node (cross-node fan-out via Redis).
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import uuid

import httpx
import websockets

from demo.common import (APPS, LB_URL, Checks, banner, bold, info, kill, ok, start, step, wait_for)
from demo.loadgen import Bucket, LoadGen, fmt_counter, ticker

STATS = "http://haproxy:8404/stats;csv"


def haproxy_status() -> dict[str, str]:
    rows = csv.DictReader(io.StringIO(httpx.get(STATS, timeout=2).text.lstrip("# ")))
    return {r["svname"]: r["status"] for r in rows if r["pxname"] == "apps" and r["svname"] in APPS}


def show_haproxy() -> str:
    st = haproxy_status()
    return "  ".join(f"{k}={v}" for k, v in sorted(st.items()))


def window(lg: LoadGen, a: int, b: int) -> Bucket:
    agg = Bucket()
    for s in range(a, b):
        x = lg.buckets.get(s)
        if not x:
            continue
        agg.ok += x.ok; agg.failed += x.failed; agg.rejected += x.rejected
        agg.errors.update(x.errors)
        agg.nodes.update(x.nodes); agg.db_reads.update(x.db_reads); agg.db_writes.update(x.db_writes)
    return agg


async def ws_fanout(c: Checks) -> None:
    step("WebSockets through HAProxy + cross-node fan-out")
    conns, hellos = [], []
    for _ in range(6):
        ws = await websockets.connect(LB_URL.replace("http", "ws") + "/ws")
        hellos.append(json.loads(await ws.recv())["node"])
        conns.append(ws)
    info(f"6 WebSocket clients via HAProxy landed on: {', '.join(hellos)}")
    c.check(len(set(hellos)) == 3, "WebSocket connections are spread over all 3 nodes")

    async with httpx.AsyncClient(base_url=LB_URL) as http:
        it = (await http.post("/api/items", json={"title": "LB fan-out demo", "start_price": 10, "duration_s": 600},
                              headers={"X-User-Id": "2"})).json()
        placed = []
        for i, node in enumerate(APPS):  # one bid through each node directly
            r = await http.post(f"http://{node}:8000/api/items/{it['id']}/bids", json={"amount": 11 + i},
                                headers={"X-User-Id": str(3 + i), "Idempotency-Key": str(uuid.uuid4())})
            placed.append((node, r.json()["amount"]))
        info("bids placed: " + ", ".join(f"${a:.0f} via {n}" for n, a in placed))

    for ws, home in zip(conns, hellos):
        got: list[tuple[float, str]] = []
        try:
            while len(got) < 3:
                ev = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                if ev.get("type") == "bid" and ev.get("item_id") == it["id"]:
                    got.append((ev["amount"], ev["node"]))
        except asyncio.TimeoutError:
            pass
        c.check(len(got) == 3, f"client on {home} saw all 3 bids: " + ", ".join(f"${a:.0f}←{n}" for a, n in got))
        await ws.close()


async def main() -> None:
    banner("DEMO [T6] Load balancing")
    c = Checks()
    step("HAProxy backend status")
    info(show_haproxy())

    lg = LoadGen(readers=6, bidders=2)
    await lg.start()
    step(f"Phase 1 — steady load (6 readers + 2 bidders on item #{lg.item_id}) for 6 s")
    await ticker(lg, 6)
    p1 = window(lg, 0, 6)
    info(bold(f"app nodes: {fmt_counter(p1.nodes)}"))
    info(bold(f"reads by DB node: {fmt_counter(p1.db_reads)}   writes by DB node: {fmt_counter(p1.db_writes)}"))
    shares = [p1.nodes.get(a, 0) / max(1, sum(p1.nodes.values())) for a in APPS]
    c.check(all(0.25 <= s <= 0.42 for s in shares), "HTTP load spread evenly (each node 25–42 %)")
    c.check(len(p1.db_writes) == 1, f"all writes went to one DB node (the primary): {list(p1.db_writes)}")
    c.check(len(p1.db_reads) >= 2 and not (set(p1.db_reads) & set(p1.db_writes)),
            "reads served by replicas only (read/write splitting)")

    step("Phase 2 — kill app2 (docker kill, SIGKILL)")
    t_kill = int(lg.elapsed()) + 1
    kill("app2")
    down = asyncio.create_task(asyncio.to_thread(
        wait_for, lambda: haproxy_status().get("app2", "").startswith("DOWN"), 10, 0.2, "app2 DOWN"))
    await ticker(lg, t_kill + 6, "app2 killed")
    info(f"HAProxy marked app2 DOWN {await down:.1f}s after the kill  ({show_haproxy()})")
    p2 = window(lg, t_kill + 2, t_kill + 6)
    info(bold(f"app nodes after removal: {fmt_counter(p2.nodes)}"))
    c.check("app2" not in p2.nodes and {"app1", "app3"} <= set(p2.nodes), "traffic redistributed to app1 + app3")
    w = window(lg, t_kill - 1, t_kill + 6)
    c.check(w.failed == 0, f"failed requests while app2 died: {w.failed} {dict(w.errors) if w.errors else ''}")

    step("Phase 3 — restart app2")
    t_up = int(lg.elapsed()) + 1
    start("app2")
    took = await asyncio.to_thread(wait_for, lambda: haproxy_status().get("app2") == "UP", 30, 0.2, "app2 UP")
    info(f"HAProxy re-added app2 after {took:.1f}s  ({show_haproxy()})")
    t_back = int(lg.elapsed()) + 1
    await ticker(lg, t_back + 4, "app2 back")
    p3 = window(lg, t_back, t_back + 4)
    info(bold(f"app nodes after re-add: {fmt_counter(p3.nodes)}"))
    c.check(p3.nodes.get("app2", 0) > 0, "app2 receives traffic again")
    await lg.stop()
    t = lg.total
    info(f"total: ok={t.ok} rejected={t.rejected} failed={t.failed} accepted bids={t.bids_ok} retries={lg.retries}")

    await ws_fanout(c)
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
