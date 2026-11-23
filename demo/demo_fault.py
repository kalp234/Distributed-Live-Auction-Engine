"""[T2] Fault-tolerance demo: kill components one after another under continuous load.

  phase 1  kill app2 (SIGKILL)               HAProxy reroutes; its WebSocket clients reconnect
  phase 2  kill a Shard A replica (the SYNC one) Patroni makes the async replica sync; reads
                                               skip the dead replica (circuit breaker)
  phase 3  kill Redis                         bids continue; live push degrades to polling
  phase 4  restore everything                 Redis board rebuilt from DB; replica re-streams

Throughout: readers + bidders through HAProxy, WebSocket watchers, and an auction that is
scheduled to close in the middle of the chaos. Afterwards we prove that every bid the
clients were told was accepted is in the database (on the primary AND every replica),
the auction was settled, and the money invariants hold.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from dataclasses import dataclass, field

import httpx
import websockets

from demo import invariants
from demo.common import (LB_URL, Checks, banner, bold, dim, info, is_running, kill, patroni_cluster,
                         print_cluster, start, step, wait_for, yellow)
from demo.db import connect
from demo.loadgen import Bucket, LoadGen, ticker


@dataclass
class Watcher:
    """A WebSocket client that reconnects (through HAProxy) whenever it is dropped."""
    nodes: list[str] = field(default_factory=list)
    events: Counter = field(default_factory=Counter)
    modes: list[str] = field(default_factory=list)
    reconnects: int = 0

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                async with websockets.connect(LB_URL.replace("http", "ws") + "/ws", open_timeout=3) as ws:
                    while not stop.is_set():
                        try:
                            ev = json.loads(await asyncio.wait_for(ws.recv(), timeout=0.5))
                        except asyncio.TimeoutError:
                            continue
                        self.events[ev.get("type")] += 1
                        if ev.get("type") == "hello":
                            self.nodes.append(ev["node"])
                            self.modes.append(ev["mode"])
                        elif ev.get("type") == "mode":
                            self.modes.append(ev["mode"])
            except Exception:
                pass
            if not stop.is_set():
                self.reconnects += 1
                await asyncio.sleep(0.3)


def phase_summary(lg: LoadGen, a: int, b: int) -> Bucket:
    agg = Bucket()
    for s in range(a, b):
        x = lg.buckets.get(s)
        if x:
            agg.ok += x.ok; agg.rejected += x.rejected; agg.failed += x.failed; agg.bids_ok += x.bids_ok
            agg.errors.update(x.errors)
    return agg


def roles() -> dict[str, str]:
    c = patroni_cluster("A") or {"members": []}
    return {m["role"]: m["name"] for m in c["members"]}


async def main() -> None:
    banner("DEMO [T2] Fault tolerance — kill an app node, a DB replica and Redis under load")
    c = Checks()
    http = httpx.AsyncClient(base_url=LB_URL, timeout=15)

    # An auction that will end in the middle of the chaos; it must still be settled.
    closing = (await http.post("/api/items", json={"title": "Closes during chaos", "start_price": 5, "duration_s": 14},
                               headers={"X-User-Id": "2"})).json()["id"]
    r = await http.post(f"/api/items/{closing}/bids", json={"amount": 42}, headers={"X-User-Id": "3"})
    info(f"auction #{closing} ends in 14 s; user 3 leads with $42 ({r.status_code})")

    stop = asyncio.Event()
    watchers = [Watcher() for _ in range(6)]
    wtasks = [asyncio.create_task(w.run(stop)) for w in watchers]
    lg = LoadGen(readers=6, bidders=3)
    await lg.start()
    await asyncio.sleep(1)
    info(f"load: 6 readers + 3 bidders on item #{lg.item_id}; 6 WebSocket watchers on {Counter(w.nodes[0] for w in watchers if w.nodes)}")
    marks: dict[str, int] = {}

    step("Phase 0 — steady state")
    await ticker(lg, 4)

    step("Phase 1 — kill app2 (SIGKILL, mid-auction)")
    marks["app2"] = int(lg.elapsed()) + 1
    kill("app2")
    await ticker(lg, marks["app2"] + 5, "app2 dead")

    r0 = roles()
    sync = r0.get("sync_standby")
    step(f"Phase 2 — kill a Shard A replica: the SYNC standby {sync}")
    marks["replica"] = int(lg.elapsed()) + 1
    kill(sync)
    await ticker(lg, marks["replica"] + 7, f"{sync} dead")
    print_cluster("A")
    stall = [sec for sec in range(marks["replica"], marks["replica"] + 6) if lg.buckets.get(sec, Bucket()).bids_ok == 0]
    info(yellow(f"write stall while Patroni re-assigned the sync standby: {len(stall)} s with 0 commits "
                f"(remote_apply commits wait for the sync replica — the price of zero-loss replication)"))
    r1 = roles()
    c.check(r1.get("sync_standby") not in (None, sync), f"Patroni made {r1.get('sync_standby')} the new sync standby")

    step("Phase 3 — kill Redis")
    marks["redis"] = int(lg.elapsed()) + 1
    kill("redis")
    await ticker(lg, marks["redis"] + 5, "redis dead")
    live = (await http.get("/api/live")).json()
    info(f"GET /api/live while Redis is down → source={bold(live['source'])} ({len(live['items'])} open items)")
    c.check(live["source"] == "db", "live board falls back to the database")

    step("Phase 4 — restore Redis, app2 and the replica")
    marks["restore"] = int(lg.elapsed()) + 1
    start("redis"); start("app2"); start(sync)
    await asyncio.to_thread(wait_for, lambda: (patroni_cluster("A") and all(
        m["state"] in ("running", "streaming") for m in patroni_cluster("A")["members"])
        and len(patroni_cluster("A")["members"]) == 3), 60, 0.5, f"{sync} streaming again")
    await ticker(lg, int(lg.elapsed()) + 4, "restored")
    live = (await http.get("/api/live")).json()
    info(f"GET /api/live after Redis restart → source={bold(live['source'])} ({len(live['items'])} open items rebuilt from DB)")
    c.check(live["source"] == "redis" and len(live["items"]) > 0, "Redis live board rebuilt from the database")
    await lg.stop()
    await asyncio.sleep(1.0)
    stop.set()
    await asyncio.gather(*wtasks)

    # ------------------------------------------------------------------ verification
    step("Per-phase results")
    end = max(lg.buckets) + 1
    spans = [("steady", 0, marks["app2"]), ("app2 killed", marks["app2"], marks["replica"]),
             ("replica killed", marks["replica"], marks["redis"]), ("redis killed", marks["redis"], marks["restore"]),
             ("restore", marks["restore"], end)]
    for name, a, b in spans:
        s = phase_summary(lg, a, b)
        info(f"{name:15} ok={s.ok:<5} rejected={s.rejected:<4} failed={s.failed:<3} committed bids={s.bids_ok:<4}"
             + (dim(f" errors={dict(s.errors)}") if s.errors else ""))
    t = lg.total
    info(bold(f"total: ok={t.ok} rejected={t.rejected} failed={t.failed} committed bids={t.bids_ok} client retries={lg.retries}"))
    c.check(t.failed <= max(5, t.ok // 200), f"bidding and reads continued through every failure (failed {t.failed} of {t.ok + t.failed + t.rejected})")

    step("No committed bid lost")
    confirmed = {b["bid_id"] for b in lg.accepted_bids}
    a = await connect("A")
    stored = {r["id"] for r in await a.fetch("SELECT id FROM bids WHERE item_id = $1", lg.item_id)}
    closed = await a.fetchrow("SELECT status, highest_bidder, current_price FROM items WHERE id = $1", closing)
    await a.close()
    info(f"bids confirmed to clients: {len(confirmed)}   stored on primary: {len(stored)}")
    c.check(confirmed <= stored, f"every confirmed bid is on the primary (missing: {len(confirmed - stored)})")
    for m in (patroni_cluster("A") or {"members": []})["members"]:
        if m["role"] == "leader":
            continue
        rc = await connect("A", member=m["name"])
        on_rep = {r["id"] for r in await rc.fetch("SELECT id FROM bids WHERE item_id = $1", lg.item_id)}
        await rc.close()
        c.check(confirmed <= on_rep, f"…and on replica {m['name']} ({m['role']}): {len(on_rep)} bids")

    step("Auction that ended during the chaos")
    info(f"auction #{closing}: status={closed['status']} winner=user {closed['highest_bidder']} price=${closed['current_price']}")
    c.check(closed["status"] == "sold" and closed["highest_bidder"] == 3, "closed and settled exactly once by the closer leader")

    step("Live updates (WebSocket watchers)")
    reconnects = sum(w.reconnects for w in watchers)
    modes = Counter(m for w in watchers for m in w.modes)
    bids_seen = sum(w.events["bid"] for w in watchers)
    info(f"reconnects={reconnects}  hello nodes={Counter(n for w in watchers for n in w.nodes)}  "
         f"mode messages={dict(modes)}  bid events received={bids_seen}")
    orphans = [w for w in watchers if w.nodes and w.nodes[0] == "app2"]
    c.check(all(len(w.nodes) > 1 and w.nodes[1] != "app2" for w in orphans),
            f"the {len(orphans)} client(s) of the killed app2 reconnected to other nodes: {[w.nodes for w in orphans]}")
    c.check(modes.get("polling", 0) > 0 and modes.get("push", 0) > 0, "clients told to poll while Redis was down, back to push after")
    c.check(bids_seen > 0, "live bid events kept flowing")

    for svc in ("app2", sync, "redis"):
        c.check(is_running(svc), f"{svc} running again")
    step("Invariants")
    await invariants.run(c)
    await http.aclose()
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
