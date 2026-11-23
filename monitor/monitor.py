"""Monitor / dashboard service.

Polls every second and serves one JSON snapshot (/api/state) + a dashboard page (/):
  * Patroni view of both shards: current primary, roles, timelines        [T3]
  * pg_stat_replication on each primary: lag in bytes and ms                [T1]
  * app node health/readiness + realtime mode                              [T2]
  * HAProxy per-node status and request rate (LB distribution)             [T6]
  * 2PC coordinator log state counts + prepared (in-doubt) transactions    [T4][T5]
  * money invariant on the wallet shard                                     [T4]
  * live event log from Redis + topology changes detected here
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import asyncpg
import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import FileResponse

log = logging.getLogger("monitor")
logging.basicConfig(level=logging.INFO)

PATRONI = {"A": os.environ.get("PATRONI_A", "").split(","), "B": os.environ.get("PATRONI_B", "").split(",")}
DBNAME = {"A": os.environ.get("DB_A_NAME", "auction"), "B": os.environ.get("DB_B_NAME", "wallet")}
APPS = ["app1", "app2", "app3"]
HAPROXY_STATS = os.environ.get("HAPROXY_STATS", "http://haproxy:8404/stats;csv")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
DB_USER, DB_PW = os.environ.get("DB_USER", "auction_app"), os.environ.get("DB_PASSWORD", "auction_pw")

REPLICATION_SQL = """
SELECT application_name AS name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint AS replay_lag_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)::bigint  AS flush_lag_bytes,
       round(extract(epoch FROM replay_lag) * 1000, 1)::float8 AS replay_lag_ms
FROM pg_stat_replication ORDER BY 1"""


class Monitor:
    def __init__(self) -> None:
        self.http = httpx.AsyncClient(timeout=0.8)
        self.state: dict[str, Any] = {}
        self.events: deque[dict[str, Any]] = deque(maxlen=150)
        self.leaders: dict[str, str | None] = {"A": None, "B": None}
        self.leader_since: dict[str, float] = {}
        self.conns: dict[str, tuple[str, asyncpg.Connection]] = {}
        self.req_prev: dict[str, int] = {}
        self.req_hist: deque[dict[str, int]] = deque(maxlen=30)   # per-second request deltas
        self.redis = aioredis.from_url(REDIS_URL, socket_timeout=1, socket_connect_timeout=1)
        self.redis_ok = False

    def event(self, kind: str, **data: Any) -> None:
        self.events.appendleft({"type": kind, "ts": time.time(), **data})

    # ------------------------------------------------------------------ sources
    async def patroni(self, shard: str) -> dict[str, Any] | None:
        for url in PATRONI[shard]:
            try:
                r = await self.http.get(f"{url}/cluster")
                if r.status_code == 200:
                    return r.json()
            except httpx.HTTPError:
                continue
        return None

    async def conn(self, shard: str, host: str) -> asyncpg.Connection:
        cur = self.conns.get(shard)
        if cur and cur[0] == host and not cur[1].is_closed():
            return cur[1]
        if cur:
            cur[1].terminate()
        c = await asyncpg.connect(host=host, user=DB_USER, password=DB_PW, database=DBNAME[shard], timeout=1)
        self.conns[shard] = (host, c)
        return c

    async def shard(self, shard: str) -> dict[str, Any]:
        cl = await self.patroni(shard)
        out: dict[str, Any] = {"members": [], "leader": None, "replication": [], "error": None}
        if cl is None:
            out["error"] = "no Patroni member reachable"
            return out
        out["members"] = sorted(({k: m.get(k) for k in ("name", "role", "state", "timeline", "lag")}
                                 for m in cl["members"]), key=lambda m: m["name"])
        leader = next((m["name"] for m in cl["members"] if m["role"] == "leader" and m["state"] == "running"), None)
        out["leader"] = leader
        if leader != self.leaders[shard]:
            self.event("primary_change", shard=shard, old=self.leaders[shard], new=leader)
            self.leaders[shard] = leader
            self.leader_since[shard] = time.time()
        out["leader_since"] = self.leader_since.get(shard)
        if leader:
            try:
                c = await self.conn(shard, leader)
                out["replication"] = [dict(r) for r in await c.fetch(REPLICATION_SQL, timeout=1)]
                out["prepared_xacts"] = await c.fetchval("SELECT count(*) FROM pg_prepared_xacts", timeout=1)
                if shard == "A":
                    rows = await c.fetch("SELECT state, count(*) n, count(*) FILTER (WHERE NOT completed) pending"
                                         " FROM coord.txn_log GROUP BY state", timeout=1)
                    out["txn_log"] = {r["state"]: {"n": r["n"], "pending": r["pending"]} for r in rows}
                    out["bids"] = await c.fetchval("SELECT count(*) FROM bids", timeout=1)
                else:
                    m = await c.fetchrow("""SELECT sum(balance)::float8 total, sum(held)::float8 held,
                        (SELECT coalesce(sum(amount),0) FROM escrow_holds WHERE status='active')::float8 active
                        FROM wallets""", timeout=1)
                    out["money"] = dict(m)
            except Exception as e:
                out["error"] = f"{type(e).__name__}: {e}"
                self.conns.pop(shard, None)
        return out

    async def app(self, node: str) -> dict[str, Any]:
        try:
            r = await self.http.get(f"http://{node}:8000/api/node")
            d = r.json()
            ready = (await self.http.get(f"http://{node}:8000/readyz")).json()
            return {"up": True, "ready": ready["ready"], "writes": ready["writes_available"],
                    "realtime": d["realtime"]["mode"], "ws_clients": d["realtime"]["ws_clients"]}
        except Exception as e:
            return {"up": False, "error": type(e).__name__}

    async def haproxy(self) -> dict[str, Any]:
        try:
            text = (await self.http.get(HAPROXY_STATS)).text
        except httpx.HTTPError as e:
            return {"error": str(e)}
        rows = [r for r in csv.DictReader(io.StringIO(text.lstrip("# "))) if r["pxname"] == "apps" and r["svname"] in APPS]
        out, delta = {}, {}
        for r in rows:
            tot = int(r["req_tot"] or 0)
            delta[r["svname"]] = max(0, tot - self.req_prev.get(r["svname"], tot))
            self.req_prev[r["svname"]] = tot
            out[r["svname"]] = {"status": r["status"], "req_tot": tot, "check": r["check_status"],
                                "retries": int(r["wretr"] or 0), "redispatches": int(r["wredis"] or 0)}
        self.req_hist.append(delta)
        window = {n: sum(h.get(n, 0) for h in list(self.req_hist)[-10:]) for n in APPS}
        for n in APPS:
            out.setdefault(n, {})
            out[n]["rps"] = delta.get(n, 0)
            out[n]["last10s"] = window[n]
        return out

    # ------------------------------------------------------------------ loops
    async def poll(self) -> None:
        prev_status: dict[str, str] = {}
        while True:
            t0 = time.time()
            try:
                a, b, hp, *apps = await asyncio.gather(self.shard("A"), self.shard("B"), self.haproxy(),
                                                       *(self.app(n) for n in APPS))
                for n in APPS:
                    st = hp.get(n, {}).get("status") if isinstance(hp, dict) else None
                    if st and prev_status.get(n) and st != prev_status[n]:
                        self.event("lb_status", node=n, old=prev_status[n], new=st)
                    if st:
                        prev_status[n] = st
                self.state = {"ts": time.time(), "shards": {"A": a, "B": b}, "haproxy": hp,
                              "apps": dict(zip(APPS, apps)), "redis": self.redis_ok,
                              "req_history": list(self.req_hist), "events": list(self.events)}
            except Exception as e:
                log.exception("poll failed: %s", e)
            await asyncio.sleep(max(0.1, 1.0 - (time.time() - t0)))

    async def listen(self) -> None:
        while True:
            try:
                ps = self.redis.pubsub()
                await ps.subscribe("auction:events")
                if not self.redis_ok:
                    self.event("redis", status="up")
                self.redis_ok = True
                while True:
                    msg = await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg is None:
                        await self.redis.ping()
                        continue
                    ev = json.loads(msg["data"])
                    if ev.get("type") == "item_created":
                        ev = {"type": "item_created", "node": ev.get("node"), "item_id": ev["item"]["id"],
                              "title": ev["item"]["title"], "ts": ev.get("ts")}
                    self.events.appendleft(ev)
            except Exception:
                if self.redis_ok:
                    self.event("redis", status="down")
                self.redis_ok = False
                await asyncio.sleep(1)


mon = Monitor()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    tasks = [asyncio.create_task(mon.poll()), asyncio.create_task(mon.listen())]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Auction monitor", lifespan=lifespan)


@app.get("/api/state")
async def state() -> dict[str, Any]:
    return mon.state


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "dashboard.html", headers={"Cache-Control": "no-store"})
