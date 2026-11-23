"""Load generator: concurrent readers + bidders through HAProxy, with per-second stats.

  python -m demo.loadgen --duration 30 --readers 8 --bidders 4

Classifies every response:
  ok        2xx
  rejected  4xx business rejection (bid too low, insufficient funds …) — system working
  failed    5xx / connection error / timeout                          — system unavailable
Bids carry an Idempotency-Key and are retried with the SAME key on failure [T4], so a
retry can never double-charge.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import httpx

from demo.common import LB_URL, banner, bold, cyan, dim, green, red, yellow


@dataclass
class Bucket:
    ok: int = 0
    rejected: int = 0
    failed: int = 0
    read_failed: int = 0
    read_ok: int = 0
    bids_ok: int = 0
    nodes: Counter = field(default_factory=Counter)      # X-Served-By
    db_reads: Counter = field(default_factory=Counter)   # X-DB-Node on GETs
    db_writes: Counter = field(default_factory=Counter)  # X-DB-Node on POSTs
    errors: Counter = field(default_factory=Counter)


class LoadGen:
    def __init__(self, base: str = LB_URL, readers: int = 6, bidders: int = 3,
                 item_id: int | None = None, think: float = 0.02, bid_users: list[int] | None = None) -> None:
        self.base, self.readers, self.bidders, self.think = base, readers, bidders, think
        self.item_id = item_id
        self.bid_users = bid_users or list(range(3, 11))
        self.t0 = 0.0
        self.buckets: dict[int, Bucket] = {}
        self.total = Bucket()
        self.accepted_bids: list[dict[str, Any]] = []   # every bid the server confirmed
        self.retries = 0
        self.printed = -1   # last per-second bucket printed by ticker()
        self.write_log: list[tuple[float, str]] = []   # (time, ok|rejected|failed) per bid attempt
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self.http = httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(5.0, connect=2.0),
                                      limits=httpx.Limits(max_connections=200, max_keepalive_connections=50))

    # ------------------------------------------------------------------ bookkeeping
    def _bucket(self) -> Bucket:
        sec = int(time.time() - self.t0)
        return self.buckets.setdefault(sec, Bucket())

    def _record(self, r: httpx.Response | None, write: bool, err: str | None = None) -> str:
        b = self._bucket()
        for x in (b, self.total):
            if r is None:
                x.failed += 1
                x.read_failed += 0 if write else 1
                x.errors[err or "error"] += 1
                continue
            node = r.headers.get("x-served-by", "?")
            if not write:
                if r.status_code < 400:
                    x.read_ok += 1
                elif r.status_code >= 500:
                    x.read_failed += 1
            if r.status_code < 400:
                x.ok += 1
                x.nodes[node] += 1
                if dbn := r.headers.get("x-db-node"):
                    (x.db_writes if write else x.db_reads)[dbn] += 1
            elif r.status_code < 500:
                x.rejected += 1
                x.nodes[node] += 1
            else:
                x.failed += 1
                x.errors[f"{r.status_code}"] += 1
        kind = "failed" if r is None else ("ok" if r.status_code < 400 else ("rejected" if r.status_code < 500 else "failed"))
        if write:
            self.write_log.append((time.time(), kind))
        return kind

    # ------------------------------------------------------------------ workers
    async def _reader(self) -> None:
        paths = ["/api/items", f"/api/items/{self.item_id}" if self.item_id else "/api/items", "/api/users"]
        while not self._stop.is_set():
            try:
                r = await self.http.get(random.choice(paths))
                self._record(r, write=False)
            except httpx.HTTPError as e:
                self._record(None, write=False, err=type(e).__name__)
            await asyncio.sleep(self.think)

    async def _bidder(self, idx: int) -> None:
        user = self.bid_users[idx % len(self.bid_users)]
        price: float | None = None
        while not self._stop.is_set():
            try:
                if price is None:
                    price = (await self.http.get(f"/api/items/{self.item_id}")).json()["current_price"]
                amount = round(price + random.choice([0.01, 0.02, 0.03, 0.05]), 2)
                key = str(uuid.uuid4())
                for attempt in range(6):  # retry with the SAME idempotency key
                    try:
                        r = await self.http.post(f"/api/items/{self.item_id}/bids", json={"amount": amount},
                                                 headers={"X-User-Id": str(user), "Idempotency-Key": key})
                        kind = self._record(r, write=True)
                    except httpx.HTTPError as e:
                        kind = self._record(None, write=True, err=type(e).__name__)
                        r = None
                    if kind != "failed":
                        break
                    self.retries += 1
                    await asyncio.sleep(0.25 * (attempt + 1))
                if r is not None and r.status_code == 200:
                    body = r.json()
                    # "duplicate" = an earlier attempt of this request committed (its response
                    # was lost) — still exactly one committed bid.
                    if body.get("status") in ("accepted", "duplicate"):
                        self._bucket().bids_ok += 1
                        self.total.bids_ok += 1
                        self.accepted_bids.append(body)
                    price = amount
                else:
                    price = None  # re-read the current price
            except httpx.HTTPError:
                price = None
                await asyncio.sleep(0.2)
            except Exception:
                price = None
                await asyncio.sleep(0.2)
            await asyncio.sleep(self.think)

    # ------------------------------------------------------------------ control
    async def ensure_item(self, duration_s: int = 3600) -> int:
        if self.item_id is None:
            r = await self.http.post("/api/items", json={"title": f"Load test {time.strftime('%H:%M:%S')}",
                                                         "start_price": 1, "duration_s": duration_s},
                                     headers={"X-User-Id": "1"})
            r.raise_for_status()
            self.item_id = r.json()["id"]
        return self.item_id

    async def start(self) -> None:
        if self.bidders:
            await self.ensure_item()
        self.t0 = time.time()
        self._tasks = [asyncio.create_task(self._reader()) for _ in range(self.readers)]
        self._tasks += [asyncio.create_task(self._bidder(i)) for i in range(self.bidders)]

    async def stop(self) -> None:
        self._stop.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.http.aclose()

    def elapsed(self) -> float:
        return time.time() - self.t0


def fmt_counter(c: Counter, colour: Any = cyan) -> str:
    return " ".join(f"{colour(k)}={v}" for k, v in sorted(c.items())) or dim("-")


def fmt_bucket(sec: int, b: Bucket) -> str:
    status = green(f"ok={b.ok:<4}") + f" rej={b.rejected:<3} " + (red(f"fail={b.failed:<3}") if b.failed else dim("fail=0  "))
    return (f"  t={sec:>3}s  {status} bids={b.bids_ok:<3} │ nodes: {fmt_counter(b.nodes)} │ "
            f"reads: {fmt_counter(b.db_reads, dim)} │ writes: {fmt_counter(b.db_writes, yellow)}")


async def ticker(lg: LoadGen, until: float, label: str = "") -> None:
    """Print one line per completed second until lg.elapsed() >= until."""
    while lg.elapsed() < until:
        await asyncio.sleep(0.25)
        done = int(lg.elapsed()) - 1
        for s in range(lg.printed + 1, done + 1):
            print(fmt_bucket(s, lg.buckets.get(s, Bucket())) + (f"  {dim(label)}" if label else ""))
            lg.printed = s


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--readers", type=int, default=6)
    ap.add_argument("--bidders", type=int, default=3)
    ap.add_argument("--item", type=int, default=None)
    a = ap.parse_args()
    banner(f"Load: {a.readers} readers + {a.bidders} bidders for {a.duration:.0f}s via {LB_URL}")
    lg = LoadGen(readers=a.readers, bidders=a.bidders, item_id=a.item)
    await lg.start()
    print(f"  bidding on item #{lg.item_id}")
    await ticker(lg, a.duration)
    await lg.stop()
    t = lg.total
    print(bold(f"\n  total ok={t.ok} rejected={t.rejected} failed={t.failed} accepted_bids={t.bids_ok} retries={lg.retries}"))
    print(f"  per node: {fmt_counter(t.nodes)}\n  reads by DB node: {fmt_counter(t.db_reads)}\n  writes by DB node: {fmt_counter(t.db_writes)}")
    if t.errors:
        print(f"  errors: {dict(t.errors)}")


if __name__ == "__main__":
    asyncio.run(main())
