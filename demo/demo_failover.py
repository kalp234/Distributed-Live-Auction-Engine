"""[T3] Availability demo: automatic primary failover under continuous bid load.

For each shard (A = auction, B = wallet):
  1. continuous bids + reads through HAProxy, success/fail printed per second
  2. SIGKILL the current primary's container (whole "machine" dies)
  3. Patroni: the leader lease expires (ttl=10 s) -> the SYNC standby is promoted
     (synchronous_mode -> no acknowledged commit can be lost); apps follow within 1 s
  4. measure write downtime = last committed bid before .. first committed bid after
  5. split-brain probe: every 0.25 s ask every member pg_is_in_recovery(); at most one
     writable primary may ever exist
  6. restart the old primary -> it rejoins as a replica via pg_rewind on the new timeline
  7. every bid confirmed to a client is on the new primary; invariants hold

  python -m demo.demo_failover          # both shards
  python -m demo.demo_failover A        # one shard
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import asyncpg

from demo import invariants
from demo.common import (Checks, banner, bold, dexec, dim, docker, green, info, kill, patroni_cluster,
                         print_cluster, red, start, step, wait_for, yellow, container)
from demo.loadgen import Bucket, LoadGen, ticker

MEMBERS = {"A": ["pg-a1", "pg-a2", "pg-a3"], "B": ["pg-b1", "pg-b2"]}
DB = {"A": "auction", "B": "wallet"}
TARGET_S = 15.0


class SplitBrainProbe:
    """Samples every member directly: which ones accept writes (not in recovery)?"""

    def __init__(self, shard: str) -> None:
        self.shard = shard
        self.conns: dict[str, asyncpg.Connection | None] = {m: None for m in MEMBERS[shard]}
        self.max_primaries = 0
        self.samples = 0
        self.timeline: list[tuple[float, list[str]]] = []

    async def _is_primary(self, m: str) -> bool | None:
        try:
            if self.conns[m] is None or self.conns[m].is_closed():
                self.conns[m] = await asyncpg.connect(host=m, user="postgres", database=DB[self.shard], timeout=0.5,
                                                      password=os.environ.get("PG_SUPERUSER_PASSWORD", "postgres_pw"))
            return not await self.conns[m].fetchval("SELECT pg_is_in_recovery()", timeout=0.5)
        except Exception:
            self.conns[m] = None
            return None

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            res = await asyncio.gather(*(self._is_primary(m) for m in MEMBERS[self.shard]))
            prim = [m for m, r in zip(MEMBERS[self.shard], res) if r]
            self.samples += 1
            self.max_primaries = max(self.max_primaries, len(prim))
            if not self.timeline or self.timeline[-1][1] != prim:
                self.timeline.append((time.time(), prim))
            await asyncio.sleep(0.25)
        for c in self.conns.values():
            if c is not None:
                c.terminate()


def downtime(lg: LoadGen, t_kill: float) -> tuple[float, float, float] | None:
    """(last ok before outage, first ok after, window) for bids, or None if no outage."""
    log = sorted(lg.write_log)
    first_fail = next((t for t, k in log if t >= t_kill and k == "failed"), None)
    if first_fail is None:
        return None
    last_ok = max((t for t, k in log if t <= first_fail and k == "ok"), default=t_kill)
    first_ok = next((t for t, k in log if t > first_fail and k == "ok"), None)
    if first_ok is None:
        return None
    return last_ok, first_ok, first_ok - last_ok


async def failover(shard: str, c: Checks) -> None:
    step(f"Shard {shard} ({DB[shard]} DB) — before")
    print_cluster(shard)
    cl = patroni_cluster(shard)
    old = next(m["name"] for m in cl["members"] if m["role"] == "leader")
    sync = next((m["name"] for m in cl["members"] if m["role"] == "sync_standby"), None)
    tl0 = next(m["timeline"] for m in cl["members"] if m["role"] == "leader")
    info(f"primary={bold(old)}  sync standby={sync}  timeline={tl0}")

    lg = LoadGen(readers=4, bidders=4, think=0.01)
    await lg.start()
    stop = asyncio.Event()
    probe = SplitBrainProbe(shard)
    ptask = asyncio.create_task(probe.run(stop))
    step(f"Continuous load on item #{lg.item_id} (4 bidders + 4 readers via HAProxy)")
    await ticker(lg, 3)

    step(f"SIGKILL primary {old}  (docker kill — the whole DB server disappears)")
    t_kill = time.time()
    kill(old)
    k = int(lg.elapsed())
    t_new: list[float] = []

    def new_leader() -> bool:
        l = next((m["name"] for m in (patroni_cluster(shard) or {"members": []})["members"]
                  if m["role"] == "leader" and m["state"] == "running"), None)
        if l and l != old:
            t_new.append(time.time())
            return True
        return False

    elect = asyncio.create_task(asyncio.to_thread(wait_for, new_leader, 40, 0.2, "new leader"))
    await ticker(lg, k + 18, f"{old} killed")
    await elect
    new = next(m["name"] for m in patroni_cluster(shard)["members"] if m["role"] == "leader")
    info(f"Patroni promoted {bold(new)} {t_new[0] - t_kill:.1f}s after the kill "
         f"({'the former sync standby' if new == sync else 'NOT the sync standby!'})")
    c.check(new == sync, "the synchronous standby was promoted (zero data loss guaranteed)")

    dt = downtime(lg, t_kill)
    if dt:
        last_ok, first_ok, win = dt
        col = green if win < TARGET_S else red
        info(bold(col(f"WRITE DOWNTIME: {win:.1f}s")) + f"  (last commit {last_ok - t_kill:+.1f}s, "
             f"first commit after failover {first_ok - t_kill:+.1f}s relative to the kill)")
        c.check(win < TARGET_S, f"write downtime {win:.1f}s < {TARGET_S:.0f}s target")
    else:
        info("no failed writes observed")
    win_buckets = [lg.buckets.get(s, Bucket()) for s in range(k, k + 15)]
    by_node = Bucket().db_reads
    for b in win_buckets:
        by_node.update(b.db_reads)
    rok, rfail = sum(b.read_ok for b in win_buckets), sum(b.read_failed for b in win_buckets)
    info(f"reads during the 15 s after the kill: ok={rok} failed={rfail}  served by {dict(by_node)}")
    c.check(all(b.read_ok > 0 for b in win_buckets) and rfail <= rok // 100,
            "reads stayed available every second of the failover (served by replicas)")

    step(f"Restart old primary {old} → must rejoin as a replica (pg_rewind), not as a second primary")
    start(old)
    await asyncio.to_thread(wait_for, lambda: any(
        m["name"] == old and m["state"] == "streaming" for m in (patroni_cluster(shard) or {"members": []})["members"]),
        90, 0.5, f"{old} streaming")
    await ticker(lg, int(lg.elapsed()) + 3, f"{old} rejoined")
    stop.set()
    await ptask
    await lg.stop()
    print_cluster(shard)
    tl1 = next(m["timeline"] for m in patroni_cluster(shard)["members"] if m["role"] == "leader")
    r = __import__("subprocess").run(["docker", "logs", "--since", str(int(t_kill)), container(old)],
                                     capture_output=True, text=True)
    lines = [l for l in (r.stdout + r.stderr).splitlines()
             if any(k in l for k in ("pg_rewind exit", "servers diverged", "starting as a secondary", "no rewind"))]
    info(f"timeline {tl0} → {tl1};  {old} rejoin log:")
    for l in lines[:4]:
        info(dim("    " + l.split("INFO:", 1)[-1].strip()))
    hist = dexec(new, "patronictl", "-c", "/etc/patroni/patroni.yml", "history").strip().splitlines()
    info("patronictl history (last entry): " + hist[-2].strip() if len(hist) >= 2 else "")
    c.check(tl1 == tl0 + 1, "new timeline after promotion")
    c.check(any(m["name"] == old and m["role"] in ("replica", "sync_standby") for m in patroni_cluster(shard)["members"]),
            f"{old} is back as a replica")

    info(f"split-brain probe: {probe.samples} samples; writable primaries over time: " +
         " → ".join(f"{p or ['none']}" for _, p in probe.timeline))
    c.check(probe.max_primaries <= 1, f"never more than one writable primary (max seen: {probe.max_primaries})")

    a = await asyncpg.connect(host=new if shard == "A" else patroni_leader("A"), user="auction_app",
                              password="auction_pw", database="auction")
    stored = {r["id"] for r in await a.fetch("SELECT id FROM bids WHERE item_id = $1", lg.item_id)}
    await a.close()
    confirmed = {b["bid_id"] for b in lg.accepted_bids}
    t = lg.total
    info(f"bids: confirmed to clients={len(confirmed)} stored={len(stored)}  │ requests ok={t.ok} rejected={t.rejected} "
         f"failed={t.failed} (client retries={lg.retries})")
    c.check(confirmed <= stored, f"no confirmed bid lost across the failover (missing {len(confirmed - stored)})")


def patroni_leader(shard: str) -> str:
    return next(m["name"] for m in patroni_cluster(shard)["members"] if m["role"] == "leader")


async def main() -> None:
    shards = [s for s in sys.argv[1:] if s in ("A", "B")] or ["A", "B"]
    banner(f"DEMO [T3] Availability — automatic failover of shard {' and '.join(shards)}")
    info(yellow(f"Patroni: ttl=10s loop_wait=2s; apps poll topology every 1s; target downtime < {TARGET_S:.0f}s"))
    c = Checks()
    for s in shards:
        await failover(s, c)
    step("Invariants after failover")
    await invariants.run(c)
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
