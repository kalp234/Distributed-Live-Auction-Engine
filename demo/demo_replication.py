"""[T1] Replication demo (Shard A: primary + sync replica + async replica).

1. Topology + pg_stat_replication.
2. Writes on the primary appear on every replica.
3. Sync vs async: make the ASYNC replica a delayed replica (recovery_min_apply_delay) -> it
   serves stale data while the SYNC replica (synchronous_commit=remote_apply) is always up to
   date; commits don't wait for the async one.
4. Read-your-writes: reads without a session token can hit the stale replica; reads with
   X-Min-LSN never do (they fall back to an up-to-date node).
5. The price of sync replication: delay apply on the SYNC replica by 500 ms -> every
   synchronous commit waits; one 2PC bid makes ~6 of them on Shard A, so the cost compounds.
(WAL replay *pause* can't be used: Patroni auto-resumes paused replay within loop_wait.)
"""
from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter

import httpx

from demo.common import (wait_cluster_ready, LB_URL, Checks, banner, bold, cyan, dim, green, info, patroni_cluster,
                         print_cluster, psql, red, set_apply_delay, step, yellow)
from demo.db import connect


def roles() -> tuple[str, str, str]:
    c = patroni_cluster("A")
    assert c, "Shard A unreachable"
    by = {m["role"]: m["name"] for m in c["members"]}
    return by["leader"], by["sync_standby"], by["replica"]


async def node_view(member: str, item_id: int) -> tuple[int, float, str]:
    conn = await connect("A", member=member)
    try:
        r = await conn.fetchrow(
            "SELECT count(*) n, coalesce(max(amount), 0)::float8 top,"
            " (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()"
            "       ELSE pg_current_wal_lsn() END)::text lsn"
            " FROM bids WHERE item_id = $1", item_id)
        return r["n"], r["top"], r["lsn"]
    finally:
        await conn.close()


async def show_nodes(item_id: int, labels: dict[str, str]) -> dict[str, tuple[int, float, str]]:
    out = {}
    for m, label in labels.items():
        out[m] = await node_view(m, item_id)
        n, top, lsn = out[m]
        info(f"{m:6} {label:13} bids={n:<3} highest=${top:<8.2f} lsn={lsn}")
    return out


def show_lag(http: httpx.Client) -> dict[str, dict]:
    reps = {r["name"]: r for r in http.get("/api/replication").json()["shards"]["A"]["replicas"]}
    for name, r in sorted(reps.items()):
        col = cyan if r["sync_state"] == "sync" else dim
        info(f"{name:6} {col(r['sync_state']):5}  sent_lag={r['sent_lag_bytes']}B  flush_lag={r['flush_lag_bytes']}B  "
             f"replay_lag={bold(str(r['replay_lag_bytes']) + 'B')}  replay_lag_ms={r['replay_lag_ms']}")
    return reps


def bid(http: httpx.Client, item: int, user: int, amount: float) -> tuple[httpx.Response, float]:
    t = time.time()
    r = http.post(f"/api/items/{item}/bids", json={"amount": amount},
                  headers={"X-User-Id": str(user), "Idempotency-Key": str(uuid.uuid4())})
    return r, (time.time() - t) * 1000


async def main() -> None:
    banner("DEMO [T1] Replication")
    c = Checks()
    wait_cluster_ready()
    primary, sync, asyn = roles()
    labels = {primary: "primary", sync: "sync replica", asyn: "async replica"}
    http = httpx.Client(base_url=LB_URL, timeout=30)

    step("1. Shard A topology (Patroni) and pg_stat_replication on the primary")
    print_cluster("A")
    info(f"pg_stat_replication on {primary}: " + psql(primary, "auction",
         "SELECT string_agg(application_name||'='||sync_state, ', ' ORDER BY 1) FROM pg_stat_replication").strip())
    info(f"synchronous_commit = {psql(primary, 'auction', 'SHOW synchronous_commit').strip()}  "
         f"synchronous_standby_names = {psql(primary, 'auction', 'SHOW synchronous_standby_names').strip()}")

    step("2. Write on the primary → visible on every replica")
    item = http.post("/api/items", json={"title": "Replication demo", "start_price": 10, "duration_s": 900},
                     headers={"X-User-Id": "1"}).json()["id"]
    for i, amt in enumerate([11, 12, 13]):
        r, ms = bid(http, item, 3 + i, amt)
        info(f"bid ${amt} → {r.status_code} via {r.headers['x-served-by']} on DB {r.headers['x-db-node']} ({ms:.0f} ms)")
    await asyncio.sleep(0.3)
    v = await show_nodes(item, labels)
    c.check(len({(n, t) for n, t, _ in v.values()}) == 1, "all 3 nodes hold identical bid data")

    step(f"3. Sync vs async — make the async replica ({asyn}) lag: recovery_min_apply_delay = 1h")
    set_apply_delay(asyn, "1h")
    try:
        info(f"{asyn}: recovery_min_apply_delay = {psql(asyn, 'auction', 'SHOW recovery_min_apply_delay').strip()}")
        lat = []
        for i, amt in enumerate([20, 21, 22, 23, 24]):
            r, ms = bid(http, item, 3 + i % 4, amt)
            lat.append(ms)
        info(f"5 more bids committed; commit latency avg {sum(lat)/len(lat):.0f} ms (async replica does not slow commits)")
        v = await show_nodes(item, labels)
        c.check(v[sync][1] == 24 and v[primary][1] == 24, f"sync replica is current (remote_apply): highest=${v[sync][1]:.0f}")
        c.check(v[asyn][1] == 13, f"async replica is stale while lagging: highest=${v[asyn][1]:.0f}")
        info(yellow("replication lag as seen by the primary (WAL is received but not yet applied):"))
        reps = show_lag(http)
        c.check(reps[asyn]["replay_lag_bytes"] > 0 and reps[asyn]["flush_lag_bytes"] == 0,
                "async: flush lag 0 B but replay lag > 0 B")

        step("4. Read-your-writes with X-Min-LSN (async replica still lagging)")
        r, _ = bid(http, item, 7, 30)
        token = r.headers["x-write-lsn"]
        info(f"bid $30 accepted; response X-Write-LSN = {bold(token)}")
        for label, headers in (("WITHOUT session token", {}), ("WITH X-Min-LSN", {"X-Min-LSN": token})):
            served, stale, fallback = Counter(), 0, Counter()
            for _ in range(30):
                g = http.get(f"/api/items/{item}", headers=headers)
                served[g.headers["x-db-node"]] += 1
                if g.json()["current_price"] != 30:
                    stale += 1
                if fb := g.headers.get("x-read-fallback"):
                    fallback[fb] += 1
            col = red if stale else green
            info(f"{label:22} 30 reads → served by {dict(served)}  {col(f'stale reads = {stale}')}"
                 + (f"  fallbacks: {dict(fallback)}" if fallback else ""))
            if headers:
                c.check(stale == 0, "with X-Min-LSN no read returns stale data")
            else:
                c.check(stale > 0, "without a token some reads hit the stale async replica")
    finally:
        set_apply_delay(asyn, None)

    step(f"Remove the delay on {asyn}")
    await asyncio.sleep(0.5)
    v = await show_nodes(item, labels)
    c.check(len({t for _, t, _ in v.values()}) == 1, "async replica caught up as soon as the delay was removed")

    step(f"5. The cost of synchronous replication — delay apply on the SYNC replica ({sync}) by 500 ms")
    _, base_ms = bid(http, item, 8, 35)
    set_apply_delay(sync, "500ms")
    try:
        r, ms = bid(http, item, 8, 40)
        info(f"bid $40 → {r.status_code} after {bold(f'{ms:.0f} ms')} (vs {base_ms:.0f} ms without the delay) "
             f"≈ {ms / 500:.0f} synchronous commits on Shard A, each waiting for the sync replica to apply")
        info(dim("(txn_log INIT, PREPARE TRANSACTION, PREPARED, COMMITTED, COMMIT PREPARED, completed)"))
        c.check(r.status_code == 200 and ms >= 1500,
                "every Shard A commit waited until the sync replica had applied it (synchronous_commit=remote_apply)")
    finally:
        set_apply_delay(sync, None)
    info(dim("Trade-off (CAP): sync = no lost commits on failover + fresh reads, paid with write latency and"))
    info(dim("availability coupling to one replica; async = fast writes, but may lose the tail / serve stale reads."))
    http.close()
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
