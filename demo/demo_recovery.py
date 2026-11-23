"""[T5] Recovery demo.

 1. Coordinator crash recovery (2PC in-doubt transactions)
    1a  app3 dies (kill -9) after both shards PREPARED, before the decision
        -> Docker restarts it; startup recovery finds them in pg_prepared_xacts, log says
           PREPARED -> presumed abort -> ROLLBACK PREPARED on both shards
    1b  app3 dies right after logging COMMITTED, before phase 2
        -> startup recovery COMMITs both; the client's retry (same Idempotency-Key)
           gets the original bid back: exactly-once
    1c  app3 is SIGKILLed from outside in the middle of phase 2 and stays down
        -> the item stays locked by the prepared txn (2PC blocking: a concurrent bid waits)
           until ANOTHER node's resolver commits it from the shared log
 2. DB crash recovery: kill -9 every PostgreSQL process of the Shard A primary under write
    load -> WAL replay (redo) on restart, no committed bid lost
 3. Point-in-time recovery: base backup + continuous WAL archive; accidental
    `DELETE FROM bids`; restore a sandbox to just before it and put the rows back
 4. Redis loss: Redis restarts empty -> live board rebuilt from the database
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import time
import uuid

import asyncpg
import httpx

from demo import invariants
from demo.common import (LB_URL, Checks, banner, bold, container, dexec, dim, info, is_running, kill,
                         patroni_cluster, psql, start, step, wait_for, yellow)
from demo.db import connect
from demo.loadgen import LoadGen, ticker

PW = os.environ.get("PG_SUPERUSER_PASSWORD", "postgres_pw")


def leader(shard: str) -> str:
    return next(m["name"] for m in patroni_cluster(shard)["members"] if m["role"] == "leader")


async def prepared() -> tuple[list[str], list[str]]:
    a, b = await connect("A"), await connect("B")
    try:
        q = "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'auc:%' ORDER BY 1"
        return [r["gid"] for r in await a.fetch(q)], [r["gid"] for r in await b.fetch(q)]
    finally:
        await a.close()
        await b.close()


async def last_txn(node: str) -> dict:
    a = await connect("A")
    try:
        return dict(await a.fetchrow("SELECT txn_id::text, state, completed FROM coord.txn_log WHERE coordinator = $1"
                                     " ORDER BY ts DESC LIMIT 1", node))
    finally:
        await a.close()


async def txn(txn_id: str) -> dict:
    a = await connect("A")
    try:
        return dict(await a.fetchrow("SELECT state, completed FROM coord.txn_log WHERE txn_id = $1::uuid", txn_id))
    finally:
        await a.close()


async def item_state(item: int) -> str:
    a = await connect("A")
    try:
        r = await a.fetchrow("SELECT current_price, highest_bidder, (SELECT count(*) FROM bids WHERE item_id = $1) n"
                             " FROM items WHERE id = $1", item)
        return f"price=${r['current_price']} top=user {r['highest_bidder']} bids={r['n']}"
    finally:
        await a.close()


def app_logs(node: str, since: float, *needles: str) -> list[str]:
    r = subprocess.run(["docker", "logs", "--since", f"{since:.0f}", container(node)], capture_output=True, text=True)
    return [l for l in (r.stdout + r.stderr).splitlines() if any(n in l for n in needles)]


async def wait_healthy(node: str, timeout: float = 30) -> float:
    def ok() -> bool:
        try:
            return httpx.get(f"http://{node}:8000/readyz", timeout=0.5).status_code == 200
        except httpx.HTTPError:
            return False
    return await asyncio.to_thread(wait_for, ok, timeout, 0.1, f"{node} ready")


# ============================================================================ 1
async def coordinator_crashes(http: httpx.AsyncClient, c: Checks) -> None:
    item = (await http.post("/api/items", json={"title": "Recovery demo", "start_price": 10, "duration_s": 900},
                            headers={"X-User-Id": "1"})).json()["id"]
    await http.post(f"/api/items/{item}/bids", json={"amount": 20}, headers={"X-User-Id": "3"})

    step("1a. kill -9 of the coordinator AFTER both PREPAREs, BEFORE the decision")
    await http.post("http://app3:8000/admin/faults", json={"name": "crash_before_decision"})
    t0 = time.time()
    try:
        await http.post(f"http://app3:8000/api/items/{item}/bids", json={"amount": 30},
                        headers={"X-User-Id": "4", "Idempotency-Key": str(uuid.uuid4())})
        info("unexpected: request completed")
    except httpx.HTTPError as e:
        info(f"client: request to app3 failed ({type(e).__name__}) — the coordinator process died")
    pa, pb = await prepared()
    t = await last_txn("app3")
    info(f"immediately after: txn {t['txn_id'][:8]} log={bold(t['state'])}; prepared on A={len(pa)} B={len(pb)}")
    c.check(t["state"] == "PREPARED" and len(pa) == 1 and len(pb) == 1, "in doubt: both shards hold a prepared txn, no decision logged")
    took = await wait_healthy("app3")
    await asyncio.sleep(0.5)
    for l in app_logs("app3", t0, "resolved in-doubt", "startup recovery"):
        info(dim("  app3 " + l.split(" app3 ", 1)[-1]))
    t = await txn(t["txn_id"])
    pa, pb = await prepared()
    info(f"app3 back after {took:.1f}s → log={bold(t['state'])}; prepared A={len(pa)} B={len(pb)}; item {await item_state(item)}")
    c.check(t["state"] == "ABORTED" and not pa and not pb, "presumed abort: ROLLBACK PREPARED on both shards")

    step("1b. kill -9 of the coordinator AFTER logging COMMITTED, BEFORE phase 2")
    await http.post("http://app3:8000/admin/faults", json={"name": "crash_after_decision"})
    key, t0 = str(uuid.uuid4()), time.time()
    try:
        await http.post(f"http://app3:8000/api/items/{item}/bids", json={"amount": 40},
                        headers={"X-User-Id": "5", "Idempotency-Key": key})
    except httpx.HTTPError as e:
        info(f"client: request to app3 failed ({type(e).__name__}) — no response, outcome unknown to the client")
    pa, pb = await prepared()
    t = await last_txn("app3")
    info(f"immediately after: txn {t['txn_id'][:8]} log={bold(t['state'])}; prepared on A={len(pa)} B={len(pb)}")
    c.check(t["state"] == "COMMITTED" and len(pa) == 1 and len(pb) == 1, "decision durable (COMMITTED) but phase 2 never ran")
    await wait_healthy("app3")
    await asyncio.sleep(0.5)
    for l in app_logs("app3", t0, "resolved in-doubt"):
        info(dim("  app3 " + l.split(" app3 ", 1)[-1]))
    pa, pb = await prepared()
    info(f"after restart: prepared A={len(pa)} B={len(pb)}; item {await item_state(item)}")
    r = await http.post(f"/api/items/{item}/bids", json={"amount": 40}, headers={"X-User-Id": "5", "Idempotency-Key": key})
    info(f"client retries the same request via HAProxy → {r.status_code} status={bold(r.json()['status'])} (served by {r.headers['x-served-by']})")
    c.check(not pa and not pb and r.json()["status"] == "duplicate", "recovery COMMITTED both shards; retry is exactly-once")

    step("1c. SIGKILL app3 from outside mid-phase-2 (it stays down) → another node recovers")
    await http.post("http://app3:8000/admin/faults", json={"name": "delay_before_phase2", "arg": 30})
    task = asyncio.create_task(http.post(f"http://app3:8000/api/items/{item}/bids", json={"amount": 50},
                                         headers={"X-User-Id": "6", "Idempotency-Key": str(uuid.uuid4())}))
    await asyncio.sleep(1.0)
    kill("app3")
    t_kill = time.time()
    try:
        await task
    except httpx.HTTPError:
        pass
    t = await last_txn("app3")
    pa, pb = await prepared()
    info(f"app3 SIGKILLed; txn {t['txn_id'][:8]} log={t['state']}; prepared A={len(pa)} B={len(pb)}")
    info(yellow("2PC blocking problem: the prepared txn holds the item's row lock — a new bid has to wait:"))
    t_bid = time.time()
    r = await http.post(f"http://app1:8000/api/items/{item}/bids", json={"amount": 60},
                        headers={"X-User-Id": "7", "Idempotency-Key": str(uuid.uuid4())})
    waited = time.time() - t_bid
    info(f"bid $60 via app1 → {r.status_code} {r.json().get('status') or r.json().get('error')} after waiting {bold(f'{waited:.1f}s')}")
    found = []
    for n in ("app1", "app2"):
        found += [f"{n} " + l.split(f" {n} ", 1)[-1] for l in app_logs(n, t_kill, "resolved in-doubt")]
    for l in found:
        info(dim("  " + l))
    pa, pb = await prepared()
    c.check(waited > 2 and r.status_code == 200 and found and not pa and not pb,
            "blocked until a surviving node's resolver COMMITTED the orphan, then the new bid went through")
    info(f"item {await item_state(item)}")
    start("app3")
    await wait_healthy("app3")


# ============================================================================ 2
async def db_crash(c: Checks) -> None:
    p = leader("A")
    step(f"2. DB crash recovery — kill -9 every PostgreSQL process on the Shard A primary ({p}) under load")
    lg = LoadGen(readers=2, bidders=3, think=0.01)
    await lg.start()
    await ticker(lg, 3)
    lsn = psql(p, "auction", "SELECT pg_current_wal_lsn()").strip()
    t_kill = time.time()
    # SIGKILL every process named "postgres" (postmaster + all backends) — like a power cut
    # for the database server process; Patroni keeps running and restarts it.
    dexec(p, "sh", "-c", 'for d in /proc/[0-9]*; do [ "$(cat $d/comm 2>/dev/null)" = postgres ] '
                         '&& kill -9 "${d#/proc/}"; done; true')
    info(f"SIGKILL sent to all postgres processes on {p} at WAL {lsn} (Patroni itself keeps running)")
    k = int(lg.elapsed())
    await ticker(lg, k + 12, "postgres killed")
    await lg.stop()
    r = subprocess.run(["docker", "logs", "--since", f"{t_kill:.0f}", container(p)], capture_output=True, text=True)
    keys = ("was interrupted", "not properly shut down", "redo starts", "redo done", "crash recovery",
            "ready to accept", "starting as a secondary", "promoted")
    for l in [l for l in (r.stdout + r.stderr).splitlines() if any(x in l for x in keys)][:8]:
        info(dim("  " + l.split("| ", 1)[-1][:150]))
    a = await connect("A")
    stored = {x["id"] for x in await a.fetch("SELECT id FROM bids WHERE item_id = $1", lg.item_id)}
    await a.close()
    confirmed = {b["bid_id"] for b in lg.accepted_bids}
    info(f"primary now: {leader('A')}; confirmed bids={len(confirmed)} stored={len(stored)}; "
         f"failed requests during the crash={lg.total.failed}")
    c.check(confirmed <= stored, "WAL replay recovered every committed bid (missing 0)")


# ============================================================================ 3
SANDBOX = "auction-pitr"


async def restore_sandbox(backup: str, target: object) -> tuple[asyncpg.Connection, list[str]]:
    """Start a throw-away PostgreSQL from base backup `backup` and replay archived WAL up to
    `target` (recovery_target_time), then promote it. Returns a connection + key log lines."""
    subprocess.run(["docker", "rm", "-f", SANDBOX], capture_output=True)
    ts = target.isoformat() if hasattr(target, "isoformat") else str(target)
    # Patroni's postgresql.conf carries its own (empty) recovery_* settings; PostgreSQL treats
    # them as conflicting targets, so strip them and append ours.
    script = f"""set -e
mkdir -p /pitr && tar -xzf /backups/{backup}/base.tar.gz -C /pitr && tar -xzf /backups/{backup}/pg_wal.tar.gz -C /pitr/pg_wal
sed -i -E '/^(recovery_target|primary_conninfo|primary_slot_name|restore_command|synchronous_standby_names|archive_mode)/d' /pitr/postgresql.conf
rm -f /pitr/standby.signal /pitr/postgresql.auto.conf
cat >> /pitr/postgresql.conf <<CONF
listen_addresses = '*'
hba_file = '/pitr/pg_hba.conf'
ident_file = '/pitr/pg_ident.conf'
archive_mode = off
synchronous_standby_names = ''
hot_standby = on
restore_command = 'cp /wal_archive/%f %p'
recovery_target_time = '{ts}'
recovery_target_action = 'promote'
recovery_target_timeline = 'latest'
CONF
chown -R postgres:postgres /pitr && chmod 700 /pitr && touch /pitr/recovery.signal
exec gosu postgres postgres -D /pitr"""
    subprocess.run(["docker", "run", "-d", "--name", SANDBOX, "--network", "auction_default",
                    "-v", "auction_backups:/backups:ro", "-v", "auction_wal-archive-a:/wal_archive:ro",
                    "--entrypoint", "bash", "auction/patroni:16", "-c", script], check=True, capture_output=True)

    def logs() -> str:
        out = subprocess.run(["docker", "logs", SANDBOX], capture_output=True, text=True)
        return out.stdout + out.stderr

    def done() -> bool:
        text = logs()
        return "database system is ready to accept connections" in text or "FATAL" in text
    await asyncio.to_thread(wait_for, done, 90, 0.5, "sandbox recovery")
    if "ready to accept connections" not in logs():
        raise RuntimeError("sandbox failed:\n" + logs()[-800:])
    keys = ("starting point-in-time", "redo starts", "recovery stopping", "selected new timeline",
            "archive recovery complete", "ready to accept")
    lines = [l.split("LOG:", 1)[-1].strip()[:140] for l in logs().splitlines() if any(k in l for k in keys)]
    for _ in range(20):
        try:
            return await asyncpg.connect(host=SANDBOX, user="postgres", password=PW, database="auction", timeout=5), lines
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError("cannot connect to sandbox")


async def pitr(c: Checks) -> None:
    p = leader("A")
    step("3. Point-in-time recovery — base backup + WAL archive")
    name = f"a-{time.strftime('%Y%m%d-%H%M%S')}"
    t0 = time.time()
    dexec(p, "bash", "-c", f"PGPASSWORD=replicator_pw pg_basebackup -h {p} -U replicator -D /backups/{name} -Ft -z -X stream -c fast")
    info(f"base backup /backups/{name} taken from {p} in {time.time() - t0:.1f}s "
         f"({dexec(p, 'du', '-sh', f'/backups/{name}').split()[0]})")

    a = await connect("A")
    for i in range(3):  # a few more bids after the backup: they must come back from the WAL archive
        it = (await a.fetchval("SELECT id FROM items WHERE status = 'open' ORDER BY id LIMIT 1"))
        async with httpx.AsyncClient(base_url=LB_URL) as http:
            cur = (await http.get(f"/api/items/{it}")).json()["current_price"]
            await http.post(f"/api/items/{it}/bids", json={"amount": cur + 1 + i},
                            headers={"X-User-Id": str(3 + i), "Idempotency-Key": str(uuid.uuid4())})
    n_before = await a.fetchval("SELECT count(*) FROM bids")
    await asyncio.sleep(1.0)
    target = await a.fetchval("SELECT now()")
    await asyncio.sleep(1.0)
    info(f"bids before the accident: {bold(str(n_before))}   recovery target time: {target}")
    deleted = await a.execute("DELETE FROM bids")
    info(f"💥 accident: {bold('DELETE FROM bids')} → {deleted}; bids now: {await a.fetchval('SELECT count(*) FROM bids')}")
    seg = psql(p, "auction", "SELECT pg_walfile_name(pg_current_wal_lsn())").strip()
    psql(p, "auction", "SELECT pg_switch_wal()")
    await asyncio.to_thread(wait_for, lambda: psql(p, "auction", "SELECT last_archived_wal FROM pg_stat_archiver").strip() >= seg,
                            30, 0.5, "WAL archived")
    info(f"WAL up to segment {seg} archived to /wal_archive")

    step("   restore a sandbox instance to the target time")
    t0 = time.time()
    sb, log_lines = await restore_sandbox(name, target)
    for l in log_lines:
        info(dim("  sandbox " + l))
    rows = await sb.fetch("SELECT id, item_id, user_id, amount, ts, txn_id, idempotency_key, served_by FROM bids")
    info(f"sandbox recovered in {time.time() - t0:.1f}s: {bold(str(len(rows)))} bids as of {target}")
    c.check(len(rows) == n_before, f"PITR restored the exact pre-accident state ({len(rows)} == {n_before})")

    step("   copy the lost rows back into the live primary")
    res = await a.copy_records_to_table("bids", records=[tuple(r) for r in rows],
                                        columns=["id", "item_id", "user_id", "amount", "ts", "txn_id", "idempotency_key", "served_by"])
    n_after = await a.fetchval("SELECT count(*) FROM bids")
    info(f"{res}; bids on live primary: {n_after}")
    c.check(n_after == n_before, "live data repaired")
    await sb.close()
    await a.close()
    subprocess.run(["docker", "rm", "-f", SANDBOX], capture_output=True)


# ============================================================================ 4
async def redis_loss(http: httpx.AsyncClient, c: Checks) -> None:
    step("4. Redis loss — the live board is rebuilt from the database")
    before = (await http.get("/api/live")).json()
    info(f"before: /api/live source={before['source']} items={len(before['items'])}")
    kill("redis")
    await asyncio.sleep(1.5)
    mid = (await http.get("/api/live")).json()
    info(f"Redis killed: /api/live source={bold(mid['source'])} items={len(mid['items'])} (served from DB)")
    t0 = time.time()
    start("redis")
    await asyncio.to_thread(wait_for, lambda: httpx.get(f"{LB_URL}/api/live", timeout=1).json()["source"] == "redis",
                            30, 0.3, "redis board")
    after = (await http.get("/api/live")).json()
    info(f"Redis restarted EMPTY (no persistence); {after['source']} board back with {len(after['items'])} items "
         f"{time.time() - t0:.1f}s later")
    c.check(after["source"] == "redis" and after["items"] == mid["items"], "rebuilt board == database state")


async def main() -> None:
    banner("DEMO [T5] Recovery — coordinator crash, DB crash, PITR, Redis loss")
    c = Checks()
    async with httpx.AsyncClient(base_url=LB_URL, timeout=60) as http:
        await coordinator_crashes(http, c)
        await db_crash(c)
        await pitr(c)
        await redis_loss(http, c)
    for s in ("app3", "redis"):
        c.check(is_running(s), f"{s} running")
    step("Invariants")
    await invariants.run(c)
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
