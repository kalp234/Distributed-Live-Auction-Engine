"""[T5] Coordinator recovery: resolve in-doubt 2PC transactions.

A transaction is *in doubt* when a participant holds a prepared transaction (visible in
pg_prepared_xacts) but its coordinator died or lost contact before finishing phase 2.
Prepared transactions survive participant crashes and failovers (they are WAL-logged and
sync-replicated), and keep their row locks until resolved — the 2PC blocking problem.

Resolution follows the durable coordinator log (coord.txn_log):
    COMMITTED                 -> COMMIT PREPARED on every participant still holding it
    ABORTED                   -> ROLLBACK PREPARED
    INIT / PREPARED / missing -> decide ABORT via compare-and-set, then ROLLBACK PREPARED
                                 (presumed abort; the CAS means a still-running coordinator
                                 can't commit afterwards — it would see 0 rows updated)

Runs on every app node: once at startup (this node's own txns immediately — it knows it
crashed — others' after a grace period) and then periodically. Any node can finish any
other node's transactions because the log lives in the shared, replicated Shard A cluster.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from .config import settings
from .coordinator import TRANSIENT, TwoPhaseCommit
from .runtime import rt
from .topology import ShardTopology

log = logging.getLogger("recovery")
GRACE_S = 5.0          # don't touch other coordinators' prepared txns younger than this
STALE_INIT_S = 30.0    # INIT txns with nothing prepared after this long: coordinator is gone
                       # (must exceed lock_timeout so we never abort a txn merely waiting)
RECOVERY_LOCK = 4243   # advisory lock: one resolver pass at a time cluster-wide


async def _prepared(shard: ShardTopology) -> list[dict[str, Any]]:
    _, pool = await shard.primary_control()
    rows = await pool.fetch(
        "SELECT gid, extract(epoch FROM now() - prepared)::float8 AS age_s FROM pg_prepared_xacts"
        " WHERE gid LIKE 'auc:%' AND database = current_database()")
    return [dict(r) for r in rows]


async def resolve_once(own_immediately: bool = False) -> list[dict[str, Any]]:
    """One resolver pass. Returns the actions taken (for logs / demo output)."""
    tpc = TwoPhaseCommit(rt.shard_a, rt.shard_b)
    actions: list[dict[str, Any]] = []
    _, apool = await rt.shard_a.primary_control()
    async with apool.acquire() as lock_conn:
        if not await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", RECOVERY_LOCK):
            return actions
        try:
            scanned_all = True
            for shard in (rt.shard_a, rt.shard_b):
                try:
                    prepared = await _prepared(shard)
                except TRANSIENT:
                    scanned_all = False
                    continue
                for p in prepared:
                    _, txn_s, part = p["gid"].split(":")
                    txn_id = uuid.UUID(txn_s)
                    row = await apool.fetchrow(
                        "SELECT state, coordinator FROM coord.txn_log WHERE txn_id = $1", txn_id)
                    state = row["state"] if row else None
                    mine = row is not None and row["coordinator"] == settings.node_id
                    if p["age_s"] < GRACE_S and not (mine and own_immediately):
                        continue  # probably still in flight on a live coordinator
                    if state in (None, "INIT", "PREPARED"):
                        if row is None or await tpc._cas(txn_id, "ABORTED", ("INIT", "PREPARED")):
                            state = "ABORTED"
                        else:  # lost the race: someone just decided — follow that decision
                            state = await apool.fetchval("SELECT state FROM coord.txn_log WHERE txn_id = $1", txn_id)
                    commit = state == "COMMITTED"
                    done = await tpc.finish(shard, p["gid"], commit=commit, attempts=2)
                    act = {"txn_id": str(txn_id), "shard": part, "decision": "COMMIT" if commit else "ROLLBACK",
                           "log_state": state, "done": done, "age_s": round(p["age_s"], 1)}
                    actions.append(act)
                    log.warning("resolved in-doubt %s on shard %s -> %s PREPARED (log=%s, age %.1fs)",
                                txn_id, part, act["decision"], state, p["age_s"])

            if scanned_all:
                # Txns whose coordinator died before preparing anything: decide ABORT.
                await apool.execute(
                    "UPDATE coord.txn_log SET state = 'ABORTED', updated_at = now()"
                    " WHERE state IN ('INIT', 'PREPARED') AND NOT completed AND ts < now() - make_interval(secs => $1)"
                    " AND NOT EXISTS (SELECT 1 FROM pg_prepared_xacts WHERE gid LIKE 'auc:' || txn_id || ':%')",
                    STALE_INIT_S)
                # Decided txns with no prepared parts left anywhere are complete.
                b_left = {p["gid"].split(":")[1] for p in await _prepared(rt.shard_b)}
                a_left = {p["gid"].split(":")[1] for p in await _prepared(rt.shard_a)}
                pending = await apool.fetch(
                    "SELECT txn_id FROM coord.txn_log WHERE state IN ('COMMITTED', 'ABORTED') AND NOT completed"
                    " AND updated_at < now() - interval '1 second'")
                done_ids = [r["txn_id"] for r in pending if str(r["txn_id"]) not in a_left | b_left]
                if done_ids:
                    await apool.execute("UPDATE coord.txn_log SET completed = true WHERE txn_id = ANY($1::uuid[])", done_ids)
        finally:
            await lock_conn.execute("SELECT pg_advisory_unlock($1)", RECOVERY_LOCK)
    return actions


async def run(interval: float = 3.0) -> None:
    first = True
    while True:
        try:
            acts = await resolve_once(own_immediately=first)
            if first:
                log.info("startup recovery pass done: %d in-doubt participant txn(s) resolved", len(acts))
            first = False
            if acts:
                await rt.hub.publish({"type": "recovery", "actions": acts})
        except asyncio.CancelledError:
            raise
        except TRANSIENT:
            pass
        except Exception as e:
            log.warning("resolver pass failed: %r", e)
        await asyncio.sleep(interval)
