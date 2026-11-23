"""[T4] Two-Phase Commit coordinator across Shard A (auction) and Shard B (wallet).

Protocol (participants are PostgreSQL's native PREPARE TRANSACTION):

    INIT        log txn in coord.txn_log (Shard A cluster, sync-replicated)
    phase 1     A: BEGIN; <work>; PREPARE TRANSACTION 'auc:<txn>:A'   (vote yes / no)
                B: BEGIN; <work>; PREPARE TRANSACTION 'auc:<txn>:B'
    PREPARED    both voted yes
    decision    CAS txn_log.state -> COMMITTED  (THE durable commit point)
    phase 2     COMMIT PREPARED on A and B (retried; the resolver finishes it if we can't)
    completed   txn_log.completed = true

Any NO vote / error before the decision -> ROLLBACK (PREPARED) everywhere, state ABORTED.

Safety rule [T5]: every state transition on txn_log is a compare-and-set
(`... WHERE state IN ('INIT','PREPARED')`). A recovering node that decides ABORT and a slow
coordinator that decides COMMIT can therefore never both win: the loser sees 0 rows updated
and follows the winner's decision. Unknown txns are presumed aborted.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import asyncpg

from . import faults
from .config import settings
from .errors import AppError
from .topology import NoPrimaryError, ShardTopology

log = logging.getLogger("2pc")

# work(conn, txn_id) runs one participant's part inside its (to-be-prepared) transaction
Work = Callable[[asyncpg.Connection, uuid.UUID], Awaitable[Any]]
TRANSIENT = (OSError, asyncio.TimeoutError, NoPrimaryError, asyncpg.PostgresConnectionError,
             asyncpg.InterfaceError, asyncpg.ReadOnlySQLTransactionError, asyncpg.CannotConnectNowError,
             asyncpg.AdminShutdownError)


def gid(txn_id: uuid.UUID, shard: str) -> str:
    return f"auc:{txn_id}:{shard}"


@dataclass
class Stats:
    committed: int = 0
    aborted: int = 0
    phase2_deferred: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)


stats = Stats()


class TwoPhaseCommit:
    def __init__(self, a: ShardTopology, b: ShardTopology) -> None:
        self.a, self.b = a, b

    # ------------------------------------------------------------------ txn_log (CAS)
    async def _log(self, sql: str, *args: Any) -> Any:
        _, pool = await self.a.primary_control()
        return await pool.fetchval(sql, *args)

    async def _log_init(self, txn_id: uuid.UUID, kind: str, detail: dict[str, Any]) -> None:
        await self._log(
            "INSERT INTO coord.txn_log (txn_id, kind, state, participants, coordinator, detail)"
            " VALUES ($1, $2, 'INIT', '{auction,wallet}', $3, $4::jsonb) RETURNING 1",
            txn_id, kind, settings.node_id, json.dumps(detail, default=str))

    async def _cas(self, txn_id: uuid.UUID, new: str, allowed: tuple[str, ...]) -> bool:
        """Compare-and-set the txn state; True iff we made the transition."""
        row = await self._log(
            "UPDATE coord.txn_log SET state = $2, updated_at = now()"
            " WHERE txn_id = $1 AND state = ANY($3::text[]) RETURNING 1",
            txn_id, new, list(allowed))
        return row == 1

    async def _mark_completed(self, txn_id: uuid.UUID) -> None:
        await self._log("UPDATE coord.txn_log SET completed = true, updated_at = now()"
                        " WHERE txn_id = $1 RETURNING 1", txn_id)

    # ------------------------------------------------------------------ participants
    async def _prepare(self, shard: ShardTopology, txn_id: uuid.UUID, work: Work) -> Any:
        """Phase 1 on one participant: run `work` in a txn and PREPARE it (vote yes)."""
        _, pool = await shard.primary()
        async with pool.acquire() as conn:
            await conn.execute("BEGIN")
            try:
                await conn.execute("SET LOCAL lock_timeout = '10s'")
                try:
                    result = await work(conn, txn_id)
                except asyncpg.LockNotAvailableError:
                    raise AppError(409, "item_busy", f"shard {shard.shard}: lock wait timed out "
                                   "(contention, or rows held by an in-doubt prepared txn)")
                if shard.shard == "B" and faults.take("vote_no_b"):
                    raise AppError(503, "participant_vote_no", "injected: Shard B voted NO at PREPARE")
                await conn.execute(f"PREPARE TRANSACTION '{gid(txn_id, shard.shard)}'")
                return result
            except BaseException:
                if not conn.is_closed():
                    try:
                        await conn.execute("ROLLBACK")
                    except Exception:
                        pass
                raise

    async def finish(self, shard: ShardTopology, g: str, commit: bool, attempts: int = 6) -> bool:
        """Phase 2 on one participant. True = resolved (now or earlier, e.g. by the resolver)."""
        verb = "COMMIT" if commit else "ROLLBACK"
        delay = 0.2
        for _ in range(attempts):
            try:
                _, pool = await shard.primary_control()
                await pool.execute(f"{verb} PREPARED '{g}'")
                return True
            except asyncpg.UndefinedObjectError:
                return True  # no such prepared txn: already finished by someone else
            except asyncpg.ObjectNotInPrerequisiteStateError as e:  # "is busy": being finished concurrently
                log.info("%s PREPARED %s: %s; retrying", verb, g, e)
                await asyncio.sleep(delay)
            except TRANSIENT as e:
                log.warning("%s PREPARED %s on shard %s failed (%s); retrying", verb, g, shard.shard, e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 2.0)  # [T2] exponential backoff
        return False

    # ------------------------------------------------------------------ protocol
    async def run(self, kind: str, detail: dict[str, Any], work_a: Work,
                  work_b: Callable[[asyncpg.Connection, uuid.UUID, Any], Awaitable[Any]]) -> tuple[uuid.UUID, Any, Any]:
        """Execute work_a on Shard A and work_b on Shard B atomically. work_b also receives
        work_a's result. Raises AppError on abort; returns (txn_id, result_a, result_b)."""
        txn_id = uuid.uuid4()
        await self._log_init(txn_id, kind, detail)
        prepared: list[ShardTopology] = []
        try:
            # ---- phase 1: voting ------------------------------------------------------
            res_a = await self._prepare(self.a, txn_id, work_a)
            prepared.append(self.a)
            res_b = await self._prepare(self.b, txn_id, lambda c, t: work_b(c, t, res_a))
            prepared.append(self.b)
            if not await self._cas(txn_id, "PREPARED", ("INIT",)):
                raise AppError(503, "aborted_by_recovery", "transaction was aborted by the recovery process")
            await faults.maybe_crash("crash_before_decision")
            # ---- decision: the commit point ----------------------------------------------
            if not await self._cas(txn_id, "COMMITTED", ("INIT", "PREPARED")):
                raise AppError(503, "aborted_by_recovery", "transaction was aborted by the recovery process")
        except BaseException as e:
            await self._abort(txn_id, prepared, e)
            raise

        # ---- phase 2: completion (decision is durable; failures here are not aborts) ----
        await faults.maybe_crash("crash_after_decision")
        await faults.maybe_delay("delay_before_phase2")
        lose_b = faults.take("lose_b_in_phase2") is not None

        async def commit_b() -> bool:
            return False if lose_b else await self.finish(self.b, gid(txn_id, "B"), commit=True)

        # Both participants in parallel: shortens how long the prepared txns hold their locks.
        ok_a, ok_b = await asyncio.gather(self.finish(self.a, gid(txn_id, "A"), commit=True), commit_b())
        if ok_a and ok_b:
            try:
                await self._mark_completed(txn_id)
            except TRANSIENT:
                pass  # harmless: the resolver will mark it
        else:
            stats.phase2_deferred += 1
            log.warning("txn %s COMMITTED but phase 2 incomplete (A=%s B=%s); resolver will finish", txn_id, ok_a, ok_b)
        stats.committed += 1
        return txn_id, res_a, res_b

    async def _abort(self, txn_id: uuid.UUID, prepared: list[ShardTopology], cause: BaseException) -> None:
        reason = cause.code if isinstance(cause, AppError) else type(cause).__name__
        stats.aborted += 1
        stats.by_reason[reason] = stats.by_reason.get(reason, 0) + 1
        state: str | None = None
        try:
            if await self._cas(txn_id, "ABORTED", ("INIT", "PREPARED")):
                state = "ABORTED"
            else:
                state = await self._log("SELECT state FROM coord.txn_log WHERE txn_id = $1", txn_id)
        except TRANSIENT:
            state = None  # log unreachable: outcome unknown, the resolver will decide from the log
        if state != "ABORTED":
            # NEVER roll back unless the log says ABORTED: if our COMMITTED write succeeded but
            # its reply was lost, rolling back would contradict a durable commit decision.
            log.warning("txn %s failed (%s) with log state %s; leaving prepared parts to the resolver",
                        txn_id, reason, state)
            if isinstance(cause, AppError):
                return
            raise AppError(503, "outcome_unknown",
                           "transaction outcome unknown; retry with the same Idempotency-Key") from cause
        ok = True
        for shard in prepared:
            ok &= await self.finish(shard, gid(txn_id, shard.shard), commit=False, attempts=3)
        if ok:
            try:
                await self._mark_completed(txn_id)
            except TRANSIENT:
                pass
        log.info("txn %s ABORTED (%s)", txn_id, reason)
