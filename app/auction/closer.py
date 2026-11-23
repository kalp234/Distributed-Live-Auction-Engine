"""Auction auto-close job — runs on every app node, but only the elected leader works.

Leader election: each node tries `pg_try_advisory_lock(4242)` on a dedicated connection to
the Shard A primary. A *session* advisory lock is held for as long as that connection lives,
so exactly one node is the closer; if it dies (or the primary fails over and the connection
drops) the lock vanishes and another node takes over within a tick.
Double-close is impossible anyway: items are claimed with FOR UPDATE SKIP LOCKED + status.

[T4] Settlement of a sold item is a 2PC transaction:
    Shard A: item -> 'sold'
    Shard B: winner's active escrow hold -> 'settled'; winner balance -= price, held -= price;
             seller balance += price
(Every outbid hold was already released when it was outbid, so the only active hold on an
item is the winner's.) Items without bids are closed with a single-shard update.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

import asyncpg

from .config import settings
from .coordinator import TRANSIENT, TwoPhaseCommit
from .errors import AppError
from .runtime import rt

log = logging.getLogger("closer")
CLOSER_LOCK = 4242


class Closer:
    def __init__(self) -> None:
        self.conn: asyncpg.Connection | None = None
        self.conn_host: str | None = None
        self.is_leader = False

    async def _elect(self) -> bool:
        leader = rt.shard_a.leader_name
        if leader is None:
            return False
        if self.conn is None or self.conn.is_closed() or self.conn_host != leader:
            if self.conn is not None:
                self.conn.terminate()
            m = rt.shard_a.members[leader]
            self.conn = await asyncpg.connect(host=m.host, port=m.port, user=settings.db_user,
                                              password=settings.db_password, database=settings.db_a_name,
                                              timeout=2, server_settings={"application_name": f"closer-{settings.node_id}"})
            self.conn_host, self.is_leader = leader, False
        if not self.is_leader:
            self.is_leader = bool(await self.conn.fetchval("SELECT pg_try_advisory_lock($1)", CLOSER_LOCK))
            if self.is_leader:
                log.info("%s is now the auction-closer leader", settings.node_id)
        else:
            await self.conn.fetchval("SELECT 1", timeout=2)  # liveness of the lock-holding session
        return self.is_leader

    async def close_expired_once(self) -> list[dict[str, Any]]:
        _, pool = await rt.shard_a.primary()
        due = await pool.fetch("SELECT id FROM items WHERE status = 'open' AND ends_at <= now() ORDER BY ends_at LIMIT 20")
        out = []
        for r in due:
            try:
                ev = await settle(r["id"])
            except AppError as e:
                log.info("close of item %s skipped: %s", r["id"], e.message)
                continue
            if ev:
                out.append(ev)
                await rt.hub.publish(dict(ev))
        return out

    async def run(self, interval: float = 1.0) -> None:
        while True:
            try:
                if await self._elect():
                    closed = await self.close_expired_once()
                    if closed:
                        log.info("closed %d auction(s): %s", len(closed), [c["item_id"] for c in closed])
            except asyncio.CancelledError:
                raise
            except (TRANSIENT + (asyncpg.PostgresError,)) as e:
                self.is_leader = False
                if self.conn is not None:
                    self.conn.terminate()
                    self.conn = None
                log.debug("closer tick failed: %r", e)
            except Exception as e:
                log.warning("closer tick failed: %r", e)
            await asyncio.sleep(interval)


async def settle(item_id: int) -> dict[str, Any] | None:
    """Close one expired item. Returns the 'closed' event, or None if someone else did."""
    _, pool = await rt.shard_a.primary()
    item = await pool.fetchrow("SELECT seller_id, highest_bidder, current_price,"
                               " EXISTS (SELECT 1 FROM bids WHERE item_id = $1) AS has_bids"
                               " FROM items WHERE id = $1 AND status = 'open' AND ends_at <= now()", item_id)
    if item is None:
        return None
    if not item["has_bids"]:
        row = await pool.fetchrow("UPDATE items SET status = 'closed', version = version + 1"
                                  " WHERE id = $1 AND status = 'open' AND NOT EXISTS (SELECT 1 FROM bids WHERE item_id = $1)"
                                  " RETURNING version", item_id)
        return {"type": "closed", "item_id": item_id, "status": "closed", "winner": None, "price": None,
                "version": row["version"]} if row else None

    async def work_a(conn: asyncpg.Connection, txn_id: uuid.UUID) -> dict[str, Any]:
        row = await conn.fetchrow(
            "SELECT seller_id, highest_bidder, current_price FROM items"
            " WHERE id = $1 AND status = 'open' AND ends_at <= now() FOR UPDATE SKIP LOCKED", item_id)
        if row is None:
            raise AppError(409, "already_closed", "item already closed or busy")
        version = await conn.fetchval("UPDATE items SET status = 'sold', version = version + 1 WHERE id = $1"
                                      " RETURNING version", item_id)
        return {**dict(row), "version": version}

    async def work_b(conn: asyncpg.Connection, txn_id: uuid.UUID, a: dict[str, Any]) -> dict[str, Any]:
        winner, seller, price = a["highest_bidder"], a["seller_id"], a["current_price"]
        await conn.fetch("SELECT user_id FROM wallets WHERE user_id = ANY($1::int[]) ORDER BY user_id FOR UPDATE",
                         sorted({winner, seller}))
        hold = await conn.fetchrow(
            "UPDATE escrow_holds SET status = 'settled', closed_by = $2, updated_at = now()"
            " WHERE item_id = $1 AND status = 'active' RETURNING user_id, amount", item_id, txn_id)
        if hold is None or hold["user_id"] != winner or hold["amount"] != price:
            raise AppError(500, "escrow_mismatch", f"item {item_id}: active hold {dict(hold) if hold else None} "
                                                   f"does not match winner {winner} @ {price}")
        await conn.execute("UPDATE wallets SET balance = balance - $2, held = held - $2 WHERE user_id = $1", winner, price)
        await conn.execute("UPDATE wallets SET balance = balance + $2 WHERE user_id = $1", seller, price)
        return {}

    txn_id, a, _ = await TwoPhaseCommit(rt.shard_a, rt.shard_b).run(
        "close", {"item_id": item_id, "winner": item["highest_bidder"], "price": str(item["current_price"])},
        work_a, work_b)
    return {"type": "closed", "item_id": item_id, "status": "sold", "winner": a["highest_bidder"],
            "seller": a["seller_id"], "price": float(a["current_price"]), "version": a["version"], "txn_id": str(txn_id)}
