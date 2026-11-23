"""[T4] Bid placement as one distributed transaction across both shards.

Shard A (auction) part:  lock the item row (SELECT ... FOR UPDATE), validate (open, not
                         expired, not own item, amount > current price), insert the bid,
                         bump current_price / highest_bidder / version.
Shard B (wallet) part:   lock the involved wallets in user_id order (deadlock-free), release
                         the previous highest bidder's escrow hold, check the bidder's free
                         funds, place a new escrow hold for the bid amount.
Both parts are PREPAREd and then committed together by the 2PC coordinator — either the
bid exists AND the money is held, or neither.

Concurrency: the item row lock is held by the prepared transaction until COMMIT PREPARED,
so concurrent bids on one item serialize; the second one re-reads the new price and is
rejected if it is no longer higher ("exactly one wins a price slot").
Idempotency: the Idempotency-Key is UNIQUE in bids; a retry returns the original result.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import uuid

import asyncpg

from . import db
from .config import settings
from .coordinator import TwoPhaseCommit
from .errors import AppError
from .runtime import rt


async def _existing(idem_key: str) -> dict[str, Any] | None:
    _, pool = await rt.shard_a.primary()
    prev = await pool.fetchrow(
        "SELECT id, item_id, user_id, amount, txn_id FROM bids WHERE idempotency_key = $1", idem_key)
    if prev is None:
        return None
    return {"status": "duplicate", "bid_id": prev["id"], "item_id": prev["item_id"], "user_id": prev["user_id"],
            "amount": float(prev["amount"]), "txn_id": str(prev["txn_id"])}


async def place_bid(item_id: int, user_id: int, amount: Decimal, idem_key: str | None) -> dict[str, Any]:
    if idem_key and (dup := await _existing(idem_key)):
        return dup  # retry of a request that already committed: never charge twice

    async def work_a(conn: asyncpg.Connection, txn_id: uuid.UUID) -> dict[str, Any]:
        # Everything we validate must come from the LOCKED row itself: under READ COMMITTED,
        # after waiting for the lock PostgreSQL re-reads the row (EvalPlanQual) but any
        # subquery in the same statement still sees the old snapshot.
        item = await conn.fetchrow(
            "SELECT id, status, ends_at, current_price, start_price, highest_bidder, seller_id, version"
            " FROM items WHERE id = $1 FOR UPDATE", item_id)
        # A parallel retry of the same request may have committed while we waited for the
        # lock; this new statement sees it -> answer with the original result.
        if idem_key and await conn.fetchval("SELECT 1 FROM bids WHERE idempotency_key = $1", idem_key):
            raise AppError(409, "duplicate_in_flight", "request already processed")
        _validate(item, user_id, amount)
        try:
            bid_id = await conn.fetchval(
                "INSERT INTO bids (item_id, user_id, amount, txn_id, idempotency_key, served_by)"
                " VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                item_id, user_id, amount, txn_id, idem_key, settings.node_id)
        except asyncpg.UniqueViolationError:
            raise AppError(409, "duplicate_in_flight", "a request with this Idempotency-Key is already being processed")
        version = await conn.fetchval(
            "UPDATE items SET current_price = $2, highest_bidder = $3, version = version + 1"
            " WHERE id = $1 RETURNING version", item_id, amount, user_id)
        return {"bid_id": bid_id, "version": version, "prev_bidder": item["highest_bidder"]}

    async def work_b(conn: asyncpg.Connection, txn_id: uuid.UUID, a: dict[str, Any]) -> dict[str, Any]:
        users = sorted({user_id} | ({a["prev_bidder"]} if a["prev_bidder"] else set()))
        wallets = {r["user_id"]: r for r in await conn.fetch(
            "SELECT user_id, balance, held FROM wallets WHERE user_id = ANY($1::int[]) ORDER BY user_id FOR UPDATE",
            users)}
        if user_id not in wallets:
            raise AppError(404, "no_wallet", f"user {user_id} has no wallet")
        # Release the current highest hold on this item (Shard B is the source of truth for money).
        released = await conn.fetchrow(
            "UPDATE escrow_holds SET status = 'released', closed_by = $2, updated_at = now()"
            " WHERE item_id = $1 AND status = 'active' RETURNING user_id, amount", item_id, txn_id)
        if released:
            await conn.execute("UPDATE wallets SET held = held - $2 WHERE user_id = $1",
                               released["user_id"], released["amount"])
        free = await conn.fetchval("SELECT balance - held FROM wallets WHERE user_id = $1", user_id)
        if free < amount:
            raise AppError(409, "insufficient_funds", f"insufficient funds: free balance {free} < bid {amount}")
        await conn.execute(
            "INSERT INTO escrow_holds (user_id, item_id, amount, txn_id, status) VALUES ($1, $2, $3, $4, 'active')",
            user_id, item_id, amount, txn_id)
        await conn.execute("UPDATE wallets SET held = held + $2 WHERE user_id = $1", user_id, amount)
        return {"released": dict(released) if released else None}

    try:
        txn_id, ra, rb = await TwoPhaseCommit(rt.shard_a, rt.shard_b).run(
            "bid", {"item_id": item_id, "user_id": user_id, "amount": str(amount), "idempotency_key": idem_key},
            work_a, work_b)
    except AppError as e:
        if e.code == "duplicate_in_flight" and idem_key and (dup := await _existing(idem_key)):
            return dup
        raise
    async with db.write_conn(rt.shard_a) as (member, conn):
        lsn = await db.current_lsn(conn)
    event = {"type": "bid", "item_id": item_id, "bid_id": ra["bid_id"], "user_id": user_id,
             "amount": float(amount), "version": ra["version"], "txn_id": str(txn_id)}
    await rt.hub.publish(dict(event))
    rel = rb["released"]
    return {"status": "accepted", **event,
            "released_hold": {"user_id": rel["user_id"], "amount": float(rel["amount"])} if rel else None,
            "_db_node": member, "_lsn": lsn}


def _validate(item: Any, user_id: int, amount: Decimal) -> None:
    if item is None:
        raise AppError(404, "not_found", "item not found")
    if item["status"] != "open" or item["ends_at"] <= datetime.now(timezone.utc):
        raise AppError(409, "auction_closed", "auction is closed")
    if item["seller_id"] == user_id:
        raise AppError(409, "own_item", "sellers cannot bid on their own item")
    if item["highest_bidder"] is not None:   # has bids
        if amount <= item["current_price"]:
            raise AppError(409, "bid_too_low", f"bid must be > {item['current_price']}")
    elif amount < item["start_price"]:
        raise AppError(409, "bid_too_low", f"bid must be >= {item['start_price']}")
