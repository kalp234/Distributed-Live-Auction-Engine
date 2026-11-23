"""Bid placement (M2: single shard — upgraded to cross-shard 2PC in M5)."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg

from . import db
from .errors import AppError
from .config import settings
from .runtime import rt


async def place_bid(item_id: int, user_id: int, amount: Decimal, idem_key: str | None) -> dict[str, Any]:
    txn_id = uuid.uuid4()
    async with db.write_conn(rt.shard_a) as (member, conn):
        async with conn.transaction():
            if idem_key:
                prev = await conn.fetchrow(
                    "SELECT id, item_id, amount, txn_id FROM bids WHERE idempotency_key = $1", idem_key)
                if prev:
                    return {"status": "duplicate", "bid_id": prev["id"], "item_id": prev["item_id"],
                            "amount": float(prev["amount"]), "txn_id": str(prev["txn_id"]),
                            "_db_node": member}
            item = await conn.fetchrow(
                "SELECT id, status, ends_at, current_price, highest_bidder, seller_id, version, start_price,"
                " (SELECT count(*) FROM bids WHERE item_id = $1) AS n_bids"
                " FROM items WHERE id = $1 FOR UPDATE", item_id)
            _validate(item, user_id, amount)
            try:
                bid_id = await conn.fetchval(
                    "INSERT INTO bids (item_id, user_id, amount, txn_id, idempotency_key, served_by)"
                    " VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                    item_id, user_id, amount, txn_id, idem_key, settings.node_id)
            except asyncpg.UniqueViolationError:
                raise AppError(409, "duplicate_in_flight", "same Idempotency-Key is being processed")
            version = await conn.fetchval(
                "UPDATE items SET current_price = $2, highest_bidder = $3, version = version + 1"
                " WHERE id = $1 RETURNING version", item_id, amount, user_id)
        lsn = await db.current_lsn(conn)
    event = {"type": "bid", "item_id": item_id, "bid_id": bid_id, "user_id": user_id,
             "amount": float(amount), "version": version, "txn_id": str(txn_id)}
    await rt.hub.publish(dict(event))
    return {"status": "accepted", **event, "_db_node": member, "_lsn": lsn}


def _validate(item: Any, user_id: int, amount: Decimal) -> None:
    if item is None:
        raise AppError(404, "not_found", "item not found")
    if item["status"] != "open" or item["ends_at"] <= datetime.now(timezone.utc):
        raise AppError(409, "auction_closed", "auction is closed")
    if item["seller_id"] == user_id:
        raise AppError(409, "own_item", "sellers cannot bid on their own item")
    if item["n_bids"]:
        if amount <= item["current_price"]:
            raise AppError(409, "bid_too_low", f"bid must be > {item['current_price']}")
    elif amount < item["start_price"]:
        raise AppError(409, "bid_too_low", f"bid must be >= {item['start_price']}")
