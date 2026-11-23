"""Auction auto-close job (runs on every app node; exactly one does the work).

Safe with multiple nodes: each tick takes a transaction-scoped advisory lock on the Shard A
primary (`pg_try_advisory_xact_lock`). Whoever gets it closes the expired items; the others
skip. FOR UPDATE SKIP LOCKED + the status check make a double close impossible anyway.
"""
from __future__ import annotations

import asyncio
import logging

from . import db
from .runtime import rt
from .topology import NoPrimaryError

log = logging.getLogger("closer")
CLOSER_LOCK = 4242


async def close_expired_once() -> list[dict]:
    closed: list[dict] = []
    async with db.write_conn(rt.shard_a) as (_, conn):
        async with conn.transaction():
            if not await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", CLOSER_LOCK):
                return closed
            rows = await conn.fetch(
                """UPDATE items SET status = CASE WHEN highest_bidder IS NULL THEN 'closed' ELSE 'sold' END,
                                    version = version + 1
                   WHERE id IN (SELECT id FROM items WHERE status = 'open' AND ends_at <= now()
                                ORDER BY ends_at LIMIT 50 FOR UPDATE SKIP LOCKED)
                   RETURNING id, status, highest_bidder, current_price, version""")
            closed = [{"type": "closed", "item_id": r["id"], "status": r["status"],
                       "winner": r["highest_bidder"], "price": float(r["current_price"]),
                       "version": r["version"]} for r in rows]
    for ev in closed:
        await rt.hub.publish(ev)
    return closed


async def run(interval: float = 1.0) -> None:
    while True:
        try:
            closed = await close_expired_once()
            if closed:
                log.info("closed %d auction(s): %s", len(closed), [c["item_id"] for c in closed])
        except NoPrimaryError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("closer tick failed: %s", e)
        await asyncio.sleep(interval)
