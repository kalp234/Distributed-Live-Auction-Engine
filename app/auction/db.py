"""Query helpers that apply read/write splitting and read-your-writes.

[T6] `read()` runs on a replica (round-robin), `write_conn()` on the primary.
[T1] Read-your-writes: a client that just wrote passes the returned `X-Write-LSN` back as
     `X-Min-LSN`. A replica is only used if it has *replayed* at least that LSN; otherwise
     the read falls back to the primary, so a user never sees their own bid "disappear".
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

import asyncpg

from .topology import ShardTopology


@dataclass
class ReadResult:
    rows: list[asyncpg.Record]
    member: str           # DB node that answered
    fallback: str | None  # why we did not use a replica (None = replica used)


async def read(shard: ShardTopology, sql: str, *args: Any, min_lsn: str | None = None) -> ReadResult:
    name, pool = await shard.replica()
    fallback: str | None = None if name != shard.leader_name else "no-healthy-replica"
    if fallback is None:
        try:
            async with pool.acquire() as conn:
                if min_lsn:
                    caught_up = await conn.fetchval(
                        "SELECT pg_last_wal_replay_lsn() >= $1::text::pg_lsn", min_lsn)
                    if not caught_up:
                        fallback = "replica-behind-min-lsn"
                if fallback is None:
                    rows = await conn.fetch(sql, *args)
                    shard.report(name, True)
                    return ReadResult(rows, name, None)
        except (OSError, asyncio.TimeoutError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError,
                asyncpg.CannotConnectNowError, asyncpg.AdminShutdownError) as e:
            shard.report(name, False)  # [T2] trip this replica's breaker; fall back below
            fallback = f"replica-error:{type(e).__name__}"
    pname, ppool = await shard.primary()
    try:
        async with ppool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
    except (OSError, asyncio.TimeoutError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError):
        shard.report(pname, False)
        raise
    return ReadResult(rows, pname, fallback)


@asynccontextmanager
async def write_conn(shard: ShardTopology) -> AsyncIterator[tuple[str, asyncpg.Connection]]:
    name, pool = await shard.primary()
    async with pool.acquire() as conn:
        yield name, conn


async def current_lsn(conn: asyncpg.Connection) -> str:
    return str(await conn.fetchval("SELECT pg_current_wal_lsn()::text"))


async def load_board(shard: ShardTopology) -> dict[str, dict[str, Any]]:
    """Live board (open items' current state) straight from the database (source of truth)."""
    r = await read(shard, "SELECT id, current_price, highest_bidder, version FROM items WHERE status = 'open'")
    return {str(x["id"]): {"price": float(x["current_price"]), "bidder": x["highest_bidder"], "version": x["version"]}
            for x in r.rows}
