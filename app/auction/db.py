"""Query helpers that apply read/write splitting and read-your-writes.

[T6] `read()` runs on a replica (round-robin), `write_conn()` on the primary.
[T1] Read-your-writes: a client that just wrote passes the returned `X-Write-LSN` back as
     `X-Min-LSN`. A replica is only used if it has *replayed* at least that LSN; otherwise
     the read falls back to the primary, so a user never sees their own bid "disappear".
"""
from __future__ import annotations

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
                    return ReadResult(await conn.fetch(sql, *args), name, None)
        except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as e:
            fallback = f"replica-error:{type(e).__name__}"
    pname, ppool = await shard.primary()
    async with ppool.acquire() as conn:
        return ReadResult(await conn.fetch(sql, *args), pname, fallback)


@asynccontextmanager
async def write_conn(shard: ShardTopology) -> AsyncIterator[tuple[str, asyncpg.Connection]]:
    name, pool = await shard.primary()
    async with pool.acquire() as conn:
        yield name, conn


async def current_lsn(conn: asyncpg.Connection) -> str:
    return str(await conn.fetchval("SELECT pg_current_wal_lsn()::text"))
