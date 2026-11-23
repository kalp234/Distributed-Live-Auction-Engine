"""Direct DB access for demos/tests: connects to the *current* primary of a shard."""
from __future__ import annotations

import os

import asyncpg

from demo.common import leader

DBNAME = {"A": "auction", "B": "wallet"}


async def connect(shard: str, member: str | None = None, superuser: bool = False) -> asyncpg.Connection:
    host = member or leader(shard)
    if host is None:
        raise RuntimeError(f"shard {shard} has no leader")
    if superuser:
        user, pw = "postgres", os.environ.get("PG_SUPERUSER_PASSWORD", "postgres_pw")
    else:
        user, pw = os.environ.get("DB_USER", "auction_app"), os.environ.get("DB_PASSWORD", "auction_pw")
    return await asyncpg.connect(host=host, port=5432, user=user, password=pw, database=DBNAME[shard], timeout=3)
