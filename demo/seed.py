"""Seed users, wallets and auctions.

  python -m demo.seed            # seed only if empty
  python -m demo.seed --reset    # wipe both shards and reseed (fresh demo state)
"""
from __future__ import annotations

import asyncio
import sys
from decimal import Decimal

from demo.common import banner, info, ok, wait_for, leader
from demo.db import connect

USERS = ["Alice", "Bob", "Carol", "Dave", "Erin", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
STARTING_BALANCE = Decimal("1000.00")
ITEMS = [  # title, seller, start price, minutes until close
    ("Vintage Rolex Submariner", 1, 200, 120), ("Signed Beatles LP", 1, 150, 180),
    ("Banksy print (numbered)", 2, 300, 240), ("1965 Fender Stratocaster", 2, 250, 300),
    ("First-edition Dune", 1, 80, 360), ("Apollo 11 press kit", 2, 120, 420),
]


async def main(reset: bool) -> None:
    banner("Seed")
    wait_for(lambda: leader("A") and leader("B"), timeout=90, what="both shard primaries")
    a, b = await connect("A"), await connect("B")
    try:
        if reset:
            await a.execute("TRUNCATE bids, items, users, coord.txn_log RESTART IDENTITY CASCADE")
            await b.execute("TRUNCATE escrow_holds, wallets RESTART IDENTITY CASCADE")
            info("both shards truncated")
        elif await a.fetchval("SELECT count(*) FROM users"):
            ok("already seeded — skipping (use --reset to reseed)")
            return
        await a.executemany("INSERT INTO users (id, name) VALUES ($1, $2)",
                            [(i + 1, n) for i, n in enumerate(USERS)])
        await b.executemany("INSERT INTO wallets (user_id, balance) VALUES ($1, $2)",
                            [(i + 1, STARTING_BALANCE) for i in range(len(USERS))])
        await a.executemany(
            """INSERT INTO items (title, seller_id, start_price, current_price, ends_at)
               VALUES ($1, $2, $3, $3, now() + make_interval(mins => $4))""",
            [(t, s, Decimal(p), m) for t, s, p, m in ITEMS])
        ok(f"{len(USERS)} users, {len(USERS)} wallets × ${STARTING_BALANCE}, {len(ITEMS)} auctions")
    finally:
        await a.close()
        await b.close()


if __name__ == "__main__":
    asyncio.run(main("--reset" in sys.argv))
