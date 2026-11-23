"""[T4] Invariant checker — proves that no money is created or lost and that the two
shards agree with each other. Reads both shard primaries directly.

Money (Shard B):
  M1  Σ balance == money supply (10 wallets × $1000 from the seed; nothing else mints money)
  M2  0 ≤ held ≤ balance for every wallet                    (also a CHECK constraint)
  M3  every wallet's held == Σ of its active escrow holds
  M4  at most one active hold per item                        (also a unique index)
Cross-shard (A ↔ B):
  X1  open item with bids  -> exactly one active hold, by the highest bidder, = current_price
  X2  item without bids     -> no hold at all
  X3  sold item             -> exactly one settled hold, by the winner, = final price; no active hold
  X4  every bid / hold belongs to a COMMITTED transaction in coord.txn_log
Auction (Shard A):
  A1  current_price == max(bid amount) and highest_bidder == that bid's user
  A2  the highest amount appears exactly once per item (one winner per price slot)
2PC:
  T1  no prepared (in-doubt) transactions left and every txn_log entry completed
      (only meaningful once the system is quiescent)

  python -m demo.invariants            # exits 1 on any violation
"""
from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from typing import Any

from demo.common import Checks, banner, info, step
from demo.db import connect

SUPPLY_PER_WALLET = Decimal("1000.00")


async def collect() -> dict[str, Any]:
    a, b = await connect("A"), await connect("B")
    try:
        return {
            "wallets": await b.fetch("SELECT user_id, balance, held FROM wallets"),
            "holds": await b.fetch("SELECT user_id, item_id, amount, status, txn_id FROM escrow_holds"),
            "items": await a.fetch("SELECT id, status, current_price, highest_bidder, seller_id FROM items"),
            "bids": await a.fetch("SELECT item_id, user_id, amount, txn_id FROM bids"),
            "txn": {r["txn_id"]: r for r in await a.fetch("SELECT txn_id, state, completed FROM coord.txn_log")},
            "prepared_a": await a.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'auc:%'"),
            "prepared_b": await b.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'auc:%'"),
        }
    finally:
        await a.close()
        await b.close()


def check(d: dict[str, Any], c: Checks, quiescent: bool = True) -> None:
    wallets, holds, items, bids, txn = d["wallets"], d["holds"], d["items"], d["bids"], d["txn"]
    total = sum(w["balance"] for w in wallets)
    supply = SUPPLY_PER_WALLET * len(wallets)
    held = sum(w["held"] for w in wallets)
    active = [h for h in holds if h["status"] == "active"]
    info(f"Σ balance = ${total}   Σ held = ${held}   Σ active holds = ${sum(h['amount'] for h in active)}   "
         f"wallets={len(wallets)} items={len(items)} bids={len(bids)} holds={len(holds)} txns={len(txn)}")

    c.check(total == supply, f"M1 money conserved: Σ balance {total} == supply {supply}")
    bad = [w["user_id"] for w in wallets if not (0 <= w["held"] <= w["balance"])]
    c.check(not bad, f"M2 0 ≤ held ≤ balance for all wallets {bad or ''}")
    by_user: dict[int, Decimal] = {}
    for h in active:
        by_user[h["user_id"]] = by_user.get(h["user_id"], Decimal(0)) + h["amount"]
    bad = [w["user_id"] for w in wallets if w["held"] != by_user.get(w["user_id"], Decimal(0))]
    c.check(not bad, f"M3 held == Σ active holds per wallet {bad or ''}")
    per_item: dict[int, int] = {}
    for h in active:
        per_item[h["item_id"]] = per_item.get(h["item_id"], 0) + 1
    c.check(all(n == 1 for n in per_item.values()), "M4 at most one active hold per item")

    bids_by_item: dict[int, list[Any]] = {}
    for bd in bids:
        bids_by_item.setdefault(bd["item_id"], []).append(bd)
    holds_by_item: dict[int, list[Any]] = {}
    for h in holds:
        holds_by_item.setdefault(h["item_id"], []).append(h)
    x1, x2, x3, a1, a2 = [], [], [], [], []
    for it in items:
        ib, ih = bids_by_item.get(it["id"], []), holds_by_item.get(it["id"], [])
        act = [h for h in ih if h["status"] == "active"]
        settled = [h for h in ih if h["status"] == "settled"]
        if not ib:
            if ih:
                x2.append(it["id"])
            continue
        top = max(bd["amount"] for bd in ib)
        tops = [bd for bd in ib if bd["amount"] == top]
        if len(tops) != 1:
            a2.append(it["id"])
        if it["current_price"] != top or it["highest_bidder"] != tops[0]["user_id"]:
            a1.append(it["id"])
        if it["status"] == "open":
            if not (len(act) == 1 and act[0]["user_id"] == it["highest_bidder"] and act[0]["amount"] == it["current_price"]):
                x1.append(it["id"])
        elif it["status"] == "sold":
            if act or not (len(settled) == 1 and settled[0]["user_id"] == it["highest_bidder"]
                           and settled[0]["amount"] == it["current_price"]):
                x3.append(it["id"])
    c.check(not x1, f"X1 open items: one active hold = highest bid {x1 or ''}")
    c.check(not x2, f"X2 items without bids hold no money {x2 or ''}")
    c.check(not x3, f"X3 sold items: winner's hold settled to seller {x3 or ''}")
    orphan = [str(r["txn_id"])[:8] for r in list(bids) + list(holds)
              if txn.get(r["txn_id"]) is None or txn[r["txn_id"]]["state"] != "COMMITTED"]
    c.check(not orphan, f"X4 every bid/hold comes from a COMMITTED 2PC txn {orphan[:5] or ''}")
    c.check(not a1, f"A1 current_price/highest_bidder match the max bid {a1 or ''}")
    c.check(not a2, f"A2 exactly one bid at the winning price per item {a2 or ''}")
    if quiescent:
        pending = sum(1 for t in txn.values() if not t["completed"])
        c.check(d["prepared_a"] == 0 and d["prepared_b"] == 0 and pending == 0,
                f"T1 no in-doubt txns: prepared A={d['prepared_a']} B={d['prepared_b']}, txn_log incomplete={pending}")


async def wait_quiescent(timeout: float = 15) -> None:
    """In-doubt txns are finished by the resolver within a few seconds; wait for that."""
    import time
    t0 = time.time()
    while time.time() - t0 < timeout:
        d = await collect()
        if d["prepared_a"] == 0 and d["prepared_b"] == 0 and all(t["completed"] for t in d["txn"].values()):
            return
        await asyncio.sleep(1)


async def run(c: Checks | None = None, quiescent: bool = True) -> Checks:
    c = c or Checks()
    if quiescent:
        await wait_quiescent()
    check(await collect(), c, quiescent)
    return c


async def main() -> None:
    banner("Invariant check [T4]")
    step("Money, cross-shard consistency, auction state, 2PC log")
    c = await run(quiescent="--live" not in sys.argv)
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
