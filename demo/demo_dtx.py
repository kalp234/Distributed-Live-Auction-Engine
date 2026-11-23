"""[T4] Distributed transactions demo — a bid is a 2PC transaction across two DB clusters.

 (a) normal bid: INIT -> both PREPARED -> COMMITTED -> COMMIT PREPARED on both shards
 (b) insufficient funds: Shard B votes NO -> Shard A's prepared part is rolled back
 (c) failure between PREPARE and COMMIT on one shard
     c1  Shard B fails at PREPARE (after A prepared)          -> global ABORT
     c2  Shard B unreachable in phase 2 (after COMMIT decision) -> stays in doubt, the
         resolver commits it from the durable log
 (d) 100 concurrent bids on one item + 20 identical bids on one price slot
 (e) idempotency: 5 parallel retries with one Idempotency-Key -> exactly one bid
 ... then the invariant checker proves no money was created or lost.
"""
from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections import Counter
from typing import Any

import httpx

from demo import invariants
from demo.common import LB_URL, Checks, banner, bold, dim, green, info, red, step, yellow
from demo.db import connect

SELLER = 1


async def new_item(http: httpx.AsyncClient, title: str, start: float = 10) -> int:
    r = await http.post("/api/items", json={"title": title, "start_price": start, "duration_s": 900},
                        headers={"X-User-Id": str(SELLER)})
    return r.json()["id"]


async def bid(http: httpx.AsyncClient, item: int, user: int, amount: float, via: str | None = None,
              key: str | None = None) -> httpx.Response:
    url = f"http://{via}:8000/api/items/{item}/bids" if via else f"/api/items/{item}/bids"
    return await http.post(url, json={"amount": amount},
                           headers={"X-User-Id": str(user), "Idempotency-Key": key or str(uuid.uuid4())})


async def arm(http: httpx.AsyncClient, node: str, name: str, arg: float | None = None) -> None:
    await http.post(f"http://{node}:8000/admin/faults", json={"name": name, "arg": arg})


async def snapshot(item: int, user: int) -> dict[str, Any]:
    a, b = await connect("A"), await connect("B")
    try:
        it = await a.fetchrow("SELECT current_price, highest_bidder, version FROM items WHERE id = $1", item)
        nb = await a.fetchval("SELECT count(*) FROM bids WHERE item_id = $1", item)
        w = await b.fetchrow("SELECT balance, held FROM wallets WHERE user_id = $1", user)
        pa = await a.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'auc:%'")
        pb = await b.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'auc:%'")
        last = await a.fetchrow("SELECT txn_id, state, completed FROM coord.txn_log ORDER BY ts DESC LIMIT 1")
        return {"price": float(it["current_price"]), "top": it["highest_bidder"], "version": it["version"], "bids": nb,
                "balance": float(w["balance"]), "held": float(w["held"]), "prepared": (pa, pb),
                "last_txn": (str(last["txn_id"])[:8], last["state"], last["completed"]) if last else None}
    finally:
        await a.close()
        await b.close()


def show(label: str, s: dict[str, Any]) -> None:
    info(f"{label:7} item: price=${s['price']:.2f} top=user {s['top']} v{s['version']} bids={s['bids']} │ "
         f"wallet: balance=${s['balance']:.2f} held=${s['held']:.2f} │ prepared A/B={s['prepared'][0]}/{s['prepared'][1]} │ "
         f"last txn {s['last_txn']}")


async def txn_state(txn_id: str) -> tuple[str, bool, bool, bool]:
    a, b = await connect("A"), await connect("B")
    try:
        r = await a.fetchrow("SELECT state, completed FROM coord.txn_log WHERE txn_id = $1::uuid", txn_id)
        pa = await a.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid = $1", f"auc:{txn_id}:A")
        pb = await b.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid = $1", f"auc:{txn_id}:B")
        return r["state"], r["completed"], bool(pa), bool(pb)
    finally:
        await a.close()
        await b.close()


async def main() -> None:
    banner("DEMO [T4] Distributed transactions (2PC across Shard A + Shard B)")
    c = Checks()
    http = httpx.AsyncClient(base_url=LB_URL, timeout=30)

    # ---------------------------------------------------------------- (a)
    step("(a) Normal bid — atomic across both shards")
    item = await new_item(http, "2PC demo (a)")
    before = await snapshot(item, 3)
    show("before", before)
    r = await bid(http, item, 3, 50)
    body = r.json()
    info(f"POST bid $50 by user 3 → {r.status_code} {body['status']} txn={body['txn_id'][:8]} via {r.headers['x-served-by']}")
    after = await snapshot(item, 3)
    show("after", after)
    st = await txn_state(body["txn_id"])
    info(f"txn_log: state={st[0]} completed={st[1]}  prepared on A={st[2]} B={st[3]}")
    c.check(after["bids"] == 1 and after["price"] == 50 and after["held"] == before["held"] + 50 and st[:2] == ("COMMITTED", True),
            "bid row on Shard A AND $50 escrow hold on Shard B, txn COMMITTED + completed")

    info(yellow("watching the protocol from outside (phase 2 delayed 2 s on app1):"))
    base4 = (await snapshot(item, 4))["held"]
    await arm(http, "app1", "delay_before_phase2", 2.0)
    task = asyncio.create_task(bid(http, item, 4, 60, via="app1"))
    seen: list[str] = []
    t0 = time.time()
    while not task.done():
        s = await snapshot(item, 4)
        line = f"state={s['last_txn'][1]:9} completed={str(s['last_txn'][2]):5} prepared A/B={s['prepared'][0]}/{s['prepared'][1]}  " \
               f"visible price=${s['price']:.0f} user4 held=${s['held']:.0f}"
        if not seen or seen[-1] != line:
            info(dim(f"t={time.time() - t0:4.1f}s ") + line)
            seen.append(line)
        await asyncio.sleep(0.3)
    s = await snapshot(item, 4)
    info(dim(f"t={time.time() - t0:4.1f}s ") + f"state={s['last_txn'][1]:9} completed={s['last_txn'][2]!s:5} "
         f"prepared A/B={s['prepared'][0]}/{s['prepared'][1]}  visible price=${s['price']:.0f} user4 held=${s['held']:.0f}")
    c.check(any("COMMITTED completed=False prepared A/B=1/1" in x for x in seen),
            "observed decision COMMITTED while both participants still PREPARED (in doubt)")
    c.check(s["price"] == 60 and s["held"] == base4 + 60, "after phase 2 the bid and the hold became visible together")

    # ---------------------------------------------------------------- (b)
    step("(b) Insufficient funds — Shard B votes NO, Shard A's prepared part rolls back")
    before = await snapshot(item, 5)
    show("before", before)
    r = await bid(http, item, 5, 5000)
    info(f"POST bid $5000 by user 5 (balance $1000) → {r.status_code} {r.json()}")
    after = await snapshot(item, 5)
    show("after", after)
    c.check(r.status_code == 409 and after["bids"] == before["bids"] and after["price"] == before["price"]
            and after["version"] == before["version"] and after["held"] == before["held"] and after["last_txn"][1] == "ABORTED",
            "full abort: no bid, item unchanged on A, wallet unchanged on B, txn ABORTED")

    # ---------------------------------------------------------------- (c1)
    step("(c1) Failure at PREPARE on Shard B (Shard A already prepared) → global abort")
    await arm(http, "app2", "vote_no_b")
    before = await snapshot(item, 6)
    r = await bid(http, item, 6, 70, via="app2")
    info(f"POST bid $70 via app2 with fault vote_no_b → {r.status_code} {r.json().get('error')}")
    after = await snapshot(item, 6)
    show("after", after)
    c.check(r.status_code == 503 and after["bids"] == before["bids"] and after["price"] == before["price"]
            and after["prepared"] == (0, 0) and after["last_txn"][1] == "ABORTED",
            "A's PREPARED part was rolled back (ROLLBACK PREPARED); nothing left prepared")

    # ---------------------------------------------------------------- (c2)
    step("(c2) Shard B unreachable in phase 2 (after the COMMIT decision) → resolver finishes it")
    await arm(http, "app2", "lose_b_in_phase2")
    before = await snapshot(item, 6)
    r = await bid(http, item, 6, 75, via="app2")
    txn = r.json()["txn_id"]
    info(f"POST bid $75 via app2 with fault lose_b_in_phase2 → {r.status_code} {r.json()['status']} txn={txn[:8]}")
    s = await snapshot(item, 6)
    st = await txn_state(txn)
    show("in doubt", s)
    info(f"txn_log: state={st[0]} completed={st[1]} prepared A={st[2]} B={bold(str(st[3]))}  "
         f"→ the bid is visible on A but user 6's hold is still locked inside a prepared txn on B")
    c.check(st == ("COMMITTED", False, False, True) and s["price"] == 75 and s["held"] == before["held"],
            "decision COMMITTED, A committed, B still PREPARED (in doubt)")
    t0 = time.time()
    while (st := await txn_state(txn))[3] and time.time() - t0 < 20:
        await asyncio.sleep(0.5)
    s = await snapshot(item, 6)
    show("resolved", s)
    info(f"resolver committed Shard B's part {time.time() - t0:.1f}s later; txn_log completed={st[1]}")
    c.check(not st[3] and s["held"] == before["held"] + 75, "resolver applied COMMIT PREPARED on B: hold now visible")

    # ---------------------------------------------------------------- (d)
    step("(d) 100 concurrent bids on ONE item from 8 users (through HAProxy → 3 nodes)")
    item_d = await new_item(http, "2PC demo (d) — 100 concurrent bids")
    amounts = [random.choice(range(11, 61)) for _ in range(100)]
    t0 = time.time()
    rs = await asyncio.gather(*(bid(http, item_d, 3 + i % 8, a) for i, a in enumerate(amounts)))
    dt = time.time() - t0
    outcome = Counter(x.json().get("status") or x.json().get("error") for x in rs)
    nodes = Counter(x.headers.get("x-served-by") for x in rs)
    info(f"100 bids in {dt:.2f}s → {dict(outcome)}  via {dict(nodes)}")
    a = await connect("A")
    top = await a.fetchrow("SELECT current_price, highest_bidder FROM items WHERE id = $1", item_d)
    winners = await a.fetchval("SELECT count(*) FROM bids WHERE item_id = $1 AND amount = $2", item_d, top["current_price"])
    await a.close()
    info(f"final price ${top['current_price']} (max requested ${max(amounts)}) by user {top['highest_bidder']}; "
         f"bids at that price: {winners}")
    c.check(outcome.get("accepted", 0) >= 1 and set(outcome) <= {"accepted", "bid_too_low"},
            "every request either committed or was cleanly rejected (no 5xx)")
    c.check(float(top["current_price"]) == max(amounts) and winners == 1, "exactly one highest bid = max requested amount")

    item_s = await new_item(http, "2PC demo (d) — same price slot")
    rs = await asyncio.gather(*(bid(http, item_s, 3 + i % 8, 100) for i in range(20)))
    outcome = Counter(x.json().get("status") or x.json().get("error") for x in rs)
    info(f"20 concurrent bids of exactly $100 → {dict(outcome)}")
    c.check(outcome.get("accepted") == 1, "exactly one bid wins the $100 price slot")

    # ---------------------------------------------------------------- (e)
    step("(e) Idempotency — 5 parallel retries of one request (same Idempotency-Key)")
    key = str(uuid.uuid4())
    before = await snapshot(item_s, 9)
    rs = await asyncio.gather(*(bid(http, item_s, 9, 150, key=key) for _ in range(5)))
    outcome = Counter(x.json().get("status") or x.json().get("error") for x in rs)
    after = await snapshot(item_s, 9)
    info(f"responses: {dict(outcome)}; bids on item {before['bids']} → {after['bids']}; "
         f"user 9 held ${before['held']:.0f} → ${after['held']:.0f}")
    c.check(after["bids"] == before["bids"] + 1 and after["held"] == before["held"] + 150,
            "exactly one bid and one $150 hold — never double-charged")
    ids = {x.json().get("bid_id") for x in rs}
    c.check(outcome.get("accepted") == 1 and outcome.get("duplicate") == 4 and len(ids) == 1,
            "the 4 retries got the ORIGINAL result back (status=duplicate, same bid_id)")

    await http.aclose()
    step("Invariants after all scenarios")
    await asyncio.sleep(0.5)
    await invariants.run(c)
    c.finish()


if __name__ == "__main__":
    asyncio.run(main())
