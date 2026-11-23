"""[T4] Idempotency keys and concurrency control."""
from __future__ import annotations

import asyncio
import uuid
from collections import Counter

import pytest

from demo import invariants
from demo.common import Checks
from tests.conftest import bid, new_item, wallet

pytestmark = pytest.mark.integration


async def test_sequential_retry_returns_original(http):
    item = await new_item(http)
    key = str(uuid.uuid4())
    r1, r2 = await bid(http, item, 3, 15, key=key), await bid(http, item, 3, 15, key=key)
    assert r1.json()["status"] == "accepted" and r2.json()["status"] == "duplicate"
    assert r1.json()["bid_id"] == r2.json()["bid_id"] and r1.json()["txn_id"] == r2.json()["txn_id"]


async def test_parallel_retries_create_one_bid(http):
    item = await new_item(http)
    key, w = str(uuid.uuid4()), await wallet(8)
    rs = await asyncio.gather(*(bid(http, item, 8, 22, key=key) for _ in range(6)))
    assert Counter(r.json()["status"] for r in rs) == {"accepted": 1, "duplicate": 5}
    assert len({r.json()["bid_id"] for r in rs}) == 1
    assert (await wallet(8))["held"] == w["held"] + 22


async def test_rejected_attempt_does_not_burn_the_key(http):
    item = await new_item(http)
    key = str(uuid.uuid4())
    assert (await bid(http, item, 9, 99999, key=key)).status_code == 409   # aborted: nothing remembered
    assert (await bid(http, item, 9, 12, key=key)).json()["status"] == "accepted"


async def test_one_winner_per_price_slot(http):
    item = await new_item(http)
    rs = await asyncio.gather(*(bid(http, item, 3 + i % 8, 77) for i in range(16)))
    assert Counter(r.json().get("status") or r.json()["error"] for r in rs) == {"accepted": 1, "bid_too_low": 15}


async def test_concurrent_increasing_bids_keep_single_highest(http):
    item = await new_item(http)
    amounts = list(range(11, 61))
    rs = await asyncio.gather(*(bid(http, item, 3 + i % 8, a) for i, a in enumerate(amounts)))
    assert all(r.status_code in (200, 409) for r in rs)
    d = (await http.get(f"/api/items/{item}")).json()
    assert d["current_price"] == 60
    assert [b["amount"] for b in d["bids"]].count(60) == 1


async def test_wallet_cannot_be_overcommitted_across_items(http):
    """Same user bids $300 on 5 items at once with $1000 free: wallet row lock allows only 3."""
    user = 10
    free = (lambda w: w["balance"] - w["held"])(await wallet(user))
    items = [await new_item(http) for _ in range(5)]
    rs = await asyncio.gather(*(bid(http, it, user, 300) for it in items))
    ok = sum(r.status_code == 200 for r in rs)
    assert ok == int(free // 300)
    assert Counter(r.json().get("error") for r in rs if r.status_code != 200) == {"insufficient_funds": 5 - ok}
    w = await wallet(user)
    assert w["held"] <= w["balance"]


async def test_invariants_hold():
    c = Checks()
    await invariants.run(c)
    assert not c.failed, c.failed
