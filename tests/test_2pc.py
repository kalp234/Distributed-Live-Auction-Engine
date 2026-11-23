"""[T4] 2PC paths against the running stack."""
from __future__ import annotations

import asyncio
import time

import pytest

from tests.conftest import arm, bid, new_item, txn_state, wallet

pytestmark = pytest.mark.integration


async def test_commit_applies_to_both_shards(http):
    item = await new_item(http)
    w0 = await wallet(3)
    r = await bid(http, item, 3, 40)
    assert r.status_code == 200 and r.json()["status"] == "accepted"
    st = await txn_state(r.json()["txn_id"])
    assert st == {"state": "COMMITTED", "completed": True, "prepared_A": False, "prepared_B": False}
    assert (await wallet(3))["held"] == w0["held"] + 40
    detail = (await http.get(f"/api/items/{item}", headers={"X-Min-LSN": r.headers["x-write-lsn"]})).json()
    assert detail["current_price"] == 40 and detail["highest_bidder"] == 3


async def test_outbid_releases_previous_hold(http):
    item = await new_item(http)
    w3, w4 = await wallet(3), await wallet(4)
    assert (await bid(http, item, 3, 20)).status_code == 200
    r = await bid(http, item, 4, 25)
    assert r.json()["released_hold"] == {"user_id": 3, "amount": 20.0}
    assert (await wallet(3))["held"] == w3["held"] and (await wallet(4))["held"] == w4["held"] + 25


async def test_insufficient_funds_aborts_everywhere(http):
    item = await new_item(http)
    before = (await http.get(f"/api/items/{item}")).json()
    r = await bid(http, item, 5, 99999)
    assert r.status_code == 409 and r.json()["error"] == "insufficient_funds"
    after = (await http.get(f"/api/items/{item}")).json()
    assert after["version"] == before["version"] and after["bids"] == []


async def test_vote_no_on_b_rolls_back_prepared_a(http):
    item = await new_item(http)
    await arm(http, "app1", "vote_no_b")
    r = await bid(http, item, 6, 30, via="app1")
    assert r.status_code == 503 and r.json()["error"] == "participant_vote_no"
    after = (await http.get(f"http://app1:8000/api/items/{item}")).json()
    assert after["bids"] == [] and after["version"] == 0


async def test_phase2_loss_is_finished_by_resolver(http):
    item = await new_item(http)
    w = await wallet(7)
    await arm(http, "app2", "lose_b_in_phase2")
    r = await bid(http, item, 7, 33, via="app2")
    assert r.status_code == 200
    txn = r.json()["txn_id"]
    st = await txn_state(txn)
    assert st["state"] == "COMMITTED" and st["prepared_B"] and not st["prepared_A"]
    t0 = time.time()
    while (await txn_state(txn))["prepared_B"] and time.time() - t0 < 20:
        await asyncio.sleep(0.5)
    assert not (await txn_state(txn))["prepared_B"]
    assert (await wallet(7))["held"] == w["held"] + 33
