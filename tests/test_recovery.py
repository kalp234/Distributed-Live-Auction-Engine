"""[T5] Coordinator recovery: crash paths and the resolver's decision rules."""
from __future__ import annotations

import asyncio
import time
import uuid

import httpx
import pytest

from demo.db import connect
from tests.conftest import arm, bid, new_item, txn_state, wallet

pytestmark = pytest.mark.integration


async def wait_ready(node: str, timeout: float = 30) -> None:
    t0 = time.time()
    async with httpx.AsyncClient(timeout=1) as c:
        while time.time() - t0 < timeout:
            try:
                if (await c.get(f"http://{node}:8000/readyz")).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.2)
    raise TimeoutError(node)


async def last_txn(node: str) -> str:
    a = await connect("A")
    try:
        return str(await a.fetchval("SELECT txn_id FROM coord.txn_log WHERE coordinator = $1 ORDER BY ts DESC LIMIT 1", node))
    finally:
        await a.close()


async def test_crash_before_decision_is_rolled_back(http):
    item = await new_item(http)
    await arm(http, "app3", "crash_before_decision")
    with pytest.raises(httpx.HTTPError):
        await bid(http, item, 4, 30, via="app3")
    txn = await last_txn("app3")
    await wait_ready("app3")
    await asyncio.sleep(0.5)
    st = await txn_state(txn)
    assert st["state"] == "ABORTED" and not st["prepared_A"] and not st["prepared_B"]
    assert (await http.get(f"/api/items/{item}")).json()["bids"] == []


async def test_crash_after_decision_is_committed_and_retry_is_exactly_once(http):
    item = await new_item(http)
    key, w = str(uuid.uuid4()), await wallet(5)
    await arm(http, "app3", "crash_after_decision")
    with pytest.raises(httpx.HTTPError):
        await bid(http, item, 5, 30, via="app3", key=key)
    txn = await last_txn("app3")
    await wait_ready("app3")
    await asyncio.sleep(0.5)
    st = await txn_state(txn)
    assert st["state"] == "COMMITTED" and not st["prepared_A"] and not st["prepared_B"]
    r = await bid(http, item, 5, 30, key=key)
    assert r.json()["status"] == "duplicate" and r.json()["txn_id"] == txn
    assert (await wallet(5))["held"] == w["held"] + 30


async def test_recovery_abort_beats_slow_coordinator(http):
    """CAS safety: if recovery decides ABORT while the coordinator is between PREPARED and
    its COMMIT decision, the coordinator must follow (roll back), never commit."""
    item = await new_item(http)
    await arm(http, "app1", "delay_before_decision", 2.0)
    task = asyncio.create_task(bid(http, item, 6, 31, via="app1"))
    await asyncio.sleep(1.0)
    txn = await last_txn("app1")
    a = await connect("A")
    won = await a.fetchval("UPDATE coord.txn_log SET state = 'ABORTED' WHERE txn_id = $1::uuid"
                           " AND state IN ('INIT','PREPARED') RETURNING 1", txn)   # what the resolver does
    await a.close()
    assert won == 1
    r = await task
    st = await txn_state(txn)
    assert st["state"] == "ABORTED" and not st["prepared_A"] and not st["prepared_B"]
    # the API's retry then placed the bid in a NEW transaction (exactly one bid exists)
    bids = (await http.get(f"/api/items/{item}")).json()["bids"]
    assert r.status_code == 200 and len(bids) == 1 and bids[0]["txn_id"] != txn


async def _orphan(state: str | None, coordinator: str, uid: int) -> str:
    """Leave a prepared txn on both shards (as a dead coordinator would) + optional log row."""
    txn = str(uuid.uuid4())
    a, b = await connect("A"), await connect("B")
    try:
        if state:
            await a.execute("INSERT INTO coord.txn_log (txn_id, kind, state, participants, coordinator, ts)"
                            " VALUES ($1::uuid, 'test', $2, '{auction,wallet}', $3, now() - interval '1 minute')",
                            txn, state, coordinator)
        await a.execute("BEGIN")
        await a.execute("INSERT INTO users (id, name) VALUES ($1, 'orphan-test')", uid)
        await a.execute(f"PREPARE TRANSACTION 'auc:{txn}:A'")
        await b.execute("BEGIN")
        await b.execute("UPDATE wallets SET balance = balance WHERE user_id = 1")
        await b.execute(f"PREPARE TRANSACTION 'auc:{txn}:B'")
    finally:
        await a.close()
        await b.close()
    return txn


async def _user_exists(uid: int) -> bool:
    a = await connect("A")
    try:
        return bool(await a.fetchval("SELECT count(*) FROM users WHERE id = $1", uid))
    finally:
        await a.execute("DELETE FROM users WHERE id = $1", uid)
        await a.close()


@pytest.mark.parametrize("state, expect_commit", [("COMMITTED", True), ("PREPARED", False),
                                                  ("INIT", False), (None, False)])
async def test_resolver_follows_the_log(http, state, expect_commit):
    uid = 9000 + ["COMMITTED", "PREPARED", "INIT", None].index(state)
    await asyncio.sleep(0)  # keep ordering simple
    txn = await _orphan(state, "app1", uid)
    await asyncio.sleep(5.5)  # older than the resolver's grace period
    await http.post("http://app1:8000/admin/recover")
    st = await txn_state(txn)
    assert not st["prepared_A"] and not st["prepared_B"]
    assert await _user_exists(uid) == expect_commit
    if state:
        assert st["state"] == ("COMMITTED" if expect_commit else "ABORTED")


async def test_resolver_respects_grace_period_for_other_coordinators(http):
    txn = await _orphan("PREPARED", "app2", 9100)
    acts = (await http.post("http://app1:8000/admin/recover")).json()["actions"]
    assert all(x["txn_id"] != txn for x in acts)            # too young, owned by a live node
    assert (await txn_state(txn))["prepared_A"]
    await asyncio.sleep(5.5)
    await http.post("http://app1:8000/admin/recover")
    st = await txn_state(txn)
    assert st["state"] == "ABORTED" and not st["prepared_A"] and not st["prepared_B"]
    assert not await _user_exists(9100)
