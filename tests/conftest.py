"""Shared fixtures. Integration tests talk to the running stack (run via `make test`, which
executes pytest inside the tools container on the compose network)."""
from __future__ import annotations

import os
import uuid
from typing import Any, AsyncIterator

import httpx
import pytest

LB = os.environ.get("LB_URL", "http://haproxy:80")
SELLER = 1


@pytest.fixture(scope="session")
async def reset_db() -> None:
    """Integration tests start from a freshly seeded database (money supply = $10,000)."""
    from demo import seed
    await seed.main(reset=True)


@pytest.fixture(scope="session")
async def http(reset_db: None) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=LB, timeout=30) as c:
        yield c


async def new_item(http: httpx.AsyncClient, start: float = 10, duration_s: int = 900) -> int:
    r = await http.post("/api/items", json={"title": f"test-{uuid.uuid4().hex[:6]}", "start_price": start,
                                            "duration_s": duration_s}, headers={"X-User-Id": str(SELLER)})
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def bid(http: httpx.AsyncClient, item: int, user: int, amount: float, *, key: str | None = None,
              via: str | None = None) -> httpx.Response:
    url = f"http://{via}:8000/api/items/{item}/bids" if via else f"/api/items/{item}/bids"
    return await http.post(url, json={"amount": amount},
                           headers={"X-User-Id": str(user), "Idempotency-Key": key or str(uuid.uuid4())})


async def arm(http: httpx.AsyncClient, node: str, name: str, arg: float | None = None) -> None:
    r = await http.post(f"http://{node}:8000/admin/faults", json={"name": name, "arg": arg})
    assert r.status_code == 200, r.text


async def wallet(user: int) -> dict[str, Any]:
    from demo.db import connect
    b = await connect("B")
    try:
        return dict(await b.fetchrow("SELECT balance, held FROM wallets WHERE user_id = $1", user))
    finally:
        await b.close()


async def txn_state(txn_id: str) -> dict[str, Any]:
    from demo.db import connect
    a, b = await connect("A"), await connect("B")
    try:
        r = await a.fetchrow("SELECT state, completed FROM coord.txn_log WHERE txn_id = $1::uuid", txn_id)
        return {"state": r["state"] if r else None, "completed": r["completed"] if r else None,
                "prepared_A": bool(await a.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid=$1", f"auc:{txn_id}:A")),
                "prepared_B": bool(await b.fetchval("SELECT count(*) FROM pg_prepared_xacts WHERE gid=$1", f"auc:{txn_id}:B"))}
    finally:
        await a.close()
        await b.close()
