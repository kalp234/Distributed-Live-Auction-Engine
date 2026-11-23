"""Cross-node live fan-out: WS clients on app1/app2/app3 all see a bid placed via one node."""
from __future__ import annotations

import asyncio
import json
import sys
import uuid

import httpx
import websockets

from demo.common import APPS, Checks, banner, info, step


async def collect(node: str, out: dict[str, list[dict]], ready: asyncio.Event, n_ready: list[int]) -> None:
    async with websockets.connect(f"ws://{node}:8000/ws") as ws:
        hello = json.loads(await ws.recv())
        out[node].append(hello)
        n_ready[0] += 1
        if n_ready[0] == len(APPS):
            ready.set()
        try:
            while True:
                out[node].append(json.loads(await asyncio.wait_for(ws.recv(), timeout=4)))
        except asyncio.TimeoutError:
            pass


async def main(via: str) -> None:
    banner(f"Live fan-out check (bid via {via})")
    c = Checks()
    out: dict[str, list[dict]] = {a: [] for a in APPS}
    ready, n = asyncio.Event(), [0]
    tasks = [asyncio.create_task(collect(a, out, ready, n)) for a in APPS]
    await asyncio.wait_for(ready.wait(), 5)
    async with httpx.AsyncClient() as http:
        items = (await http.get(f"http://{via}:8000/api/items?status=open")).json()
        it = items[0]
        amount = it["current_price"] + 7
        bidder = next(u for u in range(3, 11) if u != it["seller_id"] and u != it["highest_bidder"])
        step(f"POST bid ${amount} on item #{it['id']} via {via}")
        r = await http.post(f"http://{via}:8000/api/items/{it['id']}/bids", json={"amount": amount},
                            headers={"X-User-Id": str(bidder), "Idempotency-Key": str(uuid.uuid4())})
        info(f"{r.status_code} {r.text[:120]}")
    await asyncio.gather(*tasks)
    step("Events received by each node's WebSocket clients")
    for a in APPS:
        bids = [e for e in out[a] if e.get("type") == "bid" and e.get("item_id") == it["id"] and e.get("amount") == amount]
        c.check(len(bids) == 1, f"client on {a} (hello from {out[a][0]['node']}, mode={out[a][0]['mode']}) got bid event: {bool(bids)}"
                + (f" published by {bids[0]['node']}" if bids else ""))
    c.finish()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "app2"))
