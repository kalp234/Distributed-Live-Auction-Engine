"""FastAPI entry point for one stateless app node (app1/app2/app3)."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from .config import settings
from .topology import ShardTopology

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {settings.node_id} %(name)s %(levelname)s %(message)s")
log = logging.getLogger("main")

STARTED_AT = time.time()


class State:
    shard_a: ShardTopology
    shard_b: ShardTopology
    tasks: list[asyncio.Task[Any]]


state = State()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    state.shard_a = ShardTopology("A", settings.db_a_name, settings.patroni_a,
                                  settings.db_user, settings.db_password, settings.max_replica_lag_bytes)
    state.shard_b = ShardTopology("B", settings.db_b_name, settings.patroni_b,
                                  settings.db_user, settings.db_password, settings.max_replica_lag_bytes)
    # First discovery synchronously so the node is useful as soon as it's up.
    await asyncio.gather(state.shard_a.refresh(), state.shard_b.refresh(), return_exceptions=True)
    state.tasks = [
        asyncio.create_task(state.shard_a.run(settings.topology_interval)),
        asyncio.create_task(state.shard_b.run(settings.topology_interval)),
    ]
    log.info("node %s started; A leader=%s B leader=%s", settings.node_id,
             state.shard_a.leader_name, state.shard_b.leader_name)
    yield
    for t in state.tasks:
        t.cancel()
    await state.shard_a.close()
    await state.shard_b.close()


app = FastAPI(title="Distributed Live Auction Engine", lifespan=lifespan)


@app.middleware("http")
async def served_by(request: Request, call_next: Any) -> Response:
    """[T6] Every response says which app node produced it."""
    response: Response = await call_next(request)
    response.headers["X-Served-By"] = settings.node_id
    return response


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Liveness: the process is up and its event loop responds."""
    return {"node": settings.node_id, "status": "alive", "uptime_s": round(time.time() - STARTED_AT, 1)}


@app.get("/readyz")
async def readyz() -> Response:
    """[T6] Readiness for HAProxy. Ready = can serve at least reads from Shard A.
    A primary failover does NOT make nodes unready (reads keep flowing from replicas [T3]);
    write availability is reported separately."""
    a, b = state.shard_a, state.shard_b
    body = {
        "node": settings.node_id,
        "ready": a.readable,
        "writes_available": a.leader_name is not None and b.leader_name is not None,
        "shard_a_leader": a.leader_name,
        "shard_b_leader": b.leader_name,
    }
    return JSONResponse(body, status_code=200 if body["ready"] else 503)


@app.get("/api/topology")
async def topology() -> dict[str, Any]:
    return {"node": settings.node_id, "shards": [state.shard_a.snapshot(), state.shard_b.snapshot()]}


@app.get("/api/whoami")
async def whoami() -> dict[str, Any]:
    """Round-trips to the Shard A primary and one replica; shows routing in action."""
    out: dict[str, Any] = {"node": settings.node_id}
    for label, getter in (("primary", state.shard_a.primary), ("replica", state.shard_a.replica)):
        try:
            name, pool = await getter()
            row = await pool.fetchrow("SELECT inet_server_addr()::text AS addr, pg_is_in_recovery() AS in_recovery")
            out[label] = {"member": name, "in_recovery": row["in_recovery"]}
        except Exception as e:
            out[label] = {"error": str(e)}
    return out
