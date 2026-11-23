"""FastAPI entry point for one stateless app node (app1/app2/app3)."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import asyncpg
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from . import db, recovery
from .closer import Closer
from .api import router
from .config import settings
from .errors import AppError
from .realtime import Hub
from .runtime import rt
from .topology import NoPrimaryError, ShardTopology

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {settings.node_id} %(name)s %(levelname)s %(message)s")
log = logging.getLogger("main")

STARTED_AT = time.time()
STATIC = Path(__file__).parent / "static"


async def on_topology_event(kind: str, data: dict[str, Any]) -> None:
    """Forward DB topology changes (e.g. a new primary) to live clients + dashboard."""
    if hasattr(rt, "hub"):
        await rt.hub.broadcast_local({"type": kind, **data, "node": settings.node_id, "ts": time.time()})


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    rt.shard_a = ShardTopology("A", settings.db_a_name, settings.patroni_a, settings.db_user,
                               settings.db_password, settings.max_replica_lag_bytes, on_topology_event)
    rt.shard_b = ShardTopology("B", settings.db_b_name, settings.patroni_b, settings.db_user,
                               settings.db_password, settings.max_replica_lag_bytes, on_topology_event)
    rt.hub = Hub(settings.node_id, settings.redis_url, board_loader=lambda: db.load_board(rt.shard_a))
    # First discovery before serving so the node is useful immediately.
    await asyncio.gather(rt.shard_a.refresh(), rt.shard_b.refresh(), return_exceptions=True)
    rt.closer = Closer()
    rt.tasks = [
        asyncio.create_task(rt.shard_a.run(settings.topology_interval)),
        asyncio.create_task(rt.shard_b.run(settings.topology_interval)),
        asyncio.create_task(rt.hub.run()),
        asyncio.create_task(rt.closer.run()),
        asyncio.create_task(recovery.run()),  # [T5] startup + periodic in-doubt resolution
    ]
    log.info("node %s started; A leader=%s B leader=%s", settings.node_id,
             rt.shard_a.leader_name, rt.shard_b.leader_name)
    yield
    for t in rt.tasks:
        t.cancel()
    await rt.shard_a.close()
    await rt.shard_b.close()


app = FastAPI(title="Distributed Live Auction Engine", lifespan=lifespan)
app.include_router(router)


@app.middleware("http")
async def served_by(request: Request, call_next: Any) -> Response:
    """[T6] Every response says which app node produced it."""
    response: Response = await call_next(request)
    response.headers["X-Served-By"] = settings.node_id
    return response


@app.exception_handler(AppError)
async def app_error(_: Request, e: AppError) -> JSONResponse:
    return JSONResponse({"error": e.code, "message": e.message, "node": settings.node_id}, status_code=e.status)


@app.exception_handler(NoPrimaryError)
async def no_primary(_: Request, e: NoPrimaryError) -> JSONResponse:
    # [T3] Mid-failover: writes are briefly unavailable; tell the client to retry.
    return JSONResponse({"error": "no_primary", "message": str(e), "node": settings.node_id},
                        status_code=503, headers={"Retry-After": "1"})


@app.exception_handler(asyncpg.PostgresError)
@app.exception_handler(OSError)
async def db_error(_: Request, e: Exception) -> JSONResponse:
    log.warning("db error: %r", e)
    return JSONResponse({"error": "db_unavailable", "message": f"{type(e).__name__}: {e}",
                         "node": settings.node_id}, status_code=503, headers={"Retry-After": "1"})


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Liveness: the process is up and its event loop responds."""
    return {"node": settings.node_id, "status": "alive", "uptime_s": round(time.time() - STARTED_AT, 1)}


@app.get("/readyz")
async def readyz() -> Response:
    """[T6] Readiness for HAProxy. Ready = can serve at least reads from Shard A.
    A primary failover does NOT make nodes unready (reads keep flowing from replicas [T3]);
    write availability is reported separately."""
    a, b = rt.shard_a, rt.shard_b
    body = {
        "node": settings.node_id,
        "ready": a.readable,
        "writes_available": a.leader_name is not None and b.leader_name is not None,
        "shard_a_leader": a.leader_name,
        "shard_b_leader": b.leader_name,
        "realtime": rt.hub.snapshot()["mode"],
    }
    return JSONResponse(body, status_code=200 if body["ready"] else 503)


@app.get("/api/topology")
async def topology() -> dict[str, Any]:
    return {"node": settings.node_id, "shards": [rt.shard_a.snapshot(), rt.shard_b.snapshot()]}


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store"})
