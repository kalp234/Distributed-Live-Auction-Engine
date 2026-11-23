"""REST + WebSocket routes."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Header, Response, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from . import db, faults, recovery
from .bidding import place_bid as do_bid
from .config import settings
from .coordinator import TRANSIENT
from .coordinator import stats as tpc_stats
from .errors import AppError
from .resilience import retry
from .runtime import rt

log = logging.getLogger("api")
router = APIRouter()


def _read_headers(resp: Response, r: db.ReadResult) -> None:
    resp.headers["X-DB-Node"] = r.member
    if r.fallback:
        resp.headers["X-Read-Fallback"] = r.fallback


def _item(row: Any) -> dict[str, Any]:
    d = dict(row)
    for k in ("start_price", "current_price"):
        if isinstance(d.get(k), Decimal):
            d[k] = float(d[k])
    return d


ITEM_COLS = """i.id, i.title, i.seller_id, i.start_price, i.current_price, i.highest_bidder,
               hb.name AS highest_bidder_name, i.status, i.ends_at, i.version"""


# ----------------------------------------------------------------------------- reads [T6]
@router.get("/api/users")
async def users(response: Response) -> list[dict[str, Any]]:
    ra = await db.read(rt.shard_a, "SELECT id, name FROM users ORDER BY id")
    rb = await db.read(rt.shard_b, "SELECT user_id, balance, held FROM wallets")
    wallets = {r["user_id"]: r for r in rb.rows}
    _read_headers(response, ra)
    out = []
    for u in ra.rows:
        w = wallets.get(u["id"])
        out.append({"id": u["id"], "name": u["name"],
                    "balance": float(w["balance"]) if w else None,
                    "held": float(w["held"]) if w else None})
    return out


@router.get("/api/items")
async def list_items(response: Response, status: str | None = None,
                     x_min_lsn: str | None = Header(default=None)) -> list[dict[str, Any]]:
    sql = f"SELECT {ITEM_COLS} FROM items i LEFT JOIN users hb ON hb.id = i.highest_bidder"
    args: list[Any] = []
    if status:
        sql += " WHERE i.status = $1"
        args.append(status)
    sql += " ORDER BY (i.status = 'open') DESC, i.ends_at"
    r = await db.read(rt.shard_a, sql, *args, min_lsn=x_min_lsn)
    _read_headers(response, r)
    return [_item(x) for x in r.rows]


@router.get("/api/items/{item_id}")
async def get_item(item_id: int, response: Response,
                   x_min_lsn: str | None = Header(default=None)) -> dict[str, Any]:
    r = await db.read(rt.shard_a, f"""
        SELECT {ITEM_COLS},
               COALESCE((SELECT json_agg(b ORDER BY b.amount DESC) FROM (
                   SELECT bd.id, bd.user_id, u.name AS user_name, bd.amount::float8 AS amount,
                          bd.ts, bd.served_by, bd.txn_id
                   FROM bids bd JOIN users u ON u.id = bd.user_id
                   WHERE bd.item_id = i.id ORDER BY bd.amount DESC LIMIT 25) b), '[]') AS bids
        FROM items i LEFT JOIN users hb ON hb.id = i.highest_bidder
        WHERE i.id = $1""", item_id, min_lsn=x_min_lsn)
    _read_headers(response, r)
    if not r.rows:
        raise AppError(404, "not_found", f"item {item_id} not found")
    d = _item(r.rows[0])
    d["bids"] = json.loads(d["bids"]) if isinstance(d["bids"], str) else d["bids"]
    return d


# ----------------------------------------------------------------------------- writes
class CreateItem(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    start_price: float = Field(ge=0)
    duration_s: int = Field(default=300, ge=5, le=7 * 24 * 3600)


@router.post("/api/items", status_code=201)
async def create_item(body: CreateItem, response: Response,
                      x_user_id: int = Header()) -> dict[str, Any]:
    ends_at = datetime.now(timezone.utc) + timedelta(seconds=body.duration_s)
    async with db.write_conn(rt.shard_a) as (member, conn):
        row = await conn.fetchrow(
            """INSERT INTO items (title, seller_id, start_price, current_price, ends_at)
               VALUES ($1, $2, $3, $3, $4)
               RETURNING id, title, seller_id, start_price, current_price, highest_bidder,
                         NULL::text AS highest_bidder_name, status, ends_at, version""",
            body.title, x_user_id, Decimal(str(body.start_price)), ends_at)
        lsn = await db.current_lsn(conn)
    response.headers["X-DB-Node"] = member
    response.headers["X-Write-LSN"] = lsn
    item = _item(row)
    await rt.hub.publish({"type": "item_created", "item": item})
    return item


def _retryable(e: BaseException) -> bool:
    if isinstance(e, AppError):
        return e.code in ("outcome_unknown", "aborted_by_recovery")
    return isinstance(e, TRANSIENT)


class PlaceBid(BaseModel):
    amount: float = Field(gt=0)


@router.post("/api/items/{item_id}/bids")
async def place_bid(item_id: int, body: PlaceBid, response: Response,
                    x_user_id: int = Header(),
                    idempotency_key: str | None = Header(default=None)) -> dict[str, Any]:
    # [T2] Retry transient failures (primary briefly unreachable, connection reset, outcome
    # unknown) with backoff. Safe because of the idempotency key: a retry of an attempt that
    # actually committed returns the original bid instead of creating a second one.
    key = idempotency_key or f"auto-{uuid.uuid4()}"
    result = await retry(lambda: do_bid(item_id, x_user_id, Decimal(str(body.amount)), key),
                         attempts=3, base_delay=0.2, retry_if=_retryable, what="place_bid")
    response.headers["X-DB-Node"] = result.pop("_db_node", "")
    if lsn := result.pop("_lsn", None):
        response.headers["X-Write-LSN"] = lsn
    return result


@router.get("/api/live")
async def live_board(response: Response) -> dict[str, Any]:
    """Cheap polling endpoint: live board from Redis, or from the DB if Redis is down [T2]."""
    board = await rt.hub.read_board()
    if board is not None:
        return {"source": "redis", "items": board}
    return {"source": "db", "items": await db.load_board(rt.shard_a)}


# ----------------------------------------------------------------------------- status
@router.get("/api/replication")
async def replication() -> dict[str, Any]:
    """[T1] Replication lag per replica (bytes + ms) from the primaries' pg_stat_replication."""
    await rt.shard_a._refresh_replication()  # live values, not the 1 s cache
    await rt.shard_b._refresh_replication()
    return {"node": settings.node_id,
            "shards": {s.shard: {"leader": s.leader_name, "replicas": s.snapshot()["replication"]}
                       for s in (rt.shard_a, rt.shard_b)}}


@router.get("/api/node")
async def node() -> dict[str, Any]:
    return {"node": settings.node_id, "realtime": rt.hub.snapshot(),
            "two_pc": {"committed": tpc_stats.committed, "aborted": tpc_stats.aborted,
                       "phase2_deferred": tpc_stats.phase2_deferred, "abort_reasons": tpc_stats.by_reason},
            "faults": faults.armed(),
            "shards": [rt.shard_a.snapshot(), rt.shard_b.snapshot()]}


@router.get("/api/txn/{txn_id}")
async def txn(txn_id: str) -> dict[str, Any]:
    """[T4][T5] Coordinator log entry + whether each participant still holds it prepared."""
    _, pa = await rt.shard_a.primary()
    row = await pa.fetchrow("SELECT txn_id, kind, state, participants, coordinator, detail, completed, ts, updated_at"
                            " FROM coord.txn_log WHERE txn_id = $1::uuid", txn_id)
    if row is None:
        raise AppError(404, "not_found", "unknown txn")
    out = dict(row)
    out["detail"] = json.loads(out["detail"]) if isinstance(out["detail"], str) else out["detail"]
    for s in (rt.shard_a, rt.shard_b):
        _, p = await s.primary()
        out[f"prepared_on_{s.shard}"] = bool(await p.fetchval(
            "SELECT count(*) FROM pg_prepared_xacts WHERE gid = $1", f"auc:{txn_id}:{s.shard}"))
    return out


# ----------------------------------------------------------------------------- admin (demos)
class FaultReq(BaseModel):
    name: str
    count: int = 1
    arg: float | None = None


@router.post("/admin/faults")
async def arm_fault(body: FaultReq) -> dict[str, Any]:
    try:
        faults.arm(body.name, body.count, body.arg)
    except ValueError as e:
        raise AppError(400, "bad_fault", str(e))
    return {"node": settings.node_id, "armed": faults.armed()}


@router.delete("/admin/faults")
async def clear_faults() -> dict[str, Any]:
    faults.clear()
    return {"node": settings.node_id, "armed": {}}


@router.post("/admin/recover")
async def recover_now() -> dict[str, Any]:
    """Run one resolver pass immediately (ignores the grace period for this node's txns)."""
    return {"node": settings.node_id, "actions": await recovery.resolve_once(own_immediately=True)}


# ----------------------------------------------------------------------------- live [T6]
@router.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await rt.hub.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # clients only send keep-alive pings
    except WebSocketDisconnect:
        pass
    finally:
        rt.hub.disconnect(websocket)
