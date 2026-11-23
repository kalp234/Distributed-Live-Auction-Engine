"""Live event fan-out.

[T6] Cross-node fan-out: a bid accepted on app1 is published to a Redis channel; every app
     node is subscribed and pushes it to *its own* WebSocket clients, so users connected to
     app2/app3 (via HAProxy) see it instantly.
[T2] Graceful degradation: if Redis is down, publishing falls back to local delivery and
     every client is told to switch to HTTP polling; when Redis returns, clients are told to
     go back to push mode. The database stays the source of truth, so nothing is lost —
     Redis only carries notifications plus a derived cache.
[T5] Redis loss recovery: Redis also holds a "live board" hash (current price / top bidder
     per open item) for cheap polling. Redis runs without persistence, so after a restart
     the hash is empty — on every (re)connect the hub rebuilds it from the database and
     tells clients to resync.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable

import redis.asyncio as aioredis
from fastapi import WebSocket

from .resilience import CircuitBreaker

log = logging.getLogger("realtime")
CHANNEL = "auction:events"
BOARD = "auction:board"
BoardLoader = Callable[[], Awaitable[dict[str, dict[str, Any]]]]


class Hub:
    def __init__(self, node_id: str, redis_url: str, board_loader: BoardLoader | None = None) -> None:
        self.board_loader = board_loader
        # [T2] Breaker: once Redis fails, publishes skip it immediately (no per-bid timeout)
        # until the subscribe loop reconnects.
        self.breaker = CircuitBreaker("redis", threshold=1, reset_timeout=1.0)
        self.board_rebuilds = 0
        self.node_id = node_id
        self.redis = aioredis.from_url(redis_url, socket_timeout=1.0, socket_connect_timeout=1.0,
                                       health_check_interval=2)
        self.clients: set[WebSocket] = set()
        self.redis_ok = False
        self.published = 0
        self.received = 0
        self.last_change = time.time()

    # ------------------------------------------------------------------ clients
    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)
        await ws.send_json({"type": "hello", "node": self.node_id,
                            "mode": "push" if self.redis_ok else "polling"})

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast_local(self, event: dict[str, Any]) -> None:
        dead: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await asyncio.wait_for(ws.send_json(event), timeout=1.0)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)

    # ------------------------------------------------------------------ publish
    async def publish(self, event: dict[str, Any]) -> None:
        event.setdefault("node", self.node_id)
        event.setdefault("ts", time.time())
        if self.redis_ok and self.breaker.allow():
            try:
                pipe = self.redis.pipeline(transaction=False)
                if event.get("type") == "bid":
                    pipe.hset(BOARD, str(event["item_id"]), json.dumps(
                        {"price": event["amount"], "bidder": event["user_id"], "version": event["version"]}))
                elif event.get("type") == "closed":
                    pipe.hdel(BOARD, str(event["item_id"]))
                pipe.publish(CHANNEL, json.dumps(event, default=str))
                await pipe.execute()
                self.breaker.success()
                self.published += 1
                return  # our own subscription will deliver it to local clients
            except Exception as e:
                self.breaker.failure()
                log.warning("redis publish failed (%s); degrading to local delivery", e)
                await self._set_mode(False)
        await self.broadcast_local(event)

    async def rebuild_board(self) -> int:
        """[T5] Recreate the Redis live board from the database (source of truth)."""
        if self.board_loader is None:
            return 0
        board = await self.board_loader()
        pipe = self.redis.pipeline(transaction=True)
        pipe.delete(BOARD)
        if board:
            pipe.hset(BOARD, mapping={k: json.dumps(v) for k, v in board.items()})
        await pipe.execute()
        self.board_rebuilds += 1
        return len(board)

    async def read_board(self) -> dict[str, Any] | None:
        if not (self.redis_ok and self.breaker.allow()):
            return None
        try:
            raw = await self.redis.hgetall(BOARD)
            self.breaker.success()
            return {k.decode(): json.loads(v) for k, v in raw.items()}
        except Exception:
            self.breaker.failure()
            return None

    async def _set_mode(self, ok: bool) -> None:
        if ok == self.redis_ok:
            return
        self.redis_ok = ok
        self.last_change = time.time()
        log.warning("realtime mode -> %s", "push (redis)" if ok else "polling (redis down)")
        await self.broadcast_local({"type": "mode", "mode": "push" if ok else "polling",
                                    "node": self.node_id, "ts": time.time()})

    # ------------------------------------------------------------------ subscribe loop
    async def run(self) -> None:
        backoff = 0.5
        while True:
            pubsub = self.redis.pubsub()
            try:
                await pubsub.subscribe(CHANNEL)
                was_down = not self.redis_ok
                if was_down:
                    try:
                        n = await self.rebuild_board()
                        log.warning("redis (re)connected: live board rebuilt from DB (%d open items)", n)
                    except Exception as e:  # DB briefly unavailable: board fills as bids arrive
                        log.warning("live board rebuild failed: %r", e)
                self.breaker.success()
                await self._set_mode(True)
                if was_down:
                    await self.broadcast_local({"type": "resync", "node": self.node_id, "ts": time.time()})
                backoff = 0.5
                while True:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg is None:
                        await self.redis.ping()  # detect a dead Redis promptly
                        continue
                    self.received += 1
                    await self.broadcast_local(json.loads(msg["data"]))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.breaker.failure()
                await self._set_mode(False)
                log.info("redis subscribe loop error: %s; retry in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)  # [T2] exponential backoff on reconnect
            finally:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass

    def snapshot(self) -> dict[str, Any]:
        return {"mode": "push" if self.redis_ok else "polling", "ws_clients": len(self.clients),
                "published": self.published, "received": self.received,
                "breaker": self.breaker.snapshot(), "board_rebuilds": self.board_rebuilds}
