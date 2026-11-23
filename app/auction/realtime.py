"""Live event fan-out.

[T6] Cross-node fan-out: a bid accepted on app1 is published to a Redis channel; every app
     node is subscribed and pushes it to *its own* WebSocket clients, so users connected to
     app2/app3 (via HAProxy) see it instantly.
[T2] Graceful degradation: if Redis is down, publishing falls back to local delivery and
     every client is told to switch to HTTP polling; when Redis returns, clients are told to
     go back to push mode. The database stays the source of truth, so nothing is lost —
     Redis only carries notifications.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import WebSocket

log = logging.getLogger("realtime")
CHANNEL = "auction:events"


class Hub:
    def __init__(self, node_id: str, redis_url: str) -> None:
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
        if self.redis_ok:
            try:
                await self.redis.publish(CHANNEL, json.dumps(event, default=str))
                self.published += 1
                return  # our own subscription will deliver it to local clients
            except Exception as e:
                log.warning("redis publish failed (%s); degrading to local delivery", e)
                await self._set_mode(False)
        await self.broadcast_local(event)

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
                await self._set_mode(True)
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
                "published": self.published, "received": self.received}
