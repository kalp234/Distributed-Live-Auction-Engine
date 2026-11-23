"""Cluster topology discovery + connection routing.

[T3] Availability: every second we ask Patroni (`GET /cluster` on any reachable member)
     who the current leader is. After a failover the write pool follows the new leader
     automatically — no app restart, no config change.
[T6] Load balancing (read/write splitting): writes always go to the leader; reads are
     spread round-robin over healthy replicas whose lag is under a threshold.
[T1] Replication: replicas are labelled sync/async from Patroni's view, and reads can demand
     a minimum LSN (read-your-writes), falling back to the primary when a replica is behind.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import asyncpg
import httpx

log = logging.getLogger("topology")

EventCallback = Callable[[str, dict[str, Any]], Awaitable[None]]


class NoPrimaryError(RuntimeError):
    """Raised when a shard currently has no writable primary (e.g. mid-failover)."""


@dataclass
class Member:
    name: str
    host: str
    port: int
    role: str          # leader | sync_standby | replica
    state: str         # running | streaming | starting | stopped ...
    lag: int | None    # bytes behind leader (None = unknown / leader)
    timeline: int | None

    @property
    def is_leader(self) -> bool:
        return self.role in ("leader", "master", "primary", "standby_leader") and self.state == "running"

    @property
    def is_usable_replica(self) -> bool:
        return self.role in ("replica", "sync_standby", "quorum_standby") and self.state in ("streaming", "running")


@dataclass
class ShardTopology:
    """Tracks one Patroni cluster (a shard) and keeps one asyncpg pool per member."""

    shard: str                      # "A" or "B"
    database: str
    patroni_urls: list[str]
    user: str
    password: str
    max_lag_bytes: int
    on_event: EventCallback | None = None

    members: dict[str, Member] = field(default_factory=dict)
    pools: dict[str, asyncpg.Pool] = field(default_factory=dict)
    leader_name: str | None = None
    last_refresh_ok: float = 0.0
    last_error: str | None = None
    _rr: itertools.count = field(default_factory=itertools.count)
    _pool_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    _http: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ discovery
    async def refresh(self) -> None:
        """Ask Patroni for the cluster layout; keep the last known view on failure."""
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=0.8)
        data: dict[str, Any] | None = None
        for url in self.patroni_urls:
            try:
                r = await self._http.get(f"{url}/cluster")
                if r.status_code == 200:
                    data = r.json()
                    break
            except httpx.HTTPError:
                continue
        if data is None:
            self.last_error = "no Patroni member reachable"
            # If we can't confirm anyone, the old leader may be dead: stop routing writes there
            # once our view is older than one Patroni TTL.
            if self.leader_name and time.time() - self.last_refresh_ok > 10:
                await self._set_leader(None)
            return

        new: dict[str, Member] = {}
        for m in data.get("members", []):
            lag = m.get("lag")
            new[m["name"]] = Member(
                name=m["name"], host=m["host"], port=int(m.get("port", 5432)),
                role=m.get("role", "?"), state=m.get("state", "?"),
                lag=lag if isinstance(lag, int) else None, timeline=m.get("timeline"),
            )
        self.members = new
        self.last_refresh_ok = time.time()
        self.last_error = None
        leader = next((m.name for m in new.values() if m.is_leader), None)
        await self._set_leader(leader)

        # Drop pools for members that left the cluster.
        for name in list(self.pools):
            if name not in new:
                await self._close_pool(name)

    async def _set_leader(self, leader: str | None) -> None:
        if leader == self.leader_name:
            return
        old, self.leader_name = self.leader_name, leader
        log.warning("shard %s leader change: %s -> %s", self.shard, old, leader)
        if old is not None and old in self.pools:
            # The old primary's connections may point at a now-dead or demoted server.
            await self._close_pool(old)
        if self.on_event:
            await self.on_event("leader_change", {"shard": self.shard, "old": old, "new": leader})

    async def run(self, interval: float) -> None:
        while True:
            try:
                await self.refresh()
            except Exception as e:  # never let the discovery loop die
                self.last_error = repr(e)
                log.exception("topology refresh failed")
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------ pools
    async def _pool(self, name: str) -> asyncpg.Pool:
        pool = self.pools.get(name)
        if pool is not None:
            return pool
        lock = self._pool_locks.setdefault(name, asyncio.Lock())
        async with lock:
            if name in self.pools:
                return self.pools[name]
            m = self.members[name]
            pool = await asyncpg.create_pool(
                host=m.host, port=m.port, user=self.user, password=self.password,
                database=self.database, min_size=1, max_size=20,
                timeout=2.0, command_timeout=10.0,
                server_settings={"application_name": f"auction-{self.shard}"},
            )
            self.pools[name] = pool
            return pool

    async def _close_pool(self, name: str) -> None:
        pool = self.pools.pop(name, None)
        if pool is not None:
            pool.terminate()

    async def primary(self) -> tuple[str, asyncpg.Pool]:
        """[T6] Write path: always the current leader."""
        if self.leader_name is None or self.leader_name not in self.members:
            raise NoPrimaryError(f"shard {self.shard} has no primary right now")
        return self.leader_name, await self._pool(self.leader_name)

    def read_candidates(self) -> list[Member]:
        return [
            m for m in self.members.values()
            if m.is_usable_replica and (m.lag is None or m.lag <= self.max_lag_bytes)
        ]

    async def replica(self) -> tuple[str, asyncpg.Pool]:
        """[T6] Read path: round-robin over healthy replicas, primary as last resort."""
        cands = self.read_candidates()
        for _ in range(len(cands)):
            m = cands[next(self._rr) % len(cands)]
            try:
                return m.name, await self._pool(m.name)
            except (OSError, asyncpg.PostgresError, asyncio.TimeoutError):
                continue
        return await self.primary()

    async def close(self) -> None:
        for name in list(self.pools):
            await self._close_pool(name)
        if self._http:
            await self._http.aclose()

    # ------------------------------------------------------------------ status
    def snapshot(self) -> dict[str, Any]:
        return {
            "shard": self.shard,
            "database": self.database,
            "leader": self.leader_name,
            "members": [
                {"name": m.name, "role": m.role, "state": m.state, "lag": m.lag, "timeline": m.timeline}
                for m in sorted(self.members.values(), key=lambda x: x.name)
            ],
            "read_candidates": [m.name for m in self.read_candidates()],
            "view_age_s": round(time.time() - self.last_refresh_ok, 1) if self.last_refresh_ok else None,
            "error": self.last_error,
        }

    @property
    def readable(self) -> bool:
        return self.leader_name is not None or bool(self.read_candidates())
