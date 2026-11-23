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

from .resilience import CircuitBreaker

log = logging.getLogger("topology")

EventCallback = Callable[[str, dict[str, Any]], Awaitable[None]]


REPLICATION_SQL = """
SELECT application_name AS name, state, sync_state,
       pg_current_wal_lsn()::text AS primary_lsn, replay_lsn::text AS replay_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)::bigint   AS sent_lag_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)::bigint  AS flush_lag_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint AS replay_lag_bytes,
       round(extract(epoch FROM write_lag)  * 1000, 2)::float8 AS write_lag_ms,
       round(extract(epoch FROM flush_lag)  * 1000, 2)::float8 AS flush_lag_ms,
       round(extract(epoch FROM replay_lag) * 1000, 2)::float8 AS replay_lag_ms
FROM pg_stat_replication
"""


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
    # Separate small pools for 2PC control traffic (txn_log CAS, COMMIT/ROLLBACK PREPARED,
    # resolver). Without them, requests blocked on a row lock can occupy every connection
    # while the lock holder needs one more connection to finish -> pool deadlock.
    ctl_pools: dict[str, asyncpg.Pool] = field(default_factory=dict)
    leader_name: str | None = None
    last_refresh_ok: float = 0.0
    last_error: str | None = None
    # [T1] Per-replica stats from the primary's pg_stat_replication (true replay lag).
    replication: dict[str, dict[str, Any]] = field(default_factory=dict)
    # [T2] One circuit breaker per member: a dead replica leaves read rotation after 2
    # failed connections instead of costing every read a connect timeout until Patroni
    # notices (its member key only expires after ttl).
    breakers: dict[str, CircuitBreaker] = field(default_factory=dict)
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
        await self._refresh_replication()

        # Drop pools for members that left the cluster.
        for name in set(self.pools) | set(self.ctl_pools):
            if name not in new:
                await self._close_pool(name)

    def breaker(self, name: str) -> CircuitBreaker:
        if name not in self.breakers:
            self.breakers[name] = CircuitBreaker(f"{self.shard}/{name}")
        return self.breakers[name]

    def report(self, name: str | None, ok: bool) -> None:
        """Callers report connection-level success/failure against a member."""
        if name is None:
            return
        b = self.breaker(name)
        b.success() if ok else b.failure()

    async def _set_leader(self, leader: str | None) -> None:
        if leader == self.leader_name:
            return
        if leader is not None:
            self.breaker(leader).success()  # fresh start for a newly promoted primary
        old, self.leader_name = self.leader_name, leader
        log.warning("shard %s leader change: %s -> %s", self.shard, old, leader)
        if old is not None and old in self.pools:
            # The old primary's connections may point at a now-dead or demoted server.
            await self._close_pool(old)
        if self.on_event:
            await self.on_event("leader_change", {"shard": self.shard, "old": old, "new": leader})

    async def _refresh_replication(self) -> None:
        """[T1] Ask the primary how far each replica has received/flushed/replayed."""
        if self.leader_name is None:
            self.replication = {}
            return
        try:
            _, pool = await self.primary()
            rows = await pool.fetch(REPLICATION_SQL, timeout=1.0)
            self.replication = {r["name"]: dict(r) for r in rows}
        except Exception as e:
            self.last_error = f"pg_stat_replication: {e!r}"

    async def run(self, interval: float) -> None:
        while True:
            try:
                await self.refresh()
            except Exception as e:  # never let the discovery loop die
                self.last_error = repr(e)
                log.exception("topology refresh failed")
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------ pools
    async def _pool(self, name: str, control: bool = False) -> asyncpg.Pool:
        pools = self.ctl_pools if control else self.pools
        pool = pools.get(name)
        if pool is not None:
            return pool
        lock = self._pool_locks.setdefault(f"{name}:{control}", asyncio.Lock())
        async with lock:
            if name in pools:
                return pools[name]
            m = self.members[name]
            try:
                pool = await asyncpg.create_pool(
                    host=m.host, port=m.port, user=self.user, password=self.password,
                    database=self.database, min_size=1, max_size=6 if control else 20,
                    timeout=2.0, command_timeout=15.0,
                    server_settings={"application_name": f"auction-{self.shard}{'-ctl' if control else ''}"},
                )
            except (OSError, asyncio.TimeoutError, asyncpg.PostgresError):
                self.report(name, False)
                raise
            pools[name] = pool
            return pool

    async def _close_pool(self, name: str) -> None:
        for pools in (self.pools, self.ctl_pools):
            pool = pools.pop(name, None)
            if pool is not None:
                pool.terminate()

    async def primary(self) -> tuple[str, asyncpg.Pool]:
        """[T6] Write path: always the current leader."""
        if self.leader_name is None or self.leader_name not in self.members:
            raise NoPrimaryError(f"shard {self.shard} has no primary right now")
        if not self.breaker(self.leader_name).allow():
            raise NoPrimaryError(f"shard {self.shard} primary {self.leader_name}: circuit open")
        return self.leader_name, await self._pool(self.leader_name)

    async def primary_control(self) -> tuple[str, asyncpg.Pool]:
        """[T4] Pool reserved for 2PC control statements on the current primary."""
        if self.leader_name is None or self.leader_name not in self.members:
            raise NoPrimaryError(f"shard {self.shard} has no primary right now")
        return self.leader_name, await self._pool(self.leader_name, control=True)

    def replay_lag(self, name: str) -> int | None:
        r = self.replication.get(name)
        return None if r is None or r["replay_lag_bytes"] is None else int(r["replay_lag_bytes"])

    def read_candidates(self) -> list[Member]:
        """[T1][T6] Streaming replicas whose replay lag is under the staleness bound."""
        out = []
        for m in self.members.values():
            if not m.is_usable_replica:
                continue
            lag = self.replay_lag(m.name)
            if lag is None:
                lag = m.lag
            if lag is None or lag <= self.max_lag_bytes:
                out.append(m)
        return out

    async def replica(self) -> tuple[str, asyncpg.Pool]:
        """[T6] Read path: round-robin over healthy replicas, primary as last resort."""
        cands = [m for m in self.read_candidates() if self.breaker(m.name).allow()]
        for _ in range(len(cands)):
            m = cands[next(self._rr) % len(cands)]
            try:
                return m.name, await self._pool(m.name)
            except (OSError, asyncpg.PostgresError, asyncio.TimeoutError):
                continue
        return await self.primary()

    async def close(self) -> None:
        for name in set(self.pools) | set(self.ctl_pools):
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
            "replication": [
                {k: (str(v) if k.endswith("_lsn") else v) for k, v in r.items()}
                for r in sorted(self.replication.values(), key=lambda x: x["name"])
            ],
            "read_candidates": [m.name for m in self.read_candidates()],
            "breakers": {n: b.snapshot() for n, b in sorted(self.breakers.items())},
            "view_age_s": round(time.time() - self.last_refresh_ok, 1) if self.last_refresh_ok else None,
            "error": self.last_error,
        }

    @property
    def readable(self) -> bool:
        return self.leader_name is not None or bool(self.read_candidates())
