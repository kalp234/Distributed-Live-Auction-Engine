"""Runtime configuration (all from environment variables set in docker-compose.yml)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _list(name: str, default: str = "") -> list[str]:
    return [x.strip() for x in os.environ.get(name, default).split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
    node_id: str = os.environ.get("NODE_ID", "app?")
    db_user: str = os.environ.get("DB_USER", "auction_app")
    db_password: str = os.environ.get("DB_PASSWORD", "auction_pw")
    # Patroni REST endpoints per shard; used for primary/replica discovery [T3][T6].
    patroni_a: list[str] = field(default_factory=lambda: _list("PATRONI_A"))
    patroni_b: list[str] = field(default_factory=lambda: _list("PATRONI_B"))
    db_a_name: str = os.environ.get("DB_A_NAME", "auction")
    db_b_name: str = os.environ.get("DB_B_NAME", "wallet")
    redis_url: str = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    topology_interval: float = float(os.environ.get("TOPOLOGY_INTERVAL", "1.0"))
    # Replicas lagging more than this many bytes are taken out of read rotation [T1].
    max_replica_lag_bytes: int = int(os.environ.get("MAX_REPLICA_LAG_BYTES", str(1024 * 1024)))


settings = Settings()
