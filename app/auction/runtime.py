"""Process-wide singletons shared by routes and background jobs."""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .realtime import Hub
    from .topology import ShardTopology


class Runtime:
    shard_a: "ShardTopology"
    shard_b: "ShardTopology"
    hub: "Hub"
    tasks: list[asyncio.Task[Any]] = []


rt = Runtime()
