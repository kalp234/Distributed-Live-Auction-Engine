"""Fault injection for demos/tests. Faults are armed per node via POST /admin/faults and are
consumed when hit, so "arm once, send one bid to that node" injects exactly one failure.

Injection points in the 2PC coordinator [T4][T5]:
  vote_no_b             Shard B votes NO at PREPARE                       -> full abort
  crash_before_decision both shards PREPARED, process dies (kill -9)      -> recovery ABORTS
  crash_after_decision  COMMITTED logged, process dies before phase 2      -> recovery COMMITS
  lose_b_in_phase2      phase 2 cannot reach Shard B (it stays prepared)   -> resolver COMMITS it
  delay_before_phase2   sleep `arg` seconds between decision and phase 2   (observe in-doubt state)
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

log = logging.getLogger("faults")
KNOWN = {"vote_no_b", "crash_before_decision", "crash_after_decision", "lose_b_in_phase2", "delay_before_phase2"}
_armed: dict[str, dict[str, Any]] = {}


def arm(name: str, count: int = 1, arg: float | None = None) -> None:
    if name not in KNOWN:
        raise ValueError(f"unknown fault {name!r}; known: {sorted(KNOWN)}")
    _armed[name] = {"count": count, "arg": arg}
    log.warning("fault armed: %s x%d arg=%s", name, count, arg)


def clear() -> None:
    _armed.clear()


def armed() -> dict[str, dict[str, Any]]:
    return dict(_armed)


def take(name: str) -> dict[str, Any] | None:
    f = _armed.get(name)
    if not f:
        return None
    f["count"] -= 1
    if f["count"] <= 0:
        _armed.pop(name, None)
    log.warning("FAULT INJECTED: %s", name)
    return f


async def maybe_crash(name: str) -> None:
    """Simulate `kill -9` of this app process at an exact point in the protocol."""
    if take(name):
        logging.getLogger("faults").critical("simulated crash (%s): os._exit(137)", name)
        os._exit(137)


async def maybe_delay(name: str) -> None:
    f = take(name)
    if f:
        await asyncio.sleep(float(f.get("arg") or 5))
