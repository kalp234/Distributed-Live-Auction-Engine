"""Shared helpers for demo / chaos scripts (run inside the `tools` container)."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import Any

import httpx

LB_URL = os.environ.get("LB_URL", "http://haproxy:80")
PROJECT = "auction"
PATRONI = {
    "A": ["http://pg-a1:8008", "http://pg-a2:8008", "http://pg-a3:8008"],
    "B": ["http://pg-b1:8008", "http://pg-b2:8008"],
}
APPS = ["app1", "app2", "app3"]

# ----------------------------------------------------------------------------- output
_TTY = sys.stdout.isatty() or os.environ.get("FORCE_COLOR")
def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _TTY else s

def green(s: str) -> str: return _c("32", s)
def red(s: str) -> str: return _c("31", s)
def yellow(s: str) -> str: return _c("33", s)
def cyan(s: str) -> str: return _c("36", s)
def bold(s: str) -> str: return _c("1", s)
def dim(s: str) -> str: return _c("2", s)

def banner(title: str) -> None:
    line = "═" * (len(title) + 4)
    print(f"\n{bold(cyan(line))}\n{bold(cyan('  ' + title))}\n{bold(cyan(line))}")

def step(msg: str) -> None:
    print(f"\n{bold(yellow('▶'))} {bold(msg)}")

def info(msg: str) -> None:
    print(f"  {msg}")

def ok(msg: str) -> None:
    print(f"  {green('✔')} {msg}")

def bad(msg: str) -> None:
    print(f"  {red('✘')} {msg}")

class Checks:
    """Collects pass/fail assertions and exits non-zero if any failed."""
    def __init__(self) -> None:
        self.failed: list[str] = []

    def check(self, cond: bool, msg: str) -> bool:
        (ok if cond else bad)(msg)
        if not cond:
            self.failed.append(msg)
        return cond

    def finish(self) -> None:
        if self.failed:
            print(f"\n{red(bold(f'FAILED: {len(self.failed)} check(s)'))}")
            for f in self.failed:
                print(f"  - {f}")
            sys.exit(1)
        print(f"\n{green(bold('ALL CHECKS PASSED'))}")

# ----------------------------------------------------------------------------- docker
def container(service: str) -> str:
    return f"{PROJECT}-{service}-1"

def docker(*args: str, check: bool = True) -> str:
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout

def kill(service: str, signal: str = "KILL") -> None:
    """Hard-kill a whole container (simulates a crashed machine)."""
    docker("kill", "-s", signal, container(service))

def start(service: str) -> None:
    docker("start", container(service))

def dexec(service: str, *cmd: str, check: bool = True) -> str:
    return docker("exec", container(service), *cmd, check=check)

def is_running(service: str) -> bool:
    out = docker("inspect", "-f", "{{.State.Running}}", container(service), check=False)
    return out.strip() == "true"

def psql(service: str, db: str, sql: str) -> str:
    return dexec(service, "psql", "-U", "postgres", "-d", db, "-XAtc", sql)

# ----------------------------------------------------------------------------- cluster
def patroni_cluster(shard: str) -> dict[str, Any] | None:
    for url in PATRONI[shard]:
        try:
            r = httpx.get(f"{url}/cluster", timeout=1.0)
            if r.status_code == 200:
                return r.json()
        except httpx.HTTPError:
            continue
    return None

def leader(shard: str) -> str | None:
    c = patroni_cluster(shard)
    if not c:
        return None
    return next((m["name"] for m in c["members"] if m["role"] == "leader" and m["state"] == "running"), None)

def wait_for(pred: Any, timeout: float, interval: float = 0.5, what: str = "condition") -> float:
    """Poll `pred()` until truthy; returns elapsed seconds or raises TimeoutError."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if pred():
                return time.time() - t0
        except Exception:
            pass
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what}")

def print_cluster(shard: str) -> None:
    c = patroni_cluster(shard)
    if not c:
        bad(f"shard {shard}: no Patroni member reachable")
        return
    for m in sorted(c["members"], key=lambda x: x["name"]):
        role = m["role"]
        col = green if role == "leader" else (cyan if role == "sync_standby" else dim)
        lag = m.get("lag", "")
        info(f"{m['name']:6} {col(f'{role:13}')} {m['state']:10} TL={m.get('timeline')}  lag={lag}")


def set_apply_delay(member: str, delay: str | None, db: str = "auction") -> None:
    """Turn a replica into a *delayed* replica (recovery_min_apply_delay) or back.

    Patroni owns recovery parameters (it strips them from postgresql.auto.conf), so this is
    done the Patroni way: edit that member's local patroni.yml and SIGHUP Patroni (PID 1).
    The setting is not persistent: the entrypoint re-renders patroni.yml on restart."""
    cmd = "sed -i '/recovery_min_apply_delay/d' /etc/patroni/patroni.yml"
    if delay:
        cmd += f" && sed -i 's|^  recovery_conf:|  recovery_conf:\\n    recovery_min_apply_delay: {delay}|' /etc/patroni/patroni.yml"
    dexec(member, "sh", "-c", cmd + " && kill -HUP 1")
    want = delay or "0"
    wait_for(lambda: psql(member, db, "SHOW recovery_min_apply_delay").strip() == want, 15, 0.3,
             f"{member} recovery_min_apply_delay={want}")


def cluster_ready(shard: str) -> bool:
    """Leader running, a sync standby assigned, and every replica streaming."""
    c = patroni_cluster(shard)
    if not c:
        return False
    n = {"A": 3, "B": 2}[shard]
    ms = c["members"]
    return (len(ms) == n and any(m["role"] == "leader" and m["state"] == "running" for m in ms)
            and any(m["role"] == "sync_standby" for m in ms)
            and all(m["state"] == "streaming" for m in ms if m["role"] != "leader"))


def wait_cluster_ready(timeout: float = 120) -> float:
    """After a fresh bootstrap replicas first catch up from the archive before they stream
    and Patroni assigns the sync standby; demos need that steady state."""
    return wait_for(lambda: cluster_ready("A") and cluster_ready("B"), timeout, 0.5, "both clusters streaming with a sync standby")


if __name__ == "__main__":
    t = wait_cluster_ready()
    ok(f"both DB clusters ready (leader + sync standby + streaming replicas) after {t:.1f}s")
