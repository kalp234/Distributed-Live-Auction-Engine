"""Run every demo in sequence from fresh seed data and save each output to
docs/demo-output/<demo>.txt, then print a summary table."""
from __future__ import annotations

import asyncio
import pathlib
import re
import subprocess
import sys
import time

from demo import seed
from demo.common import banner, bold, green, red, wait_cluster_ready

DEMOS = [
    ("check", "demo.check_stack", "M1 stack check"),
    ("demo-replication", "demo.demo_replication", "[T1] Replication"),
    ("demo-lb", "demo.demo_lb", "[T6] Load balancing"),
    ("demo-dtx", "demo.demo_dtx", "[T4] Distributed transactions"),
    ("demo-fault", "demo.demo_fault", "[T2] Fault tolerance"),
    ("demo-failover", "demo.demo_failover", "[T3] Availability"),
    ("demo-recovery", "demo.demo_recovery", "[T5] Recovery"),
    ("check-invariants", "demo.invariants", "[T4] Invariants"),
]
OUT = pathlib.Path("docs/demo-output")
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    banner("demo-all: every ADBMS topic, end to end")
    wait_cluster_ready()
    asyncio.run(seed.main(reset=True))
    results = []
    for target, module, title in DEMOS:
        print(f"\n{bold('━' * 78)}\n{bold(f'make {target}')}  —  {title}\n{bold('━' * 78)}", flush=True)
        t0 = time.time()
        p = subprocess.Popen([sys.executable, "-m", module], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        lines = []
        assert p.stdout
        for line in p.stdout:
            print(line, end="", flush=True)
            lines.append(ANSI.sub("", line))
        rc = p.wait()
        dt = time.time() - t0
        (OUT / f"{target}.txt").write_text(f"$ make {target}\n" + "".join(lines))
        checks = sum(1 for l in lines if l.lstrip().startswith(("✔", "✘")))
        failed = sum(1 for l in lines if l.lstrip().startswith("✘"))
        results.append((target, title, rc, checks, failed, dt))
    banner("demo-all summary")
    for target, title, rc, checks, failed, dt in results:
        status = green("PASS") if rc == 0 else red("FAIL")
        print(f"  {status}  {target:18} {title:32} {checks - failed:>3}/{checks:<3} checks  {dt:6.1f}s")
    total = sum(r[3] for r in results)
    bad = sum(r[4] for r in results)
    print(f"\n  {bold(f'{total - bad}/{total} checks passed')}; outputs in {OUT}/")
    sys.exit(0 if all(r[2] == 0 for r in results) else 1)


if __name__ == "__main__":
    main()
