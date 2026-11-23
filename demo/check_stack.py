"""M1 stack check: every service healthy, correct replication topology, LB spreading load."""
from __future__ import annotations

from collections import Counter

import httpx

from demo.common import (APPS, LB_URL, Checks, banner, docker, info, patroni_cluster,
                         print_cluster, psql, step)


def main() -> None:
    banner("Stack check (M1)")
    c = Checks()

    step("Containers")
    out = docker("ps", "--filter", "label=com.docker.compose.project=auction",
                 "--format", "{{.Label \"com.docker.compose.service\"}}\t{{.Status}}")
    rows = dict(line.split("\t", 1) for line in out.strip().splitlines())
    for svc in ["etcd", "pg-a1", "pg-a2", "pg-a3", "pg-b1", "pg-b2", "redis", *APPS, "haproxy"]:
        st = rows.get(svc, "missing")
        c.check(st.startswith("Up") and "unhealthy" not in st and "starting" not in st, f"{svc:8} {st}")

    for shard, n in (("A", 3), ("B", 2)):
        step(f"Shard {shard} (Patroni view)")
        print_cluster(shard)
        cl = patroni_cluster(shard) or {"members": []}
        roles = Counter(m["role"] for m in cl["members"])
        c.check(roles["leader"] == 1, f"exactly one leader (got {roles['leader']})")
        c.check(roles["sync_standby"] == 1, "one synchronous standby")
        if shard == "A":
            c.check(roles["replica"] == 1, "one asynchronous replica")
        c.check(len(cl["members"]) == n, f"{n} members")
        lead = next((m["name"] for m in cl["members"] if m["role"] == "leader"), None)
        if lead:
            db = "auction" if shard == "A" else "wallet"
            rep = psql(lead, db, "SELECT string_agg(application_name||':'||sync_state, ' ' ORDER BY 1) FROM pg_stat_replication")
            info(f"pg_stat_replication on {lead}: {rep.strip()}")
            c.check(psql(lead, db, "SHOW max_prepared_transactions").strip() == "100", "max_prepared_transactions=100 (2PC enabled)")
            arch = psql(lead, db, "SELECT failed_count FROM pg_stat_archiver").strip()
            c.check(arch == "0", f"WAL archiver: failed_count={arch}")

    step("App nodes (direct)")
    for a in APPS:
        try:
            r = httpx.get(f"http://{a}:8000/readyz", timeout=2).json()
            c.check(r["ready"] and r["writes_available"], f"{a}: ready, A-leader={r['shard_a_leader']} B-leader={r['shard_b_leader']}")
        except Exception as e:
            c.check(False, f"{a}: {e}")

    step("HAProxy round-robin (30 requests)")
    dist: Counter[str] = Counter()
    with httpx.Client(timeout=2) as http:
        for _ in range(30):
            dist[http.get(f"{LB_URL}/healthz").headers.get("x-served-by", "?")] += 1
    info("  ".join(f"{k}={v}" for k, v in sorted(dist.items())))
    c.check(set(dist) == set(APPS) and max(dist.values()) - min(dist.values()) <= 1, "requests spread evenly over app1..app3")
    c.finish()


if __name__ == "__main__":
    main()
