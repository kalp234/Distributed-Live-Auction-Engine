# Distributed Live Auction Engine

ADBMS course project: a real-time auction system built to demonstrate **replication, fault
tolerance, availability, distributed transactions (2PC), recovery and load balancing** on a
local Docker Compose stack. See [PLAN.md](PLAN.md) for the design and milestone plan.

## Quick start

```bash
make up       # build + start everything, wait until healthy (~30 s first time after build)
make check    # verify health, replication topology and load-balancer spread
make status   # Patroni view of both database clusters
make down     # stop (keeps data)   |   make clean  # stop and delete all data
```

| URL | What |
|-----|------|
| http://localhost:18080 | App (via HAProxy) |
| http://localhost:18404/stats | HAProxy stats |

> Full documentation (architecture diagram, per-topic walkthrough, demo outputs) lands in milestone M9.
