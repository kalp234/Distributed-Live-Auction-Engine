# PLAN — Distributed Live Auction Engine

Course project for Advanced DBMS. The system exists to **demonstrate six topics live**:
(1) Replication, (2) Fault tolerance, (3) Availability, (4) Distributed transactions,
(5) Recovery, (6) Load balancing. Code comments tag each implementation site with `[T1]`…`[T6]`.

## 1. Decisions (defaults chosen; change here if needed)

| # | Decision | Choice | Why |
|---|----------|--------|-----|
| D1 | Failover manager | **Patroni 4 + single-node etcd** (both shards) | Battle-tested leader election with TTL lease, fencing (old primary demotes itself), `pg_rewind` rejoin, and managed `synchronous_standby_names`. A custom monitor would need all of that re-implemented and would be the most likely source of split-brain bugs. Single etcd is a SPOF for *failover decisions only* (running primaries keep serving while etcd is down—Patroni demotes only when it can't renew the lease, see D9). Acceptable for a laptop demo; documented in REPORT. |
| D2 | Primary discovery for apps | App polls Patroni REST `GET /cluster` (any reachable member) every 1 s | Lets the app do its own read/write splitting [T6], pick sync vs async replicas, and report which DB node served a read. No extra hop through HAProxy for SQL. |
| D3 | Sync vs async (Shard A) | `synchronous_mode: true`, `synchronous_node_count: 1`, `synchronous_commit: remote_apply` | One replica is synchronous (commit waits until it has *applied* the WAL, so reads there are never stale); the other is async (may lag). Patroni only promotes the sync standby → zero committed-bid loss on failover. Shard B uses `synchronous_commit: on` (flush). |
| D4 | Where the 2PC coordinator log lives | Table `coord.txn_log` in the **Shard A cluster** (own schema, autocommit writes, sync-replicated) | App nodes are stateless, so the log must be shared and survive node loss. Any node can recover any other node's in-doubt transactions. |
| D5 | 2PC decision safety | Decision is a **compare-and-set** on `txn_log.state` (`INIT/PREPARED → COMMITTED` vs `→ ABORTED`) | A recovering node and a slow live coordinator can never both decide: whichever CAS wins is the decision; the loser follows it. Presumed-abort for unknown GIDs. |
| D6 | Concurrency control | `SELECT … FOR UPDATE` on the item row (Shard A), wallets locked in ascending `user_id` order (Shard B), `lock_timeout` safety net | The prepared txn keeps the item lock until COMMIT PREPARED, so bids on one item serialize; lock ordering prevents deadlocks; there is no cross-shard wait cycle because a txn holding Shard B locks is already prepared on A. `items.version` is bumped and returned for clients/optimistic checks. |
| D7 | Read-your-writes | Writes return `X-Write-LSN`; reads accept `X-Min-LSN`; a replica is used only if `pg_last_wal_replay_lsn() >= min_lsn`, otherwise fall back to primary | Real LSN-based session consistency rather than a timer. |
| D8 | Demo/test runner | A `tools` container (Python 3.12 + Docker CLI, docker socket mounted) | Host has Python 3.9; the container gives everyone the same env and can `docker kill`/`docker start` services for chaos. |
| D9 | Failover timing | Patroni `ttl=10, loop_wait=2, retry_timeout=4` (Patroni 4 clamps ttl to ≥20 s; the image patches that floor to 8 s — faster failover at the cost of more sensitivity to stalls); HAProxy checks `inter 1s fall 2 rise 2`; app topology poll 1 s | Worst-case primary failover ≈ ttl + loop_wait + poll ≈ 13 s (< 15 s target). |
| D10 | PITR target | Restore base backup + archived WAL into a **separate sandbox container** to a timestamp before the bad `DELETE`, then copy the lost rows back | Recovering without tearing down the live HA cluster; mirrors how PITR is used in practice. |
| D11 | Auth | `X-User-Id` header (UI has a user picker) | Spec says no real auth needed. |
| D12 | Seller credit | `items.seller_id`; on close the winner's escrow moves to the seller's wallet balance via 2PC | Keeps total money constant → simple invariant. |

## 2. Services & port map

| Service | Image | Role | Host port(s) |
|---------|-------|------|--------------|
| `etcd` | quay.io/coreos/etcd 3.5 | DCS for Patroni leader lease | – |
| `pg-a1`,`pg-a2`,`pg-a3` | `infra/patroni` (PG16 + Patroni) | **Shard A – Auction DB** cluster (scope `auction`): 1 primary, 1 sync + 1 async replica | 15441-15443 (SQL), 18011-18013 (Patroni REST) |
| `pg-b1`,`pg-b2` | `infra/patroni` | **Shard B – Wallet DB** cluster (scope `wallet`): primary + sync replica | 15451-15452, 18021-18022 |
| `redis` | redis 7.4 | Pub/sub fan-out of bid events across app nodes | 16380 |
| `app1`,`app2`,`app3` | `app/` (FastAPI) | Stateless API + WebSocket + 2PC coordinator + closer job | 18001-18003 |
| `haproxy` | haproxy 2.9 | HTTP + WebSocket LB, health checks | **18080** (UI/API), 18404 (stats) |
| `monitor` | `monitor/` (FastAPI) | Dashboard: health, primary, lag, LB distribution, txn_log, events | **19000** |
| `seed` | `tools/` | One-shot: seed users/wallets/items if empty | – |
| `tools` | `tools/` | Demo + chaos + tests runner (profile `tools`) | – |

Host ports are in the 1xxxx range to avoid clashing with common local services (8080, 5432, 6379…).

Volumes: `pg-a{1,2,3}-data`, `pg-b{1,2}-data`, `wal-archive-a`, `wal-archive-b` (shared by members of a shard so archiving continues after failover), `basebackups`.

## 3. Folder structure

```
.
├── docker-compose.yml        # whole stack
├── Makefile                  # up/down/seed/demo-*/load/check-invariants/test
├── PLAN.md  README.md  docs/REPORT.md
├── infra/
│   ├── patroni/              # Dockerfile, entrypoint.sh, patroni.yml, post_init.sh
│   ├── sql/                  # auction.sql (Shard A), wallet.sql (Shard B)
│   └── haproxy/haproxy.cfg
├── app/
│   ├── Dockerfile  requirements.txt
│   └── auction/
│       ├── main.py           # FastAPI app, lifespan, middleware (X-Served-By)
│       ├── config.py
│       ├── topology.py       # [T1][T3][T6] Patroni discovery, pools, R/W split
│       ├── resilience.py     # [T2] retry/backoff, circuit breaker
│       ├── coordinator.py    # [T4] 2PC coordinator + txn_log
│       ├── recovery.py       # [T5] in-doubt txn resolver
│       ├── realtime.py       # [T2][T6] Redis pub/sub + WebSocket hub, polling fallback
│       ├── closer.py         # auction auto-close (advisory-lock leader)
│       ├── faults.py         # fault injection hooks for demos
│       ├── api.py            # REST routes
│       └── static/index.html # minimal live UI
├── monitor/                  # dashboard service
├── tools/                    # Dockerfile for demo runner
├── demo/                     # seed, loadgen, chaos + demo_* scripts, check_invariants
└── tests/                    # pytest (unit + integration against running stack)
```

## 4. Milestones

| M | Scope | Done when |
|---|-------|-----------|
| M1 | Compose skeleton: Patroni clusters A (3) + B (2), etcd, Redis, HAProxy, 3 app nodes with `/healthz` `/readyz`, healthchecks, schemas | `make up` → all healthy; Patroni shows 1 leader + sync + async; app reports topology; HAProxy round-robins |
| M2 | Core auction API, WebSocket live bids, UI (single shard first), seed | Bid via UI appears live |
| M3 | LB + read/write split + `X-Served-By` + `demo-lb` | Distribution printed per node, redistribution after kill |
| M4 | Replication demos + lag endpoint (`pg_stat_replication`) + RYW fallback | `demo-replication` output |
| M5 | 2PC across shards, idempotency, concurrency, invariant checker, `demo-dtx` | 4 scenarios pass; invariants hold after 100 concurrent bids |
| M6 | Retries/backoff, circuit breaker, Redis-down polling fallback, `demo-fault` | Bids continue; no committed bid lost |
| M7 | Auto failover measurement + rejoin, `demo-failover` | Downtime printed (< 15 s), old primary rejoins as replica |
| M8 | Coordinator recovery, WAL crash recovery, PITR, Redis rebuild, `demo-recovery` | before/after state printed |
| M9 | Dashboard polish, README, REPORT, `demo-all` | full run captured |

Each milestone: run its demo/tests, paste real output into the commit/notes, then commit.

## 5. Status and changes made during implementation

All milestones M1–M9 are done; each was committed after its demo/tests passed.

Decisions added or changed while building (details in [docs/REPORT.md §6](docs/REPORT.md#6-problems-the-demos-surfaced-and-how-they-were-fixed)):

| # | Change | Reason |
|---|--------|--------|
| D15 | Host ports moved to the 1xxxx range; no fixed `container_name`s | clashed with other local projects (8080, a `haproxy` container) |
| D16 | Replica lag is simulated with a **delayed replica** (`recovery_min_apply_delay` via the member's Patroni config), not `pg_wal_replay_pause()` | Patroni resumes paused replay within one loop and strips recovery params from `postgresql.auto.conf` |
| D17 | `archive_wal.sh` instead of `test ! -f && cp` | the new primary re-archives already-archived segments after a failover; the naive command blocked archiving forever |
| D18 | Dedicated 2PC **control pools** (txn_log, COMMIT/ROLLBACK PREPARED, resolver) | pool deadlock under 100 concurrent bids on one item |
| D19 | Bid validation reads only the **locked row** | READ COMMITTED subquery snapshot let several bids win the same price slot |
| D20 | Replicas in Patroni state `in archive recovery` stay read candidates | otherwise reads failed during a primary failover |
| D21 | Circuit breakers per DB member + Redis; server-side bid retries (idempotent) | fail fast instead of paying a timeout per request |
| D22 | `make up` does not rebuild images; `make redeploy` rebuilds only apps/monitor with `--no-deps` | non-reproducible image digests made Compose recreate the DB containers |
