# Distributed Live Auction Engine — Design Report

ADBMS course project. This report explains the design choices behind the six demonstrated
topics, the consistency/availability trade-offs we accepted, how we handle the blocking
problem of two-phase commit, and what happens on every failure we could inject. Every number
quoted here comes from the demo transcripts in [`docs/demo-output/`](demo-output/), produced by
`make demo-all` on a single laptop (Docker Desktop, Apple Silicon, 10 vCPU / 8 GB).

---

## 1. System overview

| Layer | Components | Role |
|---|---|---|
| Clients | browser UI, load generator, demo scripts | HTTP + WebSocket |
| Load balancer | HAProxy 2.9 | round-robin over 3 app nodes, active + passive health checks, request retry/redispatch |
| Application | `app1..app3` (FastAPI, asyncpg), stateless | REST, WebSocket hub, **2PC coordinator**, in-doubt resolver, auction-closer |
| Messaging | Redis 7 (no persistence) | pub/sub fan-out of live events, derived "live board" cache |
| Shard A | PostgreSQL 16 ×3 under Patroni (`auction`) | items, bids, users, **coordinator log** `coord.txn_log` |
| Shard B | PostgreSQL 16 ×2 under Patroni (`wallet`) | wallets, escrow holds |
| Consensus | etcd 3.5 (single node) | Patroni leader lease |
| Observability | monitor service | dashboard: primaries, lag, LB distribution, 2PC counters, money invariant, events |

A bid touches two independent database clusters: the auction row and bid record on Shard A,
and the bidder's money (an escrow hold) on Shard B. Keeping those two consistent is the core
problem the project is built around.

---

## 2. Design decisions

| # | Decision | Alternatives considered | Why |
|---|---|---|---|
| D1 | **Patroni + etcd** for failover | custom failover monitor; repmgr | Patroni gives a TTL-based leader lease in a consensus store, self-fencing (a primary that cannot renew its lease demotes itself), `pg_rewind` rejoin and managed `synchronous_standby_names`. A home-grown monitor would have to re-implement exactly the parts that cause split-brain when done wrong. |
| D2 | App discovers topology from **Patroni's REST API** (1 s poll) | HAProxy TCP frontends for the DB, libpq multi-host | Read/write splitting happens in the app (a topic requirement), so the app needs to know which node is primary, which replica is sync and how far each lags. |
| D3 | Shard A: `synchronous_mode` with **one sync standby (`remote_apply`) + one async** | all async; quorum commit | Shows both behaviours side by side. `remote_apply` makes the sync replica read-your-writes consistent and guarantees that the promoted node has every acknowledged commit. |
| D4 | **Coordinator log in Shard A** (`coord.txn_log`, sync-replicated) | local file per app node; separate coordinator DB | App nodes are stateless and can die at any time, so the log must outlive them, and any node must be able to finish any other node's transactions. Shard A is already highly available and synchronously replicated. |
| D5 | Native **`PREPARE TRANSACTION` / `COMMIT PREPARED`** | sagas with compensation; XA middleware | Real atomic commitment with the database's own durable prepared state, which survives participant crashes and failovers (prepared transactions are WAL-logged and replicated). |
| D6 | Every `txn_log` transition is a **compare-and-set** | plain updates + timeouts | The decision is a single conditional `UPDATE`. A resolver that decides ABORT and a slow coordinator that decides COMMIT can never both succeed; the loser sees "0 rows" and follows the winner. |
| D7 | **Pessimistic row locks**: item `FOR UPDATE` on A, wallets in `user_id` order on B | optimistic (`version` check + retry) | Bids on a hot item are highly contended; optimistic retries would waste prepared transactions. `items.version` is still bumped and pushed to clients so the UI can discard stale events. |
| D8 | **Idempotency-Key** (UNIQUE in `bids`), re-checked *after* taking the item lock | client-side dedupe only | Retries after timeouts, crashes or lost responses must never create a second bid or a second hold. |
| D9 | Read-your-writes via **LSN tokens** (`X-Write-LSN` → `X-Min-LSN`) | sticky sessions; "read primary for N s after a write" | Exact, and works across stateless nodes. A replica is used only if `pg_last_wal_replay_lsn() >= token`. |
| D10 | **Separate connection pools** for 2PC control traffic | single pool per member | Found by the 100-concurrent-bids test (see §6): requests blocked on a row lock filled the pool while the lock holder needed one more connection to commit. |
| D11 | Auction closer elected with a **session advisory lock** | Redis lock; etcd lease | Lives exactly as long as the holder's DB session; vanishes automatically if the node or the primary dies. `FOR UPDATE SKIP LOCKED` + status check make double-closing impossible anyway. |
| D12 | Patroni `ttl=10, loop_wait=2, retry_timeout=4` (image patches Patroni's ttl ≥ 20 floor down to 8) | default ttl 30 | Brings hard-kill failover under the 15 s target. Cost: a 10 s lease is more sensitive to host stalls, so spurious failovers are more likely on an overloaded machine. |
| D13 | PITR restores into a **sandbox instance**, then repairs rows | restore the whole cluster in place | Recovering one table's worth of data should not take the HA cluster down. This is how PITR is normally used for logical accidents. |
| D14 | Demo/test runner is a **container with the Docker CLI** | host Python | Same Python 3.12 everywhere. It joins the compose network and can `docker kill`/`start` services for chaos. |

---

## 3. Consistency vs availability (CAP) trade-offs

**Inside one shard: synchronous vs asynchronous replication.**

* The **sync standby** (`synchronous_commit = remote_apply`): a commit returns only after the
  standby has *applied* it. Gains: (a) the standby is promoted on failover and holds every
  acknowledged commit (RPO = 0), and (b) reads there are never stale. Price: every commit pays
  a network round trip plus apply time, and commits stall while that one replica can't keep
  up. `demo-replication` step 5 delays apply on the sync replica by 500 ms: one bid then takes
  **2538 ms** instead of 7 ms, because a 2PC bid makes ~5 synchronous commits on Shard A and
  each one waits. `demo-fault` phase 2 kills the sync standby: commits stall until Patroni moves
  the sync role to the other replica (≈1 s in one run, under 1 s in the final run).
* The **async replica**: commits never wait for it, but it can serve stale reads and could lose
  the tail of WAL if it were promoted. Patroni never promotes it while a sync standby exists.
  `demo-replication` steps 3–4: with the async replica lagging, 15 of 30 untokened reads
  returned stale prices; with an `X-Min-LSN` token, 0 of 30 did.
* Patroni runs in **non-strict** `synchronous_mode`. If no sync candidate is left (e.g. Shard B
  with its only replica dead), the primary keeps accepting writes without a sync standby. This
  is an explicit **availability-over-consistency** choice for the degraded state (RPO > 0 until
  a replica returns). `synchronous_mode_strict: true` would make the opposite choice and block
  writes instead.

**Across shards: 2PC is a CP protocol.** Atomic commitment cannot stay available under
coordinator failure; prepared participants must *wait* for the decision (§4). We accept that
because a bid that exists without its money hold, or a hold without its bid, is worse than a
few seconds of blocking on one item.

**During a primary failover** a shard has no writer for ~10 s (measured: 10.2 s on A, 10.8 s on
B), but **reads stay available**. Replicas whose upstream just died report the Patroni state
`in archive recovery` and keep serving, so we chose to remain readable at the cost of being
briefly non-writable.

**Redis is an AP side channel.** It holds only notifications and a derived cache. When it is
down, writes continue, clients fall back to polling, and the board is served from the DB. When
it returns empty, the board is rebuilt from the DB. No correctness depends on Redis.

---

## 4. Two-phase commit: protocol, blocking problem, mitigations

```
coordinator (any app node)                Shard A (auction)          Shard B (wallet)
INSERT txn_log(state=INIT)  ──────────────▶ (log lives on A)
BEGIN; lock item; validate; insert bid ──▶ PREPARE 'auc:<id>:A'
                                           BEGIN; lock wallets; release prev hold;
                                           check funds; new hold ─────────────▶ PREPARE 'auc:<id>:B'
CAS txn_log INIT→PREPARED
CAS txn_log →COMMITTED   ◀── commit point (durable, sync-replicated)
COMMIT PREPARED A ∥ COMMIT PREPARED B   (parallel)
txn_log.completed = true
```

A NO vote (validation failure, insufficient funds, lock timeout, crash) before the commit point
leads to CAS → ABORTED and `ROLLBACK PREPARED` on whatever was prepared. The coordinator *never*
rolls back unless the log says ABORTED: if its own COMMITTED write succeeded but the reply was
lost, rolling back would contradict a durable decision, so it leaves the parts to the resolver.

**The blocking problem.** A prepared transaction keeps its row locks until a decision arrives.
If the coordinator dies between PREPARE and COMMIT, the item row (Shard A) and the bidders'
wallets (Shard B) stay locked. Participants cannot decide alone: committing could contradict an
abort, and aborting could contradict a commit the client already saw. `demo-recovery` 1c shows
it directly: with the coordinator SIGKILLed mid-phase-2, a new bid on the same item **waited
4.8 s**.

**Mitigations implemented:**

1. **Durable, replicated, shared decision log.** Participants are never left to guess; any
   surviving node reads the decision.
2. **Resolver on every node** (`app/auction/recovery.py`), run at startup and every 3 s under an
   advisory lock. It scans `pg_prepared_xacts` on both shards and follows the log: COMMITTED →
   `COMMIT PREPARED`; ABORTED/INIT/PREPARED/missing → CAS to ABORTED, then `ROLLBACK PREPARED`
   (presumed abort).
3. **Grace period (5 s)** before touching *another* node's prepared transaction, so a live
   coordinator is not aborted needlessly. A restarted node resolves *its own* transactions
   immediately (it knows it crashed): in demo 1a/1b the window was ~0.8 s.
4. **CAS on the decision**, so the resolver and a slow coordinator can't diverge (tested in
   `tests/test_recovery.py::test_recovery_abort_beats_slow_coordinator`).
5. **`lock_timeout` (10 s)** bounds how long a waiting bid blocks; the client gets a clean
   `409 item_busy` and can retry.
6. **Short lock hold time**: both COMMIT PREPAREDs run in parallel; 2PC control statements use a
   dedicated pool so they never queue behind blocked work.
7. **Idempotency keys**: a client whose request died with the coordinator just retries; it gets
   the original result if the transaction committed (`demo-recovery` 1b → `status=duplicate`).

**Not solved (inherent to 2PC):** while the coordinator log is unreachable, in-doubt
transactions stay blocked. Shard A's own HA (sync replica + Patroni) shrinks that window to one
failover. Non-blocking alternatives would be Paxos Commit or 3PC, or giving up atomicity for
sagas with compensations.

**Invariants** (`demo/invariants.py`, after every demo): Σ balance equals the seeded money supply
($10,000); `held` equals Σ active holds per wallet; at most one active hold per item; every open
item's hold matches its highest bid; every sold item's hold is settled to the seller; every bid
and hold belongs to a COMMITTED transaction; exactly one bid at the winning price; nothing left
in doubt.

---

## 5. Failure matrix

All rows were injected by the demo scripts and measured on this setup.

| Failure (how injected) | Detection | Recovery | Committed data lost? | Measured impact |
|---|---|---|---|---|
| App node crash (`docker kill app2`) | HAProxy passive check (first L4 error) + active `/readyz` | traffic redistributed; WS clients reconnect via LB; another node takes over closing auctions | No | removed **0.7 s** after kill, **0 failed** requests (`demo-lb`) |
| App process `kill -9` mid-2PC, before decision | restart policy; resolver finds `pg_prepared_xacts` + log `PREPARED` | presumed abort → `ROLLBACK PREPARED` on A and B | No (never committed) | resolved **0.8 s** after crash |
| …after the COMMITTED decision | same | `COMMIT PREPARED` on A and B; client retry → `duplicate` | No | resolved **0.8 s** after crash |
| …externally SIGKILLed, node stays down | other nodes' resolvers (grace 5 s) | surviving node commits from the shared log | No | concurrent bid blocked **4.8 s** (2PC blocking) |
| Participant can't be reached in phase 2 | coordinator retry exhausted → `completed=false` | resolver commits the remaining part | No | B committed **6.2 s** later (grace) |
| Participant votes NO / fails at PREPARE | error in phase 1 | global abort, `ROLLBACK PREPARED` on A | No | client gets 503/409, nothing applied |
| Shard A primary machine dies (`docker kill`) | Patroni lease expiry (ttl 10 s) | sync standby promoted, apps follow in ≤1 s, old primary rejoins via `pg_rewind` | **No** (sync standby promoted) | write downtime **10.2 s**, reads **0 failed** (4199 ok), never >1 writable primary, 2110/2110 bids kept |
| Shard B primary machine dies | same | same | **No** | write downtime **10.8 s**, 1971/1971 bids kept |
| PostgreSQL processes `kill -9` (Patroni alive) | Patroni sees postmaster gone | restart in place, **WAL redo** (crash recovery) | **No** | redo in 0.03 s, 9 failed requests, 1036/1036 bids kept |
| Sync replica dies | Patroni sees it leave `pg_stat_replication` | async replica becomes sync; reads skip dead replica (circuit breaker) | No | ≤ 1 s write stall, 0 failed requests |
| Async replica dies | circuit breaker on first connection errors | read rotation shrinks; rejoins on restart | No | none visible |
| Replica lags behind | `pg_stat_replication` replay lag; LSN token check | lagging replica dropped from rotation (> 1 MB) / tokened reads fall back | No | 0 stale reads with token |
| Redis dies | publish error / subscribe loop → breaker opens | local delivery + clients poll; DB serves `/api/live`; board rebuilt on return | No (not a source of truth) | 0 failed bids during outage |
| Accidental `DELETE FROM bids` | human | PITR: base backup + WAL archive → sandbox at target time → copy rows back | No (restored) | sandbox ready in **1.3 s**, all 7594 rows back |
| etcd dies (not demoed) | Patroni can't renew the lease | `failsafe_mode`: primary keeps running while all members are reachable; no failover possible meanwhile | No | — |

---

## 6. Problems the demos surfaced (and how they were fixed)

Building each demo before calling a milestone done found real bugs:

1. **Stale snapshot in `SELECT … FOR UPDATE`.** Five concurrent bids at the same price all
   "won". Under READ COMMITTED, after waiting for the row lock PostgreSQL re-reads the *locked
   row*, but a subquery in the same statement (`EXISTS (SELECT … FROM bids)`) still used the old
   snapshot, so later bidders believed there were no bids yet. Fix: validate only columns of the
   locked row (`highest_bidder`).
2. **Connection-pool deadlock under contention.** 100 concurrent bids on one item: waiters held
   all 20 pool connections while the lock holder needed one more to write its decision, and
   nothing moved until timeouts. Fix: a dedicated control pool for 2PC statements. 100 bids
   then finished in 0.13–0.34 s.
3. **WAL archiving stuck after failover.** `test ! -f … && cp` fails forever when the new
   primary re-archives a segment the old primary already archived, which would silently break
   PITR. Fix: `archive_wal.sh` accepts a byte-identical existing file and copies atomically.
4. **Reads unavailable during failover.** Replicas whose primary just died are reported as
   `in archive recovery`, which the router didn't treat as readable. Fix: include that state.
5. **Patroni fights manual replay pauses.** `pg_wal_replay_pause()` is undone within one Patroni
   loop, and recovery parameters in `postgresql.auto.conf` are stripped. Lag is therefore
   simulated as a *delayed replica* via the member's own Patroni config.
6. **Rebuilding images restarted the databases.** Compose recreates containers whose image digest
   changed, and buildx digests are not reproducible here. `make up` no longer rebuilds, and
   `make redeploy` rebuilds only the stateless tier with `--no-deps`.

---

## 7. Limitations and future work

* etcd, Redis and HAProxy are single instances. A production setup would run 3 etcd members,
  Redis Sentinel/Cluster, and two HAProxies with keepalived. The demo shows the DB and app tiers'
  fault tolerance, which is where the ADBMS concepts live.
* The 2PC coordinator log shares Shard A's failure domain. While Shard A has no primary, new bids
  fail fast and in-doubt transactions wait (they are resolved right after the failover).
* Patroni's ttl safety floor is patched for the demo (D12).
* Auth is a trusted `X-User-Id` header, as the brief allowed.
* PITR repair is a row-level copy for a known table. A general tool would diff and merge.
