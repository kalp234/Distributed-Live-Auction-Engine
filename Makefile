# Distributed Live Auction Engine — every command you need.
#   make help        list targets
COMPOSE  := docker compose
TOOLS    := $(COMPOSE) run --rm tools
SERVICES := etcd pg-a1 pg-a2 pg-a3 pg-b1 pg-b2 redis app1 app2 app3 haproxy monitor

.PHONY: help up down clean build redeploy ps logs status seed check check-live lint test load \
        check-invariants demo-replication demo-fault demo-failover demo-dtx demo-recovery demo-lb demo-all

help:             ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- lifecycle
# `up` builds images only if missing. Builds are not reproducible here, so rebuilding on every
# `up` would make Compose recreate (restart) the database containers.
up:               ## start the whole stack, wait until healthy, seed if empty
	$(COMPOSE) build seed
	$(COMPOSE) up -d --wait $(SERVICES)
	$(TOOLS) python -m demo.common
	$(COMPOSE) run --rm --no-deps seed

down:             ## stop the stack (keeps data volumes)
	$(COMPOSE) --profile tools down

clean:            ## stop the stack and DELETE all data volumes
	$(COMPOSE) --profile tools down -v --remove-orphans

build:            ## rebuild all images (the next `make up` then recreates the DB containers too)
	$(COMPOSE) --profile tools build

redeploy: lint    ## rebuild + restart only the stateless tier (apps, monitor) — DBs untouched
	$(COMPOSE) up -d --build --no-deps --wait app1 app2 app3 monitor

ps:               ## container status
	$(COMPOSE) ps

logs:             ## follow logs of all services
	$(COMPOSE) logs -f --tail=50

status:           ## Patroni view of both DB clusters
	@for n in pg-a1 pg-a2 pg-a3; do $(COMPOSE) exec -T $$n patronictl -c /etc/patroni/patroni.yml list 2>/dev/null && break; done
	@for n in pg-b1 pg-b2; do $(COMPOSE) exec -T $$n patronictl -c /etc/patroni/patroni.yml list 2>/dev/null && break; done

seed:             ## seed data if empty (RESET=1 to wipe and reseed)
	$(TOOLS) python -m demo.seed $(if $(RESET),--reset,)

# ---------------------------------------------------------------- checks
check:            ## health, replication topology, LB spread
	$(TOOLS) python -m demo.check_stack

check-live:       ## WebSocket clients on every node receive a bid placed via one node
	$(TOOLS) python -m demo.check_live app2

check-invariants: ## [T4] prove money is conserved and both shards agree
	$(TOOLS) python -m demo.invariants

lint:             ## byte-compile all Python with 3.12 (catches syntax errors before deploying)
	$(TOOLS) python -m compileall -q app monitor demo tests

test:             ## pytest: unit + integration (RESETS demo data first)
	$(TOOLS) python -m pytest -q $(ARGS)

load:             ## load generator (DURATION=20 READERS=6 BIDDERS=3)
	$(TOOLS) python -m demo.loadgen --duration $(or $(DURATION),20) --readers $(or $(READERS),6) --bidders $(or $(BIDDERS),3)

# ---------------------------------------------------------------- the six demos
demo-replication: ## [T1] propagation, sync vs async, read-your-writes, cost of sync
	$(TOOLS) python -m demo.demo_replication

demo-fault:       ## [T2] kill app2, a DB replica and Redis under load; no committed bid lost
	$(TOOLS) python -m demo.demo_fault

demo-failover:    ## [T3] kill each shard's primary under load (SHARD=A|B): downtime, rejoin, no split-brain
	$(TOOLS) python -m demo.demo_failover $(SHARD)

demo-dtx:         ## [T4] 2PC: commit, insufficient funds, failure between PREPARE/COMMIT, concurrency
	$(TOOLS) python -m demo.demo_dtx

demo-recovery:    ## [T5] coordinator crash (in-doubt 2PC), DB crash (WAL redo), PITR, Redis loss
	$(TOOLS) python -m demo.demo_recovery

demo-lb:          ## [T6] distribution, node kill + redistribution, WebSocket fan-out
	$(TOOLS) python -m demo.demo_lb

demo-all:         ## run every demo from fresh seed data; outputs saved to docs/demo-output/
	$(TOOLS) python -m demo.demo_all
