# Distributed Live Auction Engine — all commands.
COMPOSE := docker compose
TOOLS   := $(COMPOSE) run --rm tools


.PHONY: help build redeploy up down clean ps logs status check seed check-live load demo-lb demo-replication check-invariants demo-dtx test demo-fault demo-failover

help:            ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build:           ## rebuild all images (next `make up` then recreates the DB containers too)
	$(COMPOSE) --profile tools build

redeploy:        ## rebuild + restart only the stateless tier (apps, monitor) — DBs untouched
	$(COMPOSE) up -d --build --no-deps --wait app1 app2 app3 monitor

SERVICES := etcd pg-a1 pg-a2 pg-a3 pg-b1 pg-b2 redis app1 app2 app3 haproxy monitor

# Note: `up` builds images only if they are missing. Image builds are not reproducible here,
# so rebuilding on every `up` would make Compose recreate (restart) the database containers.
up:              ## start the whole stack, wait until healthy, seed if empty
	$(COMPOSE) --profile tools build tools
	$(COMPOSE) up -d --wait $(SERVICES)
	$(COMPOSE) run --rm --no-deps seed

down:            ## stop the stack (keeps data volumes)
	$(COMPOSE) down

clean:           ## stop the stack and DELETE all data volumes
	$(COMPOSE) --profile tools down -v --remove-orphans

ps:              ## container status
	$(COMPOSE) ps

logs:            ## follow logs of all services
	$(COMPOSE) logs -f --tail=50

status:          ## Patroni view of both DB clusters
	@for n in pg-a1 pg-a2 pg-a3; do $(COMPOSE) exec -T $$n patronictl -c /etc/patroni/patroni.yml list 2>/dev/null && break; done
	@for n in pg-b1 pg-b2; do $(COMPOSE) exec -T $$n patronictl -c /etc/patroni/patroni.yml list 2>/dev/null && break; done

check:           ## verify the stack: health, replication topology, LB spread
	$(TOOLS) python -m demo.check_stack

seed:            ## seed data if empty (RESET=1 to wipe and reseed)
	$(TOOLS) python -m demo.seed $(if $(RESET),--reset,)

check-live:      ## WebSocket clients on every node receive a bid placed via one node
	$(TOOLS) python -m demo.check_live app2

load:            ## run the load generator (DURATION=20 READERS=6 BIDDERS=3)
	$(TOOLS) python -m demo.loadgen --duration $(or $(DURATION),20) --readers $(or $(READERS),6) --bidders $(or $(BIDDERS),3)

demo-lb:         ## [T6] load balancing: distribution, node kill + redistribution, WS fan-out
	$(TOOLS) python -m demo.demo_lb

demo-replication: ## [T1] replication: propagation, sync vs async, read-your-writes, sync cost
	$(TOOLS) python -m demo.demo_replication

check-invariants: ## [T4] prove money is conserved and both shards agree
	$(TOOLS) python -m demo.invariants

demo-dtx:        ## [T4] 2PC: normal, insufficient funds, failure between PREPARE/COMMIT, 100 concurrent bids
	$(TOOLS) python -m demo.demo_dtx

test:            ## pytest: unit + integration (RESETS demo data first)
	$(TOOLS) python -m pytest -q $(ARGS)

demo-fault:      ## [T2] kill app2, a DB replica and Redis under load; no committed bid lost
	$(TOOLS) python -m demo.demo_fault

demo-failover:   ## [T3] kill each shard's primary under load: measured downtime, rejoin, no split-brain
	$(TOOLS) python -m demo.demo_failover $(SHARD)
