# Distributed Live Auction Engine — all commands.
COMPOSE := docker compose
TOOLS   := $(COMPOSE) run --rm tools

.PHONY: help build up down clean ps logs status check seed

help:            ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build:           ## build all images
	$(COMPOSE) --profile tools build

SERVICES := etcd pg-a1 pg-a2 pg-a3 pg-b1 pg-b2 redis app1 app2 app3 haproxy

up:              ## start the whole stack, wait until healthy, seed if empty
	$(COMPOSE) build
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
