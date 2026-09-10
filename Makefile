# Kafka Order Processing -- task runner
#
# Everything runs through `make <target>`; you never need to type a docker or
# python command directly. Run `make` (or `make help`) to see all targets.
#
# The virtualenv is created OUTSIDE this repo on purpose: the project lives on a
# drive mounted `noexec`, where a venv's console scripts (pip, pytest) cannot be
# executed. `make` always invokes the interpreter as `$(PYTHON) -m ...`, which
# works regardless. Override the location with:  make <target> VENV=/some/path
# ---------------------------------------------------------------------------

VENV        ?= $(HOME)/.venvs/kafka-order-processing
PYTHON      := $(VENV)/bin/python
PY_BOOTSTRAP ?= python3
STAMP       := $(VENV)/.installed

COMPOSE     := docker compose
PROFILE     := --profile tools          # so `down` also removes kafka-ui

# producer knobs -- override on the CLI, e.g.  make produce N=100 FLAKY=0.3
N           ?= 50
POISON      ?= 0.1
FLAKY       ?= 0.2
INTERVAL    ?= 0.2
START       ?= 1001

.DEFAULT_GOAL := help
.PHONY: help env install reinstall test \
        up ui start stop down reset restart logs topics groups \
        produce consume dlq demo doctor clean

# ---- help ----------------------------------------------------------------
help:  ## Show this help
	@echo "Kafka Order Processing -- make targets:"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Override vars:  VENV=$(VENV)"
	@echo "               N=$(N) POISON=$(POISON) FLAKY=$(FLAKY) INTERVAL=$(INTERVAL) START=$(START)"

# ---- python environment ------------------------------------------------
$(STAMP): requirements.txt requirements-dev.txt
	@mkdir -p $(dir $(VENV))
	$(PY_BOOTSTRAP) -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt
	@touch $(STAMP)
	@echo
	@echo ">>> environment ready: $(VENV)"

install: $(STAMP)  ## Create the virtualenv and install dependencies

reinstall:  ## Rebuild the virtualenv from scratch
	rm -rf $(VENV)
	$(MAKE) install

env:  ## Print the virtualenv path / how to activate it manually
	@echo "VENV     = $(VENV)"
	@echo "python   = $(PYTHON)"
	@echo "activate = source $(VENV)/bin/activate"

test: $(STAMP)  ## Run the unit test suite
	$(PYTHON) -m pytest

# ---- infrastructure (Kafka lifecycle) --------------------------------
up:  ## Start Kafka + Zookeeper and create the topics
	$(COMPOSE) up -d
	@echo ">>> waiting for topic creation..."
	@$(COMPOSE) logs kafka-init 2>/dev/null | tail -n 5

ui:  ## Start Kafka + Zookeeper + kafka-ui (http://localhost:8080)
	$(COMPOSE) $(PROFILE) up -d
	@echo ">>> Kafka UI: http://localhost:8080"

start: ui  ## Alias for `ui` -- bring the whole stack up

restart:  ## Recreate the stack cleanly (fixes stale container/network errors)
	$(COMPOSE) $(PROFILE) down --remove-orphans
	$(COMPOSE) $(PROFILE) up -d
	@echo ">>> Kafka UI: http://localhost:8080"

stop:  ## Stop the containers but KEEP topics + messages
	$(COMPOSE) $(PROFILE) stop

down:  ## Stop + remove containers and network (data volumes kept)
	$(COMPOSE) $(PROFILE) down --remove-orphans

reset:  ## Tear everything down AND delete all topics + messages
	$(COMPOSE) $(PROFILE) down -v --remove-orphans

logs:  ## Follow the Kafka broker log
	$(COMPOSE) logs -f kafka

topics:  ## List Kafka topics
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

groups:  ## Show consumer-group offsets and lag
	docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
	  --describe --group order-processing-group

# ---- run the pipeline ------------------------------------------------
produce: $(STAMP)  ## Send orders (vars: N POISON FLAKY INTERVAL START)
	$(PYTHON) run_producer.py --count $(N) --poison-rate $(POISON) \
	  --flaky-rate $(FLAKY) --interval $(INTERVAL) --start-id $(START)

consume: $(STAMP)  ## Run the consumer (Ctrl-C stops it and prints the summary)
	-$(PYTHON) run_consumer.py

dlq:  ## Dump the Dead Letter Queue with headers (exits after 5s idle)
	@echo "+ kafka-console-consumer --topic orders-dlq --from-beginning --property print.headers=true"
	@docker exec -i kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 --topic orders-dlq \
	  --from-beginning --timeout-ms 5000 --property print.headers=true 2>&1 \
	  | grep -avE '^\[[0-9]{4}-|TimeoutException|^[[:space:]]+at ' || true

demo: $(STAMP)  ## Start the stack and send a lively batch, then tells you what's next
	$(MAKE) ui
	@echo ">>> giving Kafka time to elect a coordinator..."
	@sleep 15
	$(PYTHON) run_producer.py --count $(N) --poison-rate 0.2 --flaky-rate 0.3 --interval 0.1
	@echo
	@echo ">>> now run:  make consume     (Ctrl-C it to see the running-average summary)"
	@echo ">>> and open: http://localhost:8080"

# ---- diagnostics / cleanup ------------------------------------------
doctor:  ## Check docker, compose and the virtualenv are usable
	@command -v docker >/dev/null 2>&1 && echo "docker    : OK" || echo "docker    : MISSING"
	@$(COMPOSE) version >/dev/null 2>&1 && echo "compose   : OK" || echo "compose   : MISSING"
	@command -v $(PY_BOOTSTRAP) >/dev/null 2>&1 && echo "python3   : OK ($$($(PY_BOOTSTRAP) --version))" || echo "python3   : MISSING"
	@test -x $(PYTHON) && echo "venv      : OK ($(VENV))" || echo "venv      : not built -- run 'make install'"
	@docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^kafka$$' && echo "kafka     : running" || echo "kafka     : stopped -- run 'make up'"

clean:  ## Remove Python caches (keeps the virtualenv)
	rm -rf .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
