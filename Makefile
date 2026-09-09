.PHONY: help up down ui logs install test produce consume dlq clean

VENV ?= venv
PY   ?= $(VENV)/bin/python

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up:            ## Start Kafka + Zookeeper and create topics
	docker compose up -d
	docker compose logs kafka-init

down:          ## Stop the stack and delete volumes
	docker compose down -v

ui:            ## Start the stack plus kafka-ui on http://localhost:8080
	docker compose --profile tools up -d

logs:          ## Follow broker logs
	docker compose logs -f kafka

install:       ## Create a venv and install dependencies
	python3 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements-dev.txt

test:          ## Run the unit test suite
	$(PY) -m pytest

produce:       ## Send demo orders. Vars: N, POISON, FLAKY, INTERVAL
	$(PY) run_producer.py --count $(or $(N),50) \
	  --poison-rate $(or $(POISON),0.1) \
	  --flaky-rate $(or $(FLAKY),0.2) \
	  --interval $(or $(INTERVAL),0.2)

consume:       ## Run the consumer
	$(PY) run_consumer.py

dlq:           ## Dump the Dead Letter Queue with headers (exits after 5s idle)
	-docker exec -i kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 --topic orders-dlq \
	  --from-beginning --timeout-ms 5000 --property print.headers=true

clean:         ## Remove the venv and Python caches
	rm -rf $(VENV) .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
