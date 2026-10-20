# Integration tests against the services in compose.yaml.
#
#   make -C integration_tests test             start the services and run every test
#   make -C integration_tests test-postgres    only the PostgreSQL tests
#   make -C integration_tests test PYTEST_ARGS='-k conflicts -x'
#   make -C integration_tests down             stop the services and drop their data
#
# The services stay up after a run, so reruns start immediately. Recipes use relative
# paths because make splits absolute paths that contain spaces.

COMPOSE ?= docker compose -f compose.yaml
PYTHON ?= uv run --no-sync python
PYTEST_ARGS ?=
# One CI job's group: the services to start (empty: all), the tests and the report.
SERVICES ?=
TESTS ?= integration_tests --ignore=integration_tests/soak
REPORT ?= results.xml

export MAGE_TEST_POSTGRES_HOST ?= 127.0.0.1
export MAGE_TEST_POSTGRES_PORT ?= 15432
export MAGE_TEST_POSTGRES_DBNAME ?= test
export MAGE_TEST_POSTGRES_USER ?= postgres
export MAGE_TEST_POSTGRES_PASSWORD ?= test
export MAGE_TEST_REDIS_PORT ?= 16379
export MAGE_TEST_REDIS_URL ?= redis://127.0.0.1:$(MAGE_TEST_REDIS_PORT)/0
export MAGE_TEST_API_PORT ?= 18000
export MAGE_TEST_API_URL ?= http://127.0.0.1:$(MAGE_TEST_API_PORT)
export MAGE_TEST_FEAST_PORT ?= 16566
export MAGE_TEST_FEAST_URL ?= http://127.0.0.1:$(MAGE_TEST_FEAST_PORT)
export MAGE_TEST_MLFLOW_PORT ?= 15000
export MAGE_TEST_MLFLOW_URL ?= http://127.0.0.1:$(MAGE_TEST_MLFLOW_PORT)
export MAGE_TEST_S3_PORT ?= 19000
export MAGE_TEST_S3_ENDPOINT ?= http://127.0.0.1:$(MAGE_TEST_S3_PORT)
export MAGE_TEST_S3_ACCESS_KEY ?= mage-test
export MAGE_TEST_S3_SECRET_KEY ?= mage-test-secret
export MAGE_TEST_MYSQL_HOST ?= 127.0.0.1
export MAGE_TEST_MYSQL_PORT ?= 13306
export MAGE_TEST_MYSQL_DATABASE ?= test
export MAGE_TEST_MYSQL_USER ?= root
export MAGE_TEST_MYSQL_PASSWORD ?= test
export MAGE_TEST_CLICKHOUSE_HOST ?= 127.0.0.1
export MAGE_TEST_CLICKHOUSE_PORT ?= 18123
export MAGE_TEST_CLICKHOUSE_USER ?= mage
export MAGE_TEST_CLICKHOUSE_PASSWORD ?= test
export MAGE_TEST_KAFKA_PORT ?= 19092
export MAGE_TEST_KAFKA_BOOTSTRAP ?= 127.0.0.1:$(MAGE_TEST_KAFKA_PORT)
export MAGE_TEST_MONGODB_PORT ?= 17017
export MAGE_TEST_MONGODB_URL ?= mongodb://127.0.0.1:$(MAGE_TEST_MONGODB_PORT)/?directConnection=true
export MAGE_TEST_TRINO_HOST ?= 127.0.0.1
export MAGE_TEST_TRINO_PORT ?= 18080
export MAGE_TEST_RABBITMQ_PORT ?= 15673
export MAGE_TEST_RABBITMQ_USER ?= mage
export MAGE_TEST_RABBITMQ_PASSWORD ?= p@ss/w:rd%
export MAGE_TEST_DB2_PORT ?= 15050
export MAGE_TEST_DB2_PASSWORD ?= db2-Pass1
export MAGE_TEST_IBM_MQ_PORT ?= 11414
export MAGE_TEST_IBM_MQ_WEB_PORT ?= 19443
export MAGE_TEST_IBM_MQ_USER ?= app
export MAGE_TEST_IBM_MQ_PASSWORD ?= p@ssw0rd
export MAGE_TEST_IBM_MQ_ADMIN_PASSWORD ?= adm1nPass
# mage service deploy builds an image and runs it in this Docker.
export MAGE_TEST_DOCKER_DEPLOY ?= 1
export MAGE_TEST_NATS_PORT ?= 14222
export MAGE_TEST_NATS_URL ?= nats://127.0.0.1:$(MAGE_TEST_NATS_PORT)
export MAGE_TEST_TOXIPROXY_PORT ?= 18474
export MAGE_TEST_TOXIPROXY_PROXY_PORT ?= 18666
export MAGE_TEST_TOXIPROXY_URL ?= http://127.0.0.1:$(MAGE_TEST_TOXIPROXY_PORT)
export MAGE_TEST_ACTIVEMQ_PORT ?= 16613
export MAGE_TEST_SQS_PORT ?= 19324
export MAGE_TEST_SQS_URL ?= http://127.0.0.1:$(MAGE_TEST_SQS_PORT)
export MAGE_TEST_PUBSUB_PORT ?= 18681
export MAGE_TEST_PUBSUB_HOST ?= 127.0.0.1:$(MAGE_TEST_PUBSUB_PORT)
export MAGE_TEST_OPENSEARCH_PORT ?= 19200
export MAGE_TEST_OPENSEARCH_URL ?= http://127.0.0.1:$(MAGE_TEST_OPENSEARCH_PORT)
export MAGE_TEST_INFLUXDB_PORT ?= 18086
export MAGE_TEST_PINOT_CONTROLLER_PORT ?= 19900
export MAGE_TEST_PINOT_BROKER_PORT ?= 18999
export MAGE_TEST_PINOT_CONTROLLER_URL ?= http://127.0.0.1:$(MAGE_TEST_PINOT_CONTROLLER_PORT)
export MAGE_TEST_INFLUXDB_URL ?= http://127.0.0.1:$(MAGE_TEST_INFLUXDB_PORT)
# MLflow prints a hint for coding agents on import.
export MLFLOW_DISABLE_AGENT_HINT ?= 1
# The rv environment of the R tests: R 4.6 with the tidyverse, mageml's
# dependencies, RPostgres, testthat, lintr and roxygen2.
export MAGE_TEST_R_PROJECT_DIR ?= $(CURDIR)/r_env
RV ?= rv

.PHONY: up down warm-rust ci-tests test test-postgres test-redis test-api test-feast test-mlflow test-duckdb test-s3 test-mysql test-mongodb test-pinot test-clickhouse test-kafka test-trino test-rabbitmq test-nats test-sqs test-pubsub test-opensearch test-influxdb test-deploy test-activemq test-streaming test-r test-soak test-kafka-outage test-benchmark benchmark-pipelines ci r-env

up:
	cd .. && $(PYTHON) integration_tests/tls_certs.py
	$(COMPOSE) up -d --build --wait $(SERVICES)

down:
	$(COMPOSE) down --volumes --remove-orphans

# Installs the R test environment's packages as rv.lock pins them.
r-env:
	cd r_env && $(RV) sync

test: up r-env
	cd .. && $(PYTHON) -m pytest integration_tests --ignore=integration_tests/soak $(PYTEST_ARGS)

# Block fusion against block by block; needs no services. MAGE_BENCHMARK_ROWS sets the rows.
test-benchmark:
	cd .. && $(PYTHON) -m pytest integration_tests/benchmark -s $(PYTEST_ARGS)

# Whole pipelines (pandas, Polars, R, Rust and a staggered pandas-to-Rust series) run with
# mage run, the scheduler, fused stages and exported services; see
# integration_tests/benchmark/pipelines/run.py. Writes results and a report under
# integration_tests/benchmark/results/. BENCHMARK_ARGS='--rows 1000000 --repeats 3'.
benchmark-pipelines: r-env
	cd .. && $(PYTHON) -m integration_tests.benchmark.pipelines $(BENCHMARK_ARGS)

test-postgres: up
	cd .. && $(PYTHON) -m pytest integration_tests/postgres $(PYTEST_ARGS)

test-redis: up
	cd .. && $(PYTHON) -m pytest integration_tests/redis $(PYTEST_ARGS)

test-api: up
	cd .. && $(PYTHON) -m pytest integration_tests/api $(PYTEST_ARGS)

test-feast: up
	cd .. && $(PYTHON) -m pytest integration_tests/feast $(PYTEST_ARGS)

test-mlflow: up
	cd .. && $(PYTHON) -m pytest integration_tests/mlflow $(PYTEST_ARGS)

test-s3: up
	cd .. && $(PYTHON) -m pytest integration_tests/s3 $(PYTEST_ARGS)

test-mysql: up
	cd .. && $(PYTHON) -m pytest integration_tests/mysql $(PYTEST_ARGS)

test-mongodb: up
	cd .. && $(PYTHON) -m pytest integration_tests/mongodb $(PYTEST_ARGS)

test-pinot: up
	cd .. && $(PYTHON) -m pytest integration_tests/pinot $(PYTEST_ARGS)

test-clickhouse: up
	cd .. && $(PYTHON) -m pytest integration_tests/clickhouse $(PYTEST_ARGS)

test-kafka: up
	cd .. && $(PYTHON) -m pytest integration_tests/kafka $(PYTEST_ARGS)

test-trino: up
	cd .. && $(PYTHON) -m pytest integration_tests/trino $(PYTEST_ARGS)

test-rabbitmq: up
	cd .. && $(PYTHON) -m pytest integration_tests/rabbitmq $(PYTEST_ARGS)

test-nats: up
	cd .. && $(PYTHON) -m pytest integration_tests/nats $(PYTEST_ARGS)

test-sqs: up
	cd .. && $(PYTHON) -m pytest integration_tests/sqs $(PYTEST_ARGS)

test-pubsub: up
	cd .. && $(PYTHON) -m pytest integration_tests/pubsub $(PYTEST_ARGS)

test-opensearch: up
	cd .. && $(PYTHON) -m pytest integration_tests/opensearch $(PYTEST_ARGS)

test-deploy:
	cd .. && $(PYTHON) -m pytest integration_tests/deploy $(PYTEST_ARGS)

test-influxdb: up
	cd .. && $(PYTHON) -m pytest integration_tests/influxdb $(PYTEST_ARGS)

test-activemq: up
	cd .. && $(PYTHON) -m pytest integration_tests/activemq $(PYTEST_ARGS)

test-streaming: up
	cd .. && $(PYTHON) -m pytest integration_tests/streaming $(PYTEST_ARGS)

test-r: up r-env
	cd .. && $(PYTHON) -m pytest integration_tests/r $(PYTEST_ARGS)

# The scheduler with many concurrent pipelines, for MAGE_TEST_SOAK_SECONDS (default 150).
# A Kafka broker down for six minutes under a running stream (needs Docker and Kafka; the
# test runs its own broker). Not part of test-soak.
test-kafka-outage:
	cd .. && MAGE_TEST_KAFKA_OUTAGE=1 $(PYTHON) -m pytest integration_tests/soak/test_kafka_long_outage.py -p no:xdist -rs $(PYTEST_ARGS)

# It runs on its own: make test-soak MAGE_TEST_SOAK_SECONDS=1800
test-soak: up
	cd .. && MAGE_JOB_ENGINE=off $(PYTHON) -m pytest integration_tests/soak -p no:xdist -rs \
		$(PYTEST_ARGS)

# The same, with every chain of blocks running as one stage (block_fusion: chains).
test-soak-fusion: up
	cd .. && MAGE_JOB_ENGINE=off MAGE_TEST_SOAK_FUSION=1 $(PYTHON) -m pytest integration_tests/soak -p no:xdist \
		-rs $(PYTEST_ARGS)

# The same with the scheduler's jobs on the Rust run engine (docs/development/run-engine.md),
# and the engine queue's own tests. ENGINE is the mage-service binary with the jobs command.
ENGINE ?= ../mage_ai/pipeline_services/service/target/release/mage-service
test-soak-engine: up
	cd .. && MAGE_JOB_ENGINE=on MAGE_ENGINE_PATH=$(abspath $(ENGINE)) $(PYTHON) -m pytest \
		mage_ai/tests/orchestration/queue/test_engine_queue.py \
		integration_tests/soak/test_scheduler.py integration_tests/soak/test_worker_crash.py \
		-p no:xdist -rs $(PYTEST_ARGS)

# DuckDB runs in process; these tests need no service.
test-duckdb:
	cd .. && $(PYTHON) -m pytest integration_tests/duckdb $(PYTEST_ARGS)

# Builds Polars and the Rust block SDK into the shared target directory once, so no
# test pays for the cold build within its time limit: the blocks Mage compiles, and the
# bundle an exported service compiles, whose features differ. Needs the services (up).
WARM_TESTS ?= \
	'integration_tests/chains/test_compiled_chains.py::test_a_rust_chain_runs_compiled_with_the_outputs_of_block_by_block' \
	'integration_tests/postgres/test_rust_pipelines.py::test_the_pipeline_runs_as_an_exported_service'
warm-rust:
	cd .. && $(PYTHON) -m pytest -p no:cacheprovider -q $(WARM_TESTS)

# CI fails on any skipped test, so a service that is not configured cannot pass silently.
ci: up r-env ci-tests

# The tests of TESTS against services already started (CI starts each job's own).
ci-tests:
	cd .. && $(PYTHON) -m pytest $(TESTS) -rs \
		--junitxml=integration_tests/$(REPORT) $(PYTEST_ARGS)
	$(PYTHON) check_no_skips.py $(REPORT)
