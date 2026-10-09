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
export MAGE_TEST_MONGODB_PORT ?= 17017
export MAGE_TEST_MONGODB_URL ?= mongodb://127.0.0.1:$(MAGE_TEST_MONGODB_PORT)/?directConnection=true
# MLflow prints a hint for coding agents on import.
export MLFLOW_DISABLE_AGENT_HINT ?= 1
# The rv environment of the R tests: R 4.6 with the tidyverse, mageml's
# dependencies, RPostgres, testthat, lintr and roxygen2.
export MAGE_TEST_R_PROJECT_DIR ?= $(CURDIR)/r_env
RV ?= rv

.PHONY: up down test test-postgres test-redis test-api test-feast test-mlflow test-duckdb test-s3 test-mysql test-mongodb test-clickhouse test-r test-soak ci r-env

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down --volumes --remove-orphans

# Installs the R test environment's packages as rv.lock pins them.
r-env:
	cd r_env && $(RV) sync

test: up r-env
	cd .. && $(PYTHON) -m pytest integration_tests --ignore=integration_tests/soak $(PYTEST_ARGS)

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

test-clickhouse: up
	cd .. && $(PYTHON) -m pytest integration_tests/clickhouse $(PYTEST_ARGS)

test-r: up r-env
	cd .. && $(PYTHON) -m pytest integration_tests/r $(PYTEST_ARGS)

# The scheduler with many concurrent pipelines, for MAGE_TEST_SOAK_SECONDS (default 150).
# It runs on its own: make test-soak MAGE_TEST_SOAK_SECONDS=1800
test-soak: up
	cd .. && $(PYTHON) -m pytest integration_tests/soak -p no:xdist -rs $(PYTEST_ARGS)

# DuckDB runs in process; these tests need no service.
test-duckdb:
	cd .. && $(PYTHON) -m pytest integration_tests/duckdb $(PYTEST_ARGS)

# CI fails on any skipped test, so a service that is not configured cannot pass silently.
ci: up r-env
	cd .. && $(PYTHON) -m pytest integration_tests --ignore=integration_tests/soak -rs \
		--junitxml=integration_tests/results.xml $(PYTEST_ARGS)
	$(PYTHON) check_no_skips.py results.xml
