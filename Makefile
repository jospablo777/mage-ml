UV ?= uv

install-hooks:
	@echo "Copying git hooks from .git-dev/hooks to .git/hooks..."
	@bash scripts/devex/copy_git_hooks.sh .git-dev/hooks .git/hooks

dev_env:
	@echo "Creating the uv environment with dev dependencies"
	$(UV) sync --locked --group dev

dev_env_all:
	@echo "Creating the uv environment with production extras and dev dependencies"
	$(UV) sync --locked --extra all --extra integrations --group dev

lock:
	$(UV) lock

lock-check:
	$(UV) lock --check

test:
	$(UV) run --no-sync pytest

requirements:
	$(UV) export --locked --no-hashes --no-annotate --no-emit-workspace \
		--no-default-groups --extra all --extra integrations \
		--output-file requirements.txt

# The playground: Mage built from this repository, PostgreSQL with a seeded shop, and
# pipelines in pandas, Polars, R and geopandas. See playground/README.md.
#   make playground            build, start and wait until Mage answers
#   make playground-check      run every pipeline once
# SEED_SCALE, PLAYGROUND_MAGE_PORT and PLAYGROUND_POSTGRES_PORT pass through.
PLAYGROUND := docker compose -f playground/compose.yaml
# The builder of the current Docker context, which `docker build` uses too, so builds
# share one cache. Another buildx builder would compile everything again.
PLAYGROUND_BUILDER ?= $(shell docker context show 2>/dev/null)
PLAYGROUND_URL = http://localhost:$(or $(PLAYGROUND_MAGE_PORT),6789)
export SEED_SCALE PLAYGROUND_MAGE_PORT PLAYGROUND_POSTGRES_PORT

playground: playground-build playground-up

playground-build:
	BUILDX_BUILDER=$(PLAYGROUND_BUILDER) $(PLAYGROUND) build

playground-up:
	$(PLAYGROUND) up -d
	@echo "Waiting for Mage (the first start seeds the database, a minute or two)..."
	@for i in $$(seq 1 300); do \
		if curl -fs -o /dev/null $(PLAYGROUND_URL)/api/status; then \
			echo "Mage is running at $(PLAYGROUND_URL)"; exit 0; fi; \
		if $(PLAYGROUND) ps -a seed --format '{{.State}} {{.ExitCode}}' | grep -q '^exited [1-9]'; then \
			echo "Seeding failed:"; $(PLAYGROUND) logs --tail 30 seed; exit 1; fi; \
		sleep 2; \
	done; \
	echo "Mage did not answer in 10 minutes; see make playground-logs"; exit 1

playground-logs:
	$(PLAYGROUND) logs -f seed mage

playground-ps:
	$(PLAYGROUND) ps -a

playground-check:
	./playground/check.sh

playground-psql:
	$(PLAYGROUND) exec postgres psql -U mage -d playground

playground-stop:
	$(PLAYGROUND) stop

playground-start: playground-up

playground-restart:
	$(PLAYGROUND) restart mage

# Removes the containers; the database and Mage's data stay.
playground-down:
	$(PLAYGROUND) down

# Deletes the database and Mage's data.
playground-reset:
	@read -p "Delete the playground database and Mage data? [y/N] " answer; \
	if [ "$$answer" = y ] || [ "$$answer" = Y ]; then $(PLAYGROUND) down -v; else echo "Kept."; fi

playground-reseed:
	$(PLAYGROUND) run --rm seed python /home/src/seed/seed.py --force

.PHONY: install-hooks dev_env dev_env_all lock lock-check test requirements \
	playground playground-build playground-up playground-logs playground-ps \
	playground-check playground-psql playground-stop playground-start \
	playground-restart playground-down playground-reset playground-reseed
