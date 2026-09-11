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

.PHONY: install-hooks dev_env dev_env_all lock lock-check test requirements
