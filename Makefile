# stream-pipeline task runner. Every target works on Linux, macOS, and Windows (Git Bash).
COMPOSE ?= docker compose

.PHONY: lint unit test integration up up-full down seed e2e fmt

lint:
	uv run ruff check src tests scripts
	uv run ruff format --check src tests scripts
	uv run mypy

fmt:
	uv run ruff format src tests scripts
	uv run ruff check --fix src tests scripts

unit:
	uv run pytest -m "not integration" --cov --cov-fail-under=80

test: lint unit

integration:
	uv run pytest -m integration -x

up:
	$(COMPOSE) --profile core up -d --wait

up-full:
	$(COMPOSE) --profile full up -d --wait

down:
	$(COMPOSE) --profile full down -v --remove-orphans

seed:
	uv run python -m generator --sink kafka --speed 60 --duration 120 --seed 42

dbt-build:
	uv run dbt build --project-dir dbt --profiles-dir dbt

dq:
	uv run python -m quality.runner --mode warn

dq-fail:
	uv run python -m quality.runner --mode fail

kill-test:
	uv run pytest tests/integration/test_exactly_once.py -m integration -v

e2e:
	bash scripts/e2e.sh
