.PHONY: init install install-dev run dev test format lint fix

init:
	uv run python scripts/init_env.py

install:
	uv sync

install-dev:
	uv sync --dev

start:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	uv run python -m uvicorn worker.main:app --host "$${APP_HOST:-0.0.0.0}" --port "$${APP_PORT:-8000}" --log-level info

dev:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	uv run python -m uvicorn main:app --reload --app-dir worker --host "$${APP_HOST:-0.0.0.0}" --port "$${APP_PORT:-8000}" --reload-include "*.py" --reload-include ".env"

test:
	uv run pytest

format:
	uv run ruff format .

lint:
	uv run ruff check .

fix:
	uv run ruff check --fix .
