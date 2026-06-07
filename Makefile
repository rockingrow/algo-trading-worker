.PHONY: install install-dev run dev test format lint fix docker-build docker-up docker-down docker-logs

install:
	uv sync

install-dev:
	uv sync --dev

start:
	uv run python -m uvicorn worker.main:app --host 0.0.0.0 --port 8000 --log-level info

dev:
	uv run python -m uvicorn main:app --reload --app-dir worker --host 0.0.0.0 --port 8000 --reload-include "*.py" --reload-include ".env"

e2e:
	uv run e2e/main.py

test:
	uv run pytest

format:
	uv run ruff format .

lint:
	uv run ruff check .

fix:
	uv run ruff check --fix .

generate-keys:
	.venv\Scripts\python.exe scripts/generate_curve_keypair.py

# ── Docker (crypto worker, Linux) ──────────────────────────────────────────
docker-build:
	docker compose build

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f worker
