.PHONY: install install-dev run dev format lint fix

install:
	uv sync

install-dev:
	uv sync --dev

run:
	uv run worker/main.py

dev:
	uv run uvicorn main:app --reload --app-dir worker --host 0.0.0.0 --port 8000 --reload-include "*.py" --reload-include ".env"

format:
	uv run ruff format .

lint:
	uv run ruff check .

fix:
	uv run ruff check --fix .
