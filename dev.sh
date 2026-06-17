#!/bin/bash
# Development script with auto-reload
set -a; [ -f .env ] && source .env; set +a
uv run uvicorn main:app --reload --app-dir worker --host "${APP_HOST:-0.0.0.0}" --port "${APP_PORT:-8000}" --reload-include "*.py" --reload-include ".env"
