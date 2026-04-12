#!/bin/bash
# Development script with auto-reload
uv run uvicorn main:app --reload --app-dir worker --host 0.0.0.0 --port 8000 --reload-include "*.py" --reload-include ".env"
