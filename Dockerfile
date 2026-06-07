# syntax=docker/dockerfile:1
#
# Crypto (CEX) worker image — Linux, cross-platform.
#
# Only the CRYPTO market is containerized: the MT5/FOREX path needs the Windows
# MetaTrader5 terminal and is excluded automatically (its dependency is marked
# `sys_platform == 'win32'` in pyproject.toml, so `uv sync` on Linux skips it).
#
# Build:  docker build -t algo-trading-worker .
# Run:    see docker-compose.yml

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

# uv for fast, lockfile-pinned installs.
COPY --from=ghcr.io/astral-sh/uv:0.8.17 /uv /bin/uv

WORKDIR /app

# 1) Dependency layer — cached unless pyproject.toml / uv.lock change.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 2) Project source, then install the project itself.
COPY README.md ./
COPY worker ./worker
RUN uv sync --frozen --no-dev

# Writable data dir for the SQLite DB (mount a volume here in compose).
RUN mkdir -p /app/data && \
    useradd --create-home --uid 10001 appuser && \
    chown -R appuser:appuser /app
USER appuser

ENV PATH="/app/.venv/bin:$PATH" \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000

EXPOSE 8000

# Matches the Makefile `start` target; logs go to stdout (captured by Docker).
CMD ["uvicorn", "worker.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
