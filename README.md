# 🚀 Algo Trading MT5 Worker

This is the execution-end of the Event-Driven trading system. It acts as a ZeroMQ subscriber waiting for highly structured trading signals from the central Broker, then executes them directly into the MetaTrader 5 Terminal.

## 🏗️ Architecture Stack
- **MetaTrader5**: Official MT5 python integration for direct terminal execution.
- **ZMQ SUB**: Resilient ZeroMQ subscriber picking up broadcasted signals.
- **Pydantic**: Deep JSON validation parsing ensuring zero execution mismatches.
- **Loguru**: Beautiful structured JSON-capable logging.
- **SQLite**: Local persistence capturing every ticker intent and subsequent MT5 execution retcode.
- **UV**: Blazing fast Python environment initialization.

---

## 📂 Project Structure

```text
worker/
├── apis/                # Web API & routing
│   └── api.py           # Health & system routes
├── helpers/             # Utility & mapping helpers
├── mt5/                 # MetaTrader 5 integration
│   ├── mt5.py    # MT5 terminal connection bridge
│   └── executor.py # Core trade execution logic
├── schemas/             # Pydantic data schemas
│   └── broker_schema.py # Signal & position validation schemas
├── services/            # Business & Infrastructure services
│   ├── notifications_service.py # Telegram notification logic
│   ├── worker_service.py        # Main background execution loop
│   └── zmq_service.py           # ZeroMQ signal subscriber
├── app.py               # FastAPI application factory & lifespan
├── db.py                # Local SQLite persistence layer
├── logger.py            # Structured logging configuration
├── main.py              # Application entry point
├── router.py            # Core router aggregation
└── settings.py          # Environment & app configuration
```

---

## ⚡ Quick Start

### 1. Requirements
Ensure you are running on Windows, as the Python `MetaTrader5` module restricts usage to Windows environments only.

### 2. Setup
Because we orchestrate dependencies via `uv`, setup is instantaneous.

```bash
# Sẽ tạo môi trường ảo và cài đặt tất cả dependencies từ pyproject.toml
uv sync
```

### 3. Cấu hình .env
Sao chép `.env.example` thành `.env` và điền chi tiết kết nối MT5 (Exness Demo/Real) cùng cấu hình ZeroMQ Broker.
```bash
cp .env.example .env
```

Vui lòng chắc chắn rằng bạn đã kích hoạt "Allow Algo Trading" bên trong Options > Expert Advisors của Terminal MetaTrader 5.

### 4. Vận hành
Khởi động Worker (từ thư mục gốc):
```bash
uv run python -m worker.main
```
Worker sẽ cấu trúc db `worker_data.sqlite`, móc vào MT5 qua bridge, và mở socket Subscribe tới ZeroMQ. Bạn có thể monitor logs in thẳng ra màn hình.