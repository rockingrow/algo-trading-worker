# 🧪 End-to-End (E2E) Testing Guide

This directory contains the tools necessary to simulate a live trading environment. By using the **E2E Broker Simulator**, you can broadcast signals to the Worker without needing an actual TradingView webhook or an external ZMQ Broker.

---

## 🏗️ Architecture Overview

The E2E testing flow works as follows:

1.  **Bruno (Client)**: Sends a REST `POST` request with a signal payload (JSON) to the Simulator.
2.  **Simulator (ZMQ PUB)**: Receives the JSON and broadcasts it via ZeroMQ (Topic: `""`).
3.  **Worker (ZMQ SUB)**: Native production listener picks up the signal from the ZMQ bus.
4.  **SignalHandler**: Orchestrates the execution logic (stale cleanup, entries, partial closes, etc.).
5.  **MT5Executor**: Executes the actual orders in your MetaTrader 5 Terminal.

---

## 🚦 Getting Started

To run a full E2E test, you must have two separate terminal windows open.

### 1. Start the Production Worker
In the project root, start the worker that connects to MT5 and listens for signals.
```bash
uv run python -m worker.main
```
*   **Port**: `8000` (API/Health)
*   **ZMQ SUB**: Listens on `tcp://localhost:5555`

### 2. Start the E2E Broker Simulator
In a second terminal, start the standalone simulator.
```bash
uv run python -m e2e.main
```
*   **Port**: `8001` (REST API)
*   **ZMQ PUB**: Binds to `tcp://*:5555`

---

## 🛠️ Testing with Bruno

We have provided a pre-configured Bruno collection in the `bruno/e2e` folder.

1.  **Open Bruno** and load the project collection.
2.  Set the environment to **Local**.
3.  Open the **e2e** folder in the collection.
4.  Select a signal to test:

### Test Cases

| Signal | Description | Logic Verified |
|---|---|---|
| **`LONG` / `SHORT`** | Opens a new position. | Stale position cleanup + Risk-based lot sizing + Hard SL settings. |
| **`TP1`** | Partial profit taking. | Partial close via counter-order + SL update to Breakeven. |
| **`TP2` / `SL` / `R_SL`** | Closes the full position. | Full exit using actual MT5 volume (ignoring signal quantity). |

---

## 📝 Important Notes

> [!IMPORTANT]
> **MT5 Terminal Requirements**:
> - MetaTrader 5 must be open and logged into an account.
> - **"Allow Algo Trading"** must be enabled in `Options > Expert Advisors`.

> [!WARNING]
> **ZMQ Port Conflicts**:
> Ensure no other ZMQ brokers are running on port `5555` before starting the simulator, or it will fail to bind.

---

## 💻 Manual Simulation via CURL
If you don't want to use Bruno, you can send signals manually using `curl`:

```bash
curl -X POST http://localhost:8001/simulator/publish \
     -H "Content-Type: application/json" \
     -d '{
       "symbol": "XAUUSD",
       "timeframe": "5",
       "timestamp": "2026-04-10 22:55:00",
       "position": {
         "action": "LONG",
         "price": 2334.5,
         "quantity": 6.0,
         "sl": 2329.5
       }
     }'
```
