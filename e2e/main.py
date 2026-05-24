from datetime import datetime
from enum import Enum
from typing import Optional

import nats
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
  from worker.schemas.broker_schema import SignalSchema
except ImportError:

  class SignalActionEnum(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    TP1 = "TP1"
    TP2 = "TP2"
    R_SL = "R_SL"
    SL = "SL"

  class PositionSchema(BaseModel):
    action: SignalActionEnum
    price: float
    quantity: Optional[float] = None
    sl: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    is_running: Optional[bool] = None

  class InputsSchema(BaseModel):
    risk_percent: float

  class SignalSchema(BaseModel):
    token: Optional[str] = None
    symbol: str
    timeframe: str
    timestamp: datetime
    position: PositionSchema
    inputs: Optional[InputsSchema] = None


app = FastAPI(title="Algo Trading - E2E Broker Simulator")

NATS_URL = "nats://localhost:4222"
NATS_SUBJECT = "signals"

_nc = None


@app.on_event("startup")
async def startup_event():
  global _nc
  print(
    f"🚀 Starting Broker Simulator (NATS PUB) at {NATS_URL}, subject={NATS_SUBJECT}..."
  )
  _nc = await nats.connect(NATS_URL)
  print("✅ Simulator connected to NATS and ready to publish.")


@app.on_event("shutdown")
async def shutdown_event():
  global _nc
  print("🛑 Shutting down Simulator...")
  if _nc:
    await _nc.drain()


@app.post("/simulator/publish")
async def publish_signal(signal: SignalSchema):
  """Receives a signal via HTTP and publishes it to NATS."""
  try:
    payload = signal.model_dump_json().encode()
    print(f"📡 Publishing: {signal.symbol} | {signal.position.action.value}")
    await _nc.publish(NATS_SUBJECT, payload)
    return {
      "status": "success",
      "message": "Signal published to NATS",
      "payload": {"symbol": signal.symbol, "action": signal.position.action},
    }
  except Exception as e:
    print(f"❌ Error publishing: {e}")
    raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/health")
async def health():
  return {"status": "ok", "mode": "e2e_simulator"}


if __name__ == "__main__":
  uvicorn.run(app, host="0.0.0.0", port=8001)
