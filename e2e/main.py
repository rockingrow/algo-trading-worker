from datetime import datetime
from enum import Enum
from typing import Optional

import uvicorn
import zmq
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Import schemas from production worker to ensure compatibility
try:
  from worker.schemas.broker_schema import SignalSchema

  logger_name = "e2e.simulator"
except ImportError:
  # Fallback if running outside of project root context
  # This is less likely but good for robustness
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

# ZeroMQ Setup
ZMQ_PUB_HOST = "tcp://*:5555"
zmq_context = zmq.Context()
zmq_socket = zmq_context.socket(zmq.PUB)


@app.on_event("startup")
def startup_event():
  print(f"🚀 Starting Broker Simulator (PUB) at {ZMQ_PUB_HOST}...")
  zmq_socket.bind(ZMQ_PUB_HOST)
  print("✅ Simulator bound and ready to broadcast.")


@app.on_event("shutdown")
def shutdown_event():
  print("🛑 Shutting down Simulator...")
  zmq_socket.close()
  zmq_context.term()


@app.post("/simulator/publish")
async def publish_signal(signal: SignalSchema):
  """
  Receives a signal via HTTP and broadcasts it over ZMQ.
  """
  try:
    # Serialize with Pydantic's model_dump_json to handle datetime properly
    message = signal.model_dump_json()
    print(f"📡 Publishing: {signal.symbol} | {signal.position.action.value}")
    zmq_socket.send_string(message)
    return {
      "status": "success",
      "message": "Signal broadcasted to ZMQ bus",
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
