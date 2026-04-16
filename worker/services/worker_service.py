import asyncio
from worker.db import log_order
from worker.logger import get_logger
from worker.mt5.mt5 import MT5
from worker.mt5.executor import MT5Executor
from worker.services.notifications_service import TelegramNotification
from worker.services.zmq_service import ZMQ

log = get_logger("worker.services.worker_service")


async def run_worker_loop(subscriber: ZMQ, executor: MT5Executor, bridge: MT5):
  notifier = TelegramNotification()
  footer = bridge.get_account_footer()

  log.info("MT5 Worker loop started.")
  notifier.send_message(f"🟢 <b>MT5 Worker connected</b>" + footer)

  try:
    loop = asyncio.get_event_loop()

    def pull_signals():
      for signal in subscriber.listen():
        log.info(
          f"Processing Signal: {signal.symbol} | {signal.position.action} | TV Time: {signal.timestamp}"
        )

        # Execute trade
        result = executor.execute_signal(signal)

        # Log to database
        log_order(
          ticket=result.get("ticket"),
          symbol=signal.symbol,
          action=signal.position.action,
          volume=result.get("volume", signal.position.quantity),
          price=result.get("price", signal.position.price),
          sl=getattr(signal.position, "sl", None),
          tp1=getattr(signal.position, "tp1", None),
          mt5_retcode=result.get("retcode", -1),
          comment=result.get("comment", ""),
        )

        # Telegram notification
        if result.get("success"):
          msg = (
            f"✅ <b>Order Filled</b>\n\n"
            f"Symbol: {signal.symbol}\n"
            f"Action: {signal.position.action}\n"
            f"Volume: {result.get('volume')}\n"
            f"Ticket: {result.get('ticket')}" + footer
          )
          notifier.send_message(msg)
        else:
          msg = (
            f"❌ <b>Order Failed</b>\n\n"
            f"Symbol: {signal.symbol}\n"
            f"Action: {signal.position.action}\n"
            f"Error: {result.get('comment')} (Code {result.get('retcode')})" + footer
          )
          notifier.send_message(msg)

    await loop.run_in_executor(None, pull_signals)

  except asyncio.CancelledError:
    log.info("Worker loop cancelled.")
  except Exception as e:
    log.error(f"Error in worker loop: {e}")
  finally:
    notifier.send_message(f"🛑 <b>MT5 Worker disconnected</b>" + footer)
