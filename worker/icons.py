"""Centralised Telegram icon palette using the `emoji` library.

All notification modules import named constants from here instead of
embedding raw Unicode codepoints, so the full icon set lives in one place.
"""

import emoji as _e

CONNECTED = _e.emojize(":green_circle:")  # 🟢
DISCONNECTED = _e.emojize(":red_circle:")  # 🔴
STOP = _e.emojize(":stop_sign:")  # 🛑
WARNING = _e.emojize(":warning:")  # ⚠️
SUCCESS = _e.emojize(":check_mark_button:")  # ✅
FAILED = _e.emojize(":cross_mark:")  # ❌
SHIELD = _e.emojize(":shield:")  # 🛡️
ALARM = _e.emojize(":rotating_light:")  # 🚨
ERROR_ALERT = _e.emojize(":police_car_light:")  # 🚨 (forwarded ERROR logs)
REJECTED = _e.emojize(":prohibited:")  # 🚫
SYNC = _e.emojize(":counterclockwise_arrows_button:")  # 🔄
ADMIN = _e.emojize(":high_voltage:")  # ⚡
UNKNOWN = _e.emojize(":red_question_mark:")  # ❓
GEAR = _e.emojize(":gear:")  # ⚙️
MANUAL = _e.emojize(":hand_with_fingers_splayed:")  # 🖐
BROKER = _e.emojize(":electric_plug:")  # 🔌
RETRYING = _e.emojize(":hourglass_not_done:")  # ⏳
PROFIT = _e.emojize(":chart_increasing:")  # 📈 (a close booked in profit)
LOSS = _e.emojize(":chart_decreasing:")  # 📉 (a close booked at a loss)

# ── Signal actions ─────────────────────────────────────────────────────────
# Shown per action in a signal cycle's timeline. Matched to the broker's
# palette (broker/helpers/emoji_constants.py) so one trade reads the same in
# the broker's channel and in the worker's.
LONG = _e.emojize(":chart_increasing:")  # 📈
SHORT = _e.emojize(":chart_decreasing:")  # 📉
TP1 = _e.emojize(":bullseye:")  # 🎯
TP2 = _e.emojize(":rocket:")  # 🚀
STOP_LOSS = _e.emojize(":hollow_red_circle:")  # ⭕
FLAT = _e.emojize(":white_flag:")  # 🏳️
SIGNAL = _e.emojize(":satellite_antenna:")  # 📡

# ── Signal-cycle status ────────────────────────────────────────────────────
# The headline of a cycle message: one line that is rewritten in place as the
# position moves, so the reader sees where the trade stands without scrolling.
CYCLE_OPENED = _e.emojize(":hourglass_not_done:")  # ⏳ position live
CYCLE_CLOSED = _e.emojize(":chequered_flag:")  # 🏁 position finished
BAR_CHART = _e.emojize(":bar_chart:")  # 📊
