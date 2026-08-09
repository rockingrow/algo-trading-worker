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
