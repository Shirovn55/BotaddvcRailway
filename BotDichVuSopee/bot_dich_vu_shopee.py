# -*- coding: utf-8 -*-
"""
BotDichVuSopee - Telegram bot backup
Flow chính:
- User bấm nút dịch vụ
- Bot yêu cầu gửi cookie
- Bot gọi API backend theo dịch vụ đã chọn
"""

import os
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests
from flask import Flask, request

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)

# =========================
# ENV
# =========================
BOT_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
TELEGRAM_WEBHOOK_SECRET = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()

API_BASE_URL = (os.getenv("API_BASE_URL") or "").strip()
API_BEARER_TOKEN = (os.getenv("API_BEARER_TOKEN") or "").strip()
TOOL_API_KEY = (os.getenv("TOOL_API_KEY") or "").strip()
BACKEND_TIMEOUT_SEC = int((os.getenv("BACKEND_TIMEOUT_SEC") or "35").strip())

PENDING_TTL_SEC = int((os.getenv("PENDING_TTL_SEC") or "900").strip())
UPDATE_WORKERS = int((os.getenv("UPDATE_WORKERS") or "10").strip())
DEBUG = (os.getenv("DEBUG") or "0").strip().lower() in ("1", "true", "yes")

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


def dprint(*args):
    if DEBUG:
        print("[DEBUG]", *args)


# =========================
# Action config
# =========================
ACTION_CHECK = "check"
ACTION_BUY_ACC = "buy_acc_shopee"
ACTION_BUY_PHONE = "buy_phone_shopee"
ACTION_FILTER_PHONE = "filter_phone_shopee"
ACTION_ADD_VOUCHER = "add_voucher"
ACTION_COOKIE_QR = "get_cookie_qr"
ACTION_ADD_MAIL = "add_mail"
ACTION_ADD_ADDRESS = "add_address_product"
ACTION_REFRESH_F_ST = "refresh_f_st"

ACTION_META = {
    ACTION_CHECK: {
        "label": "Q Check",
        "prompt": (
            "Bạn đang dùng <b>Q Check</b>.\n"
            "Gửi cookie để bắt đầu check.\n\n"
            "Bạn có thể gửi thêm tracking/ghi chú cùng tin nhắn."
        ),
        "endpoint": (os.getenv("EP_CHECK") or "/api/bot/check").strip(),
    },
    ACTION_BUY_ACC: {
        "label": "Mua ACC Shopee",
        "prompt": (
            "Bạn đang dùng <b>Mua ACC Shopee</b>.\n"
            "Gửi cookie + nội dung yêu cầu mua ACC."
        ),
        "endpoint": (os.getenv("EP_BUY_ACC") or "/api/bot/buy-acc-shopee").strip(),
    },
    ACTION_BUY_PHONE: {
        "label": "Mua Số Shopee",
        "prompt": (
            "Bạn đang dùng <b>Mua Số Shopee</b>.\n"
            "Gửi cookie + số lượng/mạng cần mua."
        ),
        "endpoint": (os.getenv("EP_BUY_PHONE") or "/api/bot/buy-phone-shopee").strip(),
    },
    ACTION_FILTER_PHONE: {
        "label": "Lọc Số Shopee",
        "prompt": (
            "Bạn đang dùng <b>Lọc Số Shopee</b>.\n"
            "Gửi cookie + nội dung lọc (nếu có)."
        ),
        "endpoint": (os.getenv("EP_FILTER_PHONE") or "/api/bot/filter-phone-shopee").strip(),
    },
    ACTION_ADD_VOUCHER: {
        "label": "Add Voucher",
        "prompt": (
            "Bạn đang dùng <b>Add Voucher</b>.\n"
            "Gửi cookie để lưu voucher."
        ),
        "endpoint": (os.getenv("EP_ADD_VOUCHER") or "/api/bot/add-voucher").strip(),
    },
    ACTION_COOKIE_QR: {
        "label": "Lấy Cookie QR",
        "prompt": (
            "Bạn đang dùng <b>Lấy Cookie QR</b>.\n"
            "Gửi cookie để hệ thống xử lý theo cấu hình backend."
        ),
        "endpoint": (os.getenv("EP_COOKIE_QR") or "/api/bot/get-cookie-qr").strip(),
    },
    ACTION_ADD_MAIL: {
        "label": "Add Mail",
        "prompt": (
            "Bạn đang dùng <b>Add Mail</b>.\n"
            "Gửi cookie + mail cần thêm."
        ),
        "endpoint": (os.getenv("EP_ADD_MAIL") or "/api/bot/add-mail").strip(),
    },
    ACTION_ADD_ADDRESS: {
        "label": "ADD Địa Chỉ - Sản Phẩm",
        "prompt": (
            "Bạn đang dùng <b>ADD Địa Chỉ - Sản Phẩm</b>.\n"
            "Gửi cookie + thông tin địa chỉ/sản phẩm cần thêm."
        ),
        "endpoint": (os.getenv("EP_ADD_ADDRESS") or "/api/bot/add-address-product").strip(),
    },
    ACTION_REFRESH_F_ST: {
        "label": "Làm mới F -> ST",
        "prompt": (
            "Bạn đang dùng <b>Làm mới F -> ST</b>.\n"
            "Gửi cookie để backend làm mới."
        ),
        "endpoint": (os.getenv("EP_REFRESH_F_ST") or "/api/bot/refresh-f-st").strip(),
    },
}

BUTTON_TO_ACTION = {
    "Q Check": ACTION_CHECK,
    "Mua ACC Shopee": ACTION_BUY_ACC,
    "Mua Số Shopee": ACTION_BUY_PHONE,
    "Lọc Số Shopee": ACTION_FILTER_PHONE,
    "Add Voucher": ACTION_ADD_VOUCHER,
    "Lấy Cookie QR": ACTION_COOKIE_QR,
    "Add Mail": ACTION_ADD_MAIL,
    "ADD Địa Chỉ - Sản Phẩm": ACTION_ADD_ADDRESS,
    "Làm mới F -> ST": ACTION_REFRESH_F_ST,
}

COMMAND_TO_ACTION = {
    "/check": ACTION_CHECK,
    "/muaacc": ACTION_BUY_ACC,
    "/muaso": ACTION_BUY_PHONE,
    "/locso": ACTION_FILTER_PHONE,
    "/addvoucher": ACTION_ADD_VOUCHER,
    "/laycookieqr": ACTION_COOKIE_QR,
    "/addmail": ACTION_ADD_MAIL,
    "/adddiachi": ACTION_ADD_ADDRESS,
    "/lammoifst": ACTION_REFRESH_F_ST,
}

PENDING_ACTIONS = {}
PENDING_LOCK = threading.Lock()

UPDATE_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, UPDATE_WORKERS), thread_name_prefix="dvs")


# =========================
# Telegram helpers
# =========================
def tg_call(method: str, payload: dict):
    if not BOT_TOKEN:
        return False, {"ok": False, "error": "TELEGRAM_TOKEN empty"}
    url = f"{BASE_URL}/{method}"
    try:
        r = requests.post(url, json=payload, timeout=20)
        data = r.json() if r.content else {}
        return r.ok and bool(data.get("ok")), data
    except Exception as e:
        return False, {"ok": False, "error": str(e)}


def build_main_keyboard():
    return {
        "keyboard": [
            [{"text": "Q Check"}, {"text": "Mua ACC Shopee"}],
            [{"text": "Mua Số Shopee"}, {"text": "Lọc Số Shopee"}],
            [{"text": "Add Voucher"}, {"text": "Lấy Cookie QR"}],
            [{"text": "Add Mail"}, {"text": "ADD Địa Chỉ - Sản Phẩm"}],
            [{"text": "Làm mới F -> ST"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
        "one_time_keyboard": False,
    }


def tg_send(chat_id: int, text: str):
    payload = {
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": build_main_keyboard(),
    }
    return tg_call("sendMessage", payload)


# =========================
# Pending state
# =========================
def now_ts() -> int:
    return int(time.time())


def set_pending_action(user_id: int, action: str):
    with PENDING_LOCK:
        PENDING_ACTIONS[int(user_id)] = {"action": action, "ts": now_ts()}


def get_pending_action(user_id: int):
    uid = int(user_id)
    with PENDING_LOCK:
        item = PENDING_ACTIONS.get(uid)
        if not item:
            return None
        if now_ts() - int(item.get("ts") or 0) > PENDING_TTL_SEC:
            PENDING_ACTIONS.pop(uid, None)
            return None
        return item


def clear_pending_action(user_id: int):
    with PENDING_LOCK:
        PENDING_ACTIONS.pop(int(user_id), None)


# =========================
# Backend helpers
# =========================
def _extract_cookie(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    for ln in lines:
        src = ln
        if src.lower().startswith("cookie:"):
            src = src.split(":", 1)[1].strip()
        if "SPC_ST=" in src or src.startswith("SPC_"):
            return src

    if "=" in raw and ";" in raw and " " not in raw:
        return raw
    return ""


def _backend_url(endpoint: str) -> str:
    ep = (endpoint or "").strip()
    if ep.startswith("http://") or ep.startswith("https://"):
        return ep
    base = (API_BASE_URL or "").strip()
    if not base:
        return ""
    return urljoin(base.rstrip("/") + "/", ep.lstrip("/"))


def _call_backend(action: str, payload: dict):
    meta = ACTION_META.get(action) or {}
    endpoint = (meta.get("endpoint") or "").strip()
    url = _backend_url(endpoint)
    if not url:
        return False, (
            f"Chưa cấu hình backend cho <b>{meta.get('label', action)}</b>.\n"
            "Hãy set API_BASE_URL hoặc endpoint env tương ứng."
        )

    headers = {"Content-Type": "application/json"}
    if TOOL_API_KEY:
        headers["X-Tool-Key"] = TOOL_API_KEY
    if API_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {API_BEARER_TOKEN}"

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=max(5, BACKEND_TIMEOUT_SEC))
    except Exception as e:
        return False, f"Lỗi gọi backend: {e}"

    try:
        data = resp.json()
    except Exception:
        data = {"ok": False, "message": (resp.text or "").strip()}

    ok = bool(data.get("ok", resp.ok))
    if ok:
        message = str(data.get("message") or "Xử lý thành công.")
        result = data.get("result")
        if result:
            message += f"\n\n<code>{result}</code>"
        return True, message

    err = str(data.get("error") or data.get("message") or f"HTTP {resp.status_code}")
    return False, err


# =========================
# Business flow
# =========================
def send_welcome(chat_id: int):
    msg = (
        "<b>BotDichVuSopee</b>\n"
        "Bấm nút dịch vụ bên dưới, bot sẽ yêu cầu gửi cookie.\n\n"
        "Lệnh nhanh: /check, /muaacc, /muaso, /locso, /addvoucher, /laycookieqr, /addmail, /adddiachi, /lammoifst\n"
        "Dùng <b>HUY</b> để hủy thao tác đang chờ."
    )
    tg_send(chat_id, msg)


def prompt_action(chat_id: int, action: str):
    meta = ACTION_META.get(action) or {}
    tg_send(chat_id, meta.get("prompt") or "Gửi cookie để tiếp tục.")


def handle_action_submission(chat_id: int, user_id: int, username: str, action: str, user_text: str):
    cookie = _extract_cookie(user_text)
    if not cookie:
        tg_send(
            chat_id,
            "Mình chưa thấy cookie hợp lệ.\n"
            "Vui lòng gửi lại cookie (có chứa SPC_ST hoặc SPC_*)."
        )
        return

    payload = {
        "action": action,
        "tele_id": int(user_id),
        "username": username or "",
        "cookie": cookie,
        "raw_input": user_text,
        "requested_at": now_ts(),
    }
    ok, result_msg = _call_backend(action, payload)
    if ok:
        clear_pending_action(user_id)
        tg_send(chat_id, f"✅ <b>{ACTION_META[action]['label']}</b>\n{result_msg}")
    else:
        tg_send(
            chat_id,
            f"❌ <b>{ACTION_META[action]['label']}</b> lỗi:\n{result_msg}\n\n"
            "Bạn có thể gửi lại cookie hoặc gõ HUY để thoát."
        )


def handle_message(msg: dict):
    chat = msg.get("chat") or {}
    from_user = msg.get("from") or {}
    text = (msg.get("text") or "").strip()
    if not text:
        return

    chat_id = int(chat.get("id") or 0)
    user_id = int(from_user.get("id") or 0)
    username = str(from_user.get("username") or "").strip()
    if chat_id <= 0 or user_id <= 0:
        return

    lower = text.lower().strip()
    if lower in ("/start", "start", "menu", "/menu"):
        clear_pending_action(user_id)
        send_welcome(chat_id)
        return

    if lower in ("huy", "huỷ", "/cancel", "cancel"):
        clear_pending_action(user_id)
        tg_send(chat_id, "Đã hủy thao tác đang chờ.")
        return

    # Button flow
    action = BUTTON_TO_ACTION.get(text)
    if action:
        set_pending_action(user_id, action)
        prompt_action(chat_id, action)
        return

    # Command flow: /check <cookie...>
    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    if cmd in COMMAND_TO_ACTION:
        action = COMMAND_TO_ACTION[cmd]
        if len(parts) > 1 and parts[1].strip():
            handle_action_submission(chat_id, user_id, username, action, parts[1].strip())
        else:
            set_pending_action(user_id, action)
            prompt_action(chat_id, action)
        return

    # If user has pending action, treat any text as cookie/input
    pending = get_pending_action(user_id)
    if pending:
        action = pending.get("action")
        if action in ACTION_META:
            handle_action_submission(chat_id, user_id, username, action, text)
            return

    tg_send(chat_id, "Chọn một tính năng bằng nút bấm hoặc dùng /start để mở menu.")


def process_update(update: dict):
    msg = update.get("message")
    if msg:
        handle_message(msg)


def safe_process_update(update: dict):
    try:
        process_update(update or {})
    except Exception:
        print("[process_update] error")
        traceback.print_exc()


# =========================
# Webhook routes
# =========================
@app.route("/", methods=["GET"])
def home():
    ready = bool(BOT_TOKEN)
    return {
        "service": "BotDichVuSopee",
        "ready": ready,
        "api_base_url": API_BASE_URL,
        "features": [v["label"] for v in ACTION_META.values()],
    }, 200 if ready else 503


@app.route("/webhook", methods=["POST"])
def webhook():
    if TELEGRAM_WEBHOOK_SECRET:
        recv = (request.headers.get("X-Telegram-Bot-Api-Secret-Token") or "").strip()
        if recv != TELEGRAM_WEBHOOK_SECRET:
            return "Unauthorized", 401

    update = request.get_json(silent=True, force=True) or {}
    if not update:
        return "bad request", 400

    UPDATE_EXECUTOR.submit(safe_process_update, update)
    return "ok", 200


def _normalize_url(raw_url: str) -> str:
    txt = (raw_url or "").strip()
    if not txt:
        return ""
    if not txt.startswith(("http://", "https://")):
        txt = "https://" + txt
    return txt.rstrip("/")


def _public_base_url() -> str:
    for key in ("WEBHOOK_URL", "APP_URL", "RAILWAY_PUBLIC_DOMAIN", "RAILWAY_STATIC_URL"):
        u = _normalize_url(os.getenv(key, ""))
        if not u:
            continue
        if u.endswith("/webhook"):
            return u[:-8]
        return u
    return ""


def ensure_webhook():
    if not BOT_TOKEN:
        print("⚠️ TELEGRAM_TOKEN trống -> bỏ qua setWebhook")
        return
    public_base = _public_base_url()
    if not public_base:
        print("⚠️ Chưa có URL public -> bỏ qua setWebhook")
        return

    payload = {
        "url": f"{public_base}/webhook",
        "allowed_updates": ["message"],
    }
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET

    ok, data = tg_call("setWebhook", payload)
    if ok:
        print(f"✅ setWebhook OK: {payload['url']}")
    else:
        print(f"⚠️ setWebhook fail: {data}")


if (os.getenv("AUTO_SET_WEBHOOK") or "1").strip().lower() in ("1", "true", "yes"):
    try:
        ensure_webhook()
    except Exception as ex:
        print("⚠️ ensure_webhook startup error:", ex)


if __name__ == "__main__":
    port = int((os.getenv("PORT") or "8080").strip())
    app.run(host="0.0.0.0", port=port)
