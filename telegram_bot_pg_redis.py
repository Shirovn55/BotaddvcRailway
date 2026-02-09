# -*- coding: utf-8 -*-
"""
NgânMiu.Store — Telegram Bot
✅ V4 FIXED - Sửa schema 7 cột + Anti-spam 5req/20s + Thưởng user mới 5100đ
✅ Schema 7 cột: Tele ID | Username | Balance | Trang Thái | Chi Chú | note | Gift Status
✅ Anti-spam: 5 request/20s → Ban 1H → Tái phạm → Ban vĩnh viễn
✅ Thưởng user mới: 5100đ (balance không bao giờ về 0)
✅ Batch update (giảm API calls)
✅ Retry logic (tăng stability)
✅ ⭐ HỖ TRỢ LƯU TỐI ĐA 10 COOKIE CÙNG LÚC ⭐
✅ 🔥 ROW CACHE + BROADCAST CACHE - GIẢM 90% SHEET CALLS 🔥
✅ 🎯 BROADCAST FIX (2025-02-04) - LẤY USER TỪ POSTGRESQL THAY VÌ CHỈ CACHE 🎯
"""

import os
import json
import re
import unicodedata
import requests
import random  # ✅ THÊM RANDOM CHO CHECK VOUCHER
from datetime import datetime, timedelta, timezone
from flask import Flask, request

# =========================================================
# PG + REDIS (Wallet DB + Anti-spam)
# =========================================================
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import redis
from contextlib import contextmanager

import urllib.parse
import time
import traceback
from collections import deque  # ✅ Thêm deque cho PROCESSED_UPDATE_IDS

# =========================================================
# TIMEZONE VIETNAM (GMT+7)
# =========================================================
VIETNAM_TZ = timezone(timedelta(hours=7))

# =========================================================
# LOAD DOTENV
# =========================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# =========================================================
# GOOGLE SHEET
# =========================================================
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# APP
# =========================================================
app = Flask(__name__)

# =========================================================
# ENV
# =========================================================
BOT_TOKEN  = os.getenv("TELEGRAM_TOKEN", "").strip()
SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "").strip()
CREDS_JSON = os.getenv("GOOGLE_SHEETS_CREDS_JSON", "").strip()
ADMIN_ID   = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
REDIS_URL    = os.getenv("REDIS_URL", "").strip()

# Mirror ví tiền ra Google Sheet để bạn theo dõi (không bắt buộc)
SHEET_MIRROR_WALLET = os.getenv("SHEET_MIRROR_WALLET", "1").strip() in ("1","true","True","YES","yes")


BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
SAVE_URL = "https://shopee.vn/api/v2/voucher_wallet/save_vouchers"

# =========================================================
# PG POOL + REDIS CLIENT
# =========================================================
PG_POOL = None
RDS = None

def _init_pg():
    global PG_POOL
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL trống -> bot sẽ fallback dùng Google Sheet cho ví tiền (không khuyến nghị).")
        return
    if PG_POOL is None:
        PG_POOL = SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DATABASE_URL,
        )

@contextmanager
def pg_conn():
    """Lấy connection từ pool, tự trả lại."""
    if PG_POOL is None:
        yield None
        return
    conn = PG_POOL.getconn()
    try:
        yield conn
    finally:
        try:
            PG_POOL.putconn(conn)
        except Exception:
            pass

def pg_exec(sql: str, params=None, fetchone=False, fetchall=False):
    if PG_POOL is None:
        return None
    with pg_conn() as conn:
        if conn is None:
            return None
        conn.autocommit = False
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            out = None
            if fetchone:
                out = cur.fetchone()
            elif fetchall:
                out = cur.fetchall()
            conn.commit()
            return out
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            dprint(f"PG error: {e}")
            return None
        finally:
            try:
                cur.close()
            except Exception:
                pass

def pg_init_tables():
    """Tạo bảng ví + bảng chống nạp trùng (tx_id)"""
    if PG_POOL is None:
        return
    pg_exec("""
    CREATE TABLE IF NOT EXISTS wallet (
        tele_id BIGINT PRIMARY KEY,
        username TEXT,
        balance BIGINT NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active',
        notes TEXT,
        gift TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
    pg_exec("""
    CREATE TABLE IF NOT EXISTS processed_tx (
        tx_id TEXT PRIMARY KEY,
        tele_id BIGINT NOT NULL,
        amount BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)

def _init_redis():
    global RDS
    if not REDIS_URL:
        print("⚠️ REDIS_URL trống -> anti-spam sẽ dùng RAM (restart là reset).")

        return
    try:
        RDS = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        RDS.ping()
    except Exception as e:
        print("⚠️ Redis init lỗi:", e)
        RDS = None

def system_init_pg_redis():
    _init_pg()
    pg_init_tables()
    _init_redis()


# =========================================================
# ⭐ MULTI-COOKIE CONFIG ⭐
# =========================================================
MAX_COOKIES_PER_REQUEST = 10
COOKIE_SEPARATOR = "\n"

# =========================================================
# TOPUP RULES (SEPAY)
# =========================================================
MIN_TOPUP_AMOUNT = 10000

# ✅ TIỀN THƯỞNG USER MỚI (5100đ để balance không bao giờ về 0)
NEW_USER_BONUS = 5100

# ✅ TIỀN THƯỞNG KÍCH HOẠT (thống nhất với NEW_USER_BONUS)
ACTIVE_GIFT_AMOUNT = 5100

# ✅ STATUS CHO PHÉP NHẬN GIFT (chặt chẽ, tránh abuse)
ALLOWED_GIFT_STATUS = ["", "new", "pending"]  # Admin set "inactive" → KHÔNG được nhận

# =========================================================
# 🔥 QR LOGIN CONFIG
# =========================================================
QR_API_BASE = os.getenv("QR_API_BASE", "https://qr-shopee-rho.vercel.app").strip()
QR_POLL_INTERVAL = 3.0  # giây check 1 lần
QR_TIMEOUT = 300  # 5 phút timeout
COOKIE_VALIDITY_DAYS = 7  # Cookie hiệu lực 7 ngày

# QR Session Management
import threading
qr_sessions = {}  # {session_id: {"user_id": user_id, "created": timestamp, "status": "waiting", "qr_image": base64}}
qr_lock = threading.Lock()

# QR Failure Tracking (chống spam get QR)
qr_failures = {}  # {user_id: {"count": int, "last_fail": timestamp}}
qr_failures_lock = threading.Lock()
MAX_QR_FAILURES = 5  # 5 lần thất bại liên tục → ban vĩnh viễn

# Cookie storage cho voucher nhanh
user_last_cookies = {}  # {user_id: {"cookie": str, "timestamp": float}}
user_cookies_lock = threading.Lock()

TOPUP_BONUS_RULES = [
    (100000, 0.20),
    (50000,  0.15),
    (20000,  0.10),
]


def normalize_voucher_key(s: str) -> str:
    """Chuẩn hoá key voucher để match ổn định (xoá mọi whitespace kể cả NBSP)."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip().lower()
    s = re.sub(r"\s+", "", s)  # space/tab/NBSP/newline...
    return s


def calc_topup_bonus(amount):
    for min_amount, percent in TOPUP_BONUS_RULES:
        if amount >= min_amount:
            bonus = int(amount * percent)
            return percent, bonus
    return 0, 0

def build_sepay_qr(user_id, amount=None):
    base = "https://qr.sepay.vn/img"
    params = {
        "acc": "101866911892",
        "bank": "VietinBank",
        "template": "compact",
        "des": f"SEVQR NAP {user_id}"
    }
    if amount:
        params["amount"] = str(int(amount))
    return base + "?" + urllib.parse.urlencode(params)

# =========================================================
# ANTI-SPAM CONFIG
# =========================================================
SPAM_THRESHOLD = 5   # 5 request spam
SPAM_WINDOW = 20     # trong 20 giây
BAN_DURATION_1H = 3600

# =========================================================
# DEBUG FLAG
# =========================================================
DEBUG = True

def dprint(*args):
    if DEBUG:
        print("[DEBUG]", *args)

# =========================================================
# GOOGLE SHEET CONNECT WITH RETRY
# =========================================================
SHEET_READY = False
sh          = None
ws_money    = None
ws_voucher  = None
ws_log      = None
ws_nap_tien = None
ws_cookies  = None  # ✅ TAB COOKIE CHO CHECK VOUCHER

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

MAX_RETRIES = 3
retry_count = 0
connected = False

while retry_count < MAX_RETRIES and not connected:
    try:
        if not CREDS_JSON:
            raise Exception("CREDS_JSON is empty")

        print(f"🔄 Connecting to Google Sheets (attempt {retry_count + 1}/{MAX_RETRIES})...")
        start_time = time.time()

        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(CREDS_JSON),
            scope
        )
        print(f"✅ Step 1: Credentials loaded ({time.time()-start_time:.2f}s)")

        gc = gspread.authorize(creds)
        print(f"✅ Step 2: Gspread authorized ({time.time()-start_time:.2f}s)")

        sh = gc.open_by_key(SHEET_ID)
        print(f"✅ Step 3: Sheet opened ({time.time()-start_time:.2f}s)")

        ws_money   = sh.worksheet("Thanh Toan")
        ws_voucher = sh.worksheet("VoucherStock")
        ws_log     = sh.worksheet("Logs")
        print(f"✅ Step 4: Core worksheets loaded ({time.time()-start_time:.2f}s)")

        try:
            ws_nap_tien = sh.worksheet("Nap Tien")
            print(f"✅ Step 5: Nap Tien loaded ({time.time()-start_time:.2f}s)")
        except Exception as e:
            ws_nap_tien = None
            print(f"⚠️ Nap Tien tab not found: {e}")

        # ✅ Load tab Cookie cho chức năng Check Voucher
        try:
            ws_cookies = sh.worksheet("Cookie")
            print(f"✅ Step 6: Cookie tab loaded ({time.time()-start_time:.2f}s)")
        except Exception as e:
            ws_cookies = None
            print(f"⚠️ Cookie tab not found: {e}")

        SHEET_READY = True
        connected = True
        print("=" * 60)
        print("✅ ✅ ✅ GOOGLE SHEETS CONNECTED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        retry_count += 1
        wait_time = 2 ** retry_count

        print("=" * 60)
        print(f"❌ Connection failed (attempt {retry_count}/{MAX_RETRIES})")
        print(f"❌ Error: {str(e)}")
        print(f"❌ Error type: {type(e).__name__}")

        if retry_count < MAX_RETRIES:
            print(f"⏳ Retrying in {wait_time}s...")
            time.sleep(wait_time)
        else:
            print("❌ ❌ ❌ ALL RETRIES FAILED - SHEET_READY = False")
            import traceback
            traceback.print_exc()
            print("=" * 60)
            SHEET_READY = False

# =========================================================
# ✅ Init PostgreSQL + Redis — LUÔN chạy, không phụ thuộc Sheet
# =========================================================
print("🔄 Initializing PostgreSQL + Redis...")
system_init_pg_redis()

# =========================================================
# 🔥 PRELOAD USERS + ROW CACHE (chạy 1 lần khi khởi động)
# =========================================================
if SHEET_READY:
    print("🔄 Preloading users + row numbers into cache...")
    try:
        all_users = ws_money.get_all_values()
        preload_count = 0

        for idx, row in enumerate(all_users[1:], start=2):  # start=2 vì header ở row 1
            if len(row) >= 1 and row[0]:
                try:
                    user_id = int(row[0])

                    # ✅ CACHE ROW NUMBER sẽ được khai báo sau
                    # cache_user_row(user_id, idx)
                    preload_count += 1
                except Exception:
                    continue

        print(f"✅ Will preload {preload_count} users into cache")

    except Exception as e:
        print(f"⚠️ Preload failed (non-critical): {e}")

# =========================================================
# STATE (GLOBAL)
# =========================================================
PENDING_VOUCHER = {}
PENDING_VOUCHER_TTL = 120  # 2 phút - expire nếu user không gửi cookie

# ✅ DYNAMIC COMBO DETECTION - Không hardcode, tự phát hiện từ Sheet
# Combo nào có trong VoucherStock với Combo = "combo1", "combo2"... đều tự động hiện
# COMBO1_KEY, COMBO2_KEY... sẽ được detect tự động

# ✅ CALLBACK RATE LIMIT - Tránh spam click BUY
CALLBACK_COOLDOWN = {}
CALLBACK_COOLDOWN_SECONDS = 2  # 2 giây giữa các click

# ✅ SPAM TRACKER
SPAM_TRACKER = {}

# =========================================================
# 🔥 ROW NUMBER CACHE - GIẢM 80% SHEET API CALLS
# =========================================================
USER_ROW_CACHE = {}
USER_ROW_CACHE_TTL = 3600  # 1 giờ
USER_ROW_CACHE_TIME = {}

def cache_user_row(user_id, row_number):
    """Cache row number của user"""
    USER_ROW_CACHE[user_id] = row_number
    USER_ROW_CACHE_TIME[user_id] = time.time()
    dprint(f"✅ Cached row for user {user_id}: row {row_number}")

def get_cached_user_row(user_id):
    """Get row number từ cache. Returns: row_number hoặc None"""
    if user_id not in USER_ROW_CACHE:
        return None
    cache_time = USER_ROW_CACHE_TIME.get(user_id, 0)
    if time.time() - cache_time > USER_ROW_CACHE_TTL:
        del USER_ROW_CACHE[user_id]
        del USER_ROW_CACHE_TIME[user_id]
        return None
    return USER_ROW_CACHE[user_id]

def invalidate_user_row_cache(user_id):
    """Xóa row cache khi cần"""
    if user_id in USER_ROW_CACHE:
        del USER_ROW_CACHE[user_id]
        del USER_ROW_CACHE_TIME[user_id]

# =========================================================
# 🔥 BROADCAST USER CACHE
# =========================================================
BROADCAST_USER_CACHE = None
BROADCAST_USER_CACHE_TIME = 0
BROADCAST_USER_CACHE_TTL = 300  # 5 phút

# ✅ BROADCAST COOLDOWN
LAST_BROADCAST_TIME = None
BROADCAST_COOLDOWN = 60

# ✅ MESSAGE DEDUPLICATION
PROCESSED_MESSAGES = set()
MAX_PROCESSED_MESSAGES = 1000

# ✅ UPDATE_ID DEDUPLICATION - Tránh Telegram resend khi Sheet lag
# Dùng deque thay vì set để xóa theo thứ tự FIFO
PROCESSED_UPDATE_IDS = deque(maxlen=2000)  # Auto-drop oldest when full

# ✅ BROADCAST LOCK
IS_BROADCASTING = False

# =========================================================
# 🔥 CHẠY PRELOAD THỰC SỰ (SAU KHI ĐỊNH NGHĨA CACHE FUNCTIONS)
# =========================================================
if SHEET_READY:
    print("🔄 Actually preloading users into ROW_CACHE...")
    try:
        all_users = ws_money.get_all_values()
        preload_count = 0

        for idx, row in enumerate(all_users[1:], start=2):
            if len(row) >= 1 and row[0]:
                try:
                    user_id = int(row[0])
                    cache_user_row(user_id, idx)
                    preload_count += 1
                except Exception:
                    continue

        print(f"✅ Preloaded {preload_count} users into ROW_CACHE")
        print(f"✅ Cache stats: {len(USER_ROW_CACHE)} row numbers cached")

    except Exception as e:
        print(f"⚠️ Preload failed (non-critical): {e}")

# =========================================================
# 🔥 VOUCHER STOCK CACHE - GIẢM 90% CALLS KHI MUA VOUCHER
# =========================================================
VOUCHER_STOCK_CACHE = {
    "rows": None,
    "ts": 0
}
VOUCHER_STOCK_TTL = 60  # 60 giây

def get_voucher_stock_cached():
    """
    ✅ Cache voucher stock 60s để tránh đốt Sheet
    Returns: list of dict
    """
    global VOUCHER_STOCK_CACHE
    
    now = time.time()
    
    # Check cache
    if VOUCHER_STOCK_CACHE["rows"] and (now - VOUCHER_STOCK_CACHE["ts"] < VOUCHER_STOCK_TTL):
        dprint("✅ VOUCHER_STOCK_CACHE HIT")
        return VOUCHER_STOCK_CACHE["rows"]
    
    # Cache miss → gọi Sheet
    dprint("⚠️ VOUCHER_STOCK_CACHE MISS, calling Sheet...")
    
    if not SHEET_READY:
        return []
    
    try:
        rows = ws_voucher.get_all_records()
        VOUCHER_STOCK_CACHE["rows"] = rows
        VOUCHER_STOCK_CACHE["ts"] = now
        dprint(f"✅ Cached {len(rows)} vouchers")
        return rows
    except Exception as e:
        dprint(f"❌ get_voucher_stock_cached error: {e}")
        # Fallback: trả cache cũ nếu có
        if VOUCHER_STOCK_CACHE["rows"]:
            dprint("⚠️ Using stale cache")
            return VOUCHER_STOCK_CACHE["rows"]
        return []

# =========================================================
# TELEGRAM UTIL
# =========================================================
def tg_send(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    try:
        requests.post(f"{BASE_URL}/sendMessage", data=payload, timeout=15)
    except Exception as e:
        dprint("tg_send error:", e)

def tg_send_photo(chat_id, photo, caption=None, reply_markup=None):
    payload = {"chat_id": chat_id, "parse_mode": "HTML"}
    if caption:
        payload["caption"] = caption
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    try:
        # ✅ Nếu là URL (nạp tiền / ảnh online) → gửi thẳng
        if isinstance(photo, str) and (photo.startswith("http://") or photo.startswith("https://")):
            payload["photo"] = photo
            requests.post(f"{BASE_URL}/sendPhoto", data=payload, timeout=20)
            return

        # ✅ Nếu là base64 (QR login) → multipart
        import base64
        photo_bytes = base64.b64decode(photo)
        files = {"photo": ("qr.png", photo_bytes, "image/png")}
        requests.post(f"{BASE_URL}/sendPhoto", data=payload, files=files, timeout=20)

    except Exception as e:
        dprint("tg_send_photo error:", e)
        if caption:
            tg_send(chat_id, f"📷 {caption}\n\n❌ Không thể gửi ảnh QR")

# Wrapper cho QR functions
def send_photo(chat_id, photo, caption=None, reply_markup=None):
    """Alias cho QR functions - support reply_markup"""
    tg_send_photo(chat_id, photo, caption, reply_markup)

def send_message(chat_id, text, reply_markup=None):
    """Alias cho QR functions"""
    tg_send(chat_id, text, reply_markup)

def tg_answer_callback(callback_id, text=None, show_alert=False):
    payload = {
        "callback_query_id": callback_id,
        "show_alert": show_alert
    }
    if text:
        payload["text"] = text

    try:
        requests.post(f"{BASE_URL}/answerCallbackQuery", data=payload, timeout=10)
    except Exception as e:
        dprint("tg_answer_callback error:", e)

def tg_edit_message(chat_id, message_id, text, reply_markup=None):
    """
    Edit message text và inline keyboard
    """
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    try:
        requests.post(f"{BASE_URL}/editMessageText", data=payload, timeout=10)
    except Exception as e:
        dprint("tg_edit_message error:", e)

# =========================================================
# KEYBOARD
# =========================================================
def build_main_keyboard(is_active=True):
    """
    Keyboard chính - 2 nút mỗi hàng cho gọn
    """
    return {
        "keyboard": [
            ["💎 Nạp tiền", "💰 Số dư"],
            ["🎁 Lưu Voucher", "📊 Check Voucher"],
            ["🔑 Get Cookie QR", "🧩 Hệ Thống Bot"],  # ✅ GỘP VÀO 1 HÀNG
            ["🖥️ Tải & Lấy Pass Tool ADD PC"]
        ],
        "resize_keyboard": True
    }

# =========================================================
# 📊 CHECK VOUCHER FUNCTIONS
# =========================================================
def get_cookie_from_sheet():
    """
    Lấy cookie ngẫu nhiên từ tab Cookie trong Google Sheet
    Trả về cookie string hoặc None nếu không có
    """
    if not SHEET_READY or ws_cookies is None:
        return None
    
    try:
        # Lấy tất cả giá trị từ cột A (Cookie)
        cookie_column = ws_cookies.col_values(1)
        
        # Bỏ qua header (dòng 1) và filter cookie hợp lệ
        valid_cookies = []
        for i, cell in enumerate(cookie_column):
            if i == 0:  # Skip header
                continue
            
            cell_str = str(cell).strip()
            # Cookie phải chứa "SPC_ST" và đủ dài
            if cell_str and "SPC_ST" in cell_str and len(cell_str) > 50:
                valid_cookies.append(cell_str)
        
        if not valid_cookies:
            dprint("❌ Không tìm thấy cookie hợp lệ trong tab Cookie")
            return None
        
        # Random pick 1 cookie
        selected_cookie = random.choice(valid_cookies)
        dprint(f"✅ Đã chọn cookie: {selected_cookie[:50]}...")
        return selected_cookie
        
    except Exception as e:
        dprint(f"get_cookie_from_sheet error: {e}")
        return None


def format_currency_check(value):
    """Format số tiền theo định dạng VN"""
    if not value: 
        return "0đ"
    value = float(value)
    if value > 100000000: 
        value = value / 100000
    return "{:,.0f}đ".format(value).replace(",", ".")


def check_one_voucher(voucher, cookie):
    """
    Check 1 voucher và trả về thông tin formatted
    Trả về: (success: bool, message: str)
    """
    url = "https://shopee.vn/api/v2/voucher_wallet/get_voucher_detail"
    
    headers = {
        'User-Agent': 'Android app Shopee appver=28320 app_type=1',
        'Cookie': cookie,
        'Content-Type': 'application/json'
    }

    payload = {
        "promotionid": voucher['promotionid'],
        "voucher_code": voucher['code'],
        "signature": voucher['signature'],
        "need_basic_info": True,
        "need_user_voucher_status": True,
        "source": "0", 
        "addition": []
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        data = response.json()
        
        if data.get('error') == 0:
            info = data['data']['voucher_basic_info']
            
            # ✅ Dùng display_name thay vì code
            display_name = voucher.get('display_name', voucher['code'])
            
            # Build message
            msg = f"🎫 <b>{display_name}</b>\n"

            # Discount info
            if info.get('discount_percentage') and info.get('discount_percentage') > 0:
                cap = info.get('coin_cap') or info.get('discount_cap') or 0
                formatted_cap = format_currency_check(cap)
                msg += f"💰 Giảm: {info['discount_percentage']}% (Tối đa {formatted_cap})\n"
            elif info.get('discount_value'):
                val = info['discount_value']
                if val > 100000000: 
                    val = val / 100000
                msg += f"💰 Giảm: {format_currency_check(val)}\n"
            else:
                msg += "💰 Giảm: Freeship/Quà tặng\n"

            # Min spend
            min_spend = format_currency_check(info.get('min_spend', 0))
            msg += f"🛒 Đơn tối thiểu: {min_spend}\n"

            # Usage percentage
            used = info['percentage_used']
            if used >= 90:
                msg += f"📊 Đã dùng: {used}% 🔴\n"
            else:
                msg += f"📊 Đã dùng: {used}% 🟢\n"

            # Claimed percentage
            claimed = info.get('percentage_claimed', 0)
            if claimed >= 100:
                msg += "📥 Lượt lưu: Đã hết lượt ⛔\n"
            else:
                msg += "📥 Lượt lưu: Còn lượt ✅\n"

            # End time
            end_time = datetime.fromtimestamp(info['end_time']).strftime('%H:%M:%S %d/%m/%Y')
            msg += f"⏰ Hạn: {end_time}\n"
            msg += "─" * 30  # ✅ THÊM DẤU NGĂN CÁCH

            return (True, msg)
        else:
            err_code = data.get('error')
            display_name = voucher.get('display_name', voucher['code'])
            msg = f"❌ {display_name}: Lỗi API ({err_code})\n" + "─" * 30
            return (False, msg)

    except Exception as e:
        display_name = voucher.get('display_name', voucher['code'])
        msg = f"❌ {display_name}: Lỗi kết nối ({str(e)[:30]})\n" + "─" * 30
        return (False, msg)


def get_vouchers_from_stock():
    """
    Lấy danh sách voucher từ VoucherStock sheet
    Trả về: list of dict {"code": str, "promotionid": int, "signature": str, "display_name": str}
    """
    if not SHEET_READY or ws_voucher is None:
        return []
    
    try:
        rows = ws_voucher.get_all_records()
    except Exception as e:
        dprint(f"get_vouchers_from_stock error: {e}")
        return []

    voucher_list = []
    seen_codes = set()  # ✅ Chống duplicate
    
    for row in rows:
        # Flexible column mapping
        def _get(*keys):
            for k in keys:
                for rk in row:
                    if str(rk).strip().lower() == k.lower():
                        v = row[rk]
                        return str(v).strip() if v is not None else ""
            return ""

        code = _get("code", "code_name", "voucher_code")
        promo_id = _get("promotion_id", "promotionid")
        sig = _get("signature", "chữ ký", "chu ky")
        display_name = _get("display_name", "display name", "ten_ma", "tên mã", "displayname")

        if code and promo_id:
            # ✅ Chống duplicate: Chỉ thêm nếu code chưa có
            if code not in seen_codes:
                try:
                    voucher_list.append({
                        "code": code,
                        "promotionid": int(promo_id),
                        "signature": sig,
                        "display_name": display_name or code  # Fallback về code nếu không có display_name
                    })
                    seen_codes.add(code)
                except ValueError:
                    continue

    return voucher_list


def handle_check_voucher(user_id, username):
    """
    Xử lý khi user nhấn nút Check Voucher
    """
    # 1. Lấy cookie từ tab Cookie
    cookie = get_cookie_from_sheet()

    if not cookie:
        tg_send(
            user_id,
            "❌ Không tìm thấy Cookie trong hệ thống!\n\n"
            "Vui lòng liên hệ Admin để thêm Cookie vào tab Cookie.",
            build_main_keyboard()
        )
        return

    # 2. Lấy danh sách voucher từ VoucherStock
    tg_send(user_id, f"📊 Đang tải danh sách voucher...")
    vouchers = get_vouchers_from_stock()

    if not vouchers:
        tg_send(
            user_id,
            "❌ Không tìm thấy voucher nào trong VoucherStock!",
            build_main_keyboard()
        )
        return

    # 3. Check từng voucher (không gửi từng batch)
    results = []
    
    for voucher in vouchers:
        success, msg = check_one_voucher(voucher, cookie)
        results.append(msg)
        time.sleep(0.3)  # Tránh spam API

    # 4. Gửi tất cả kết quả trong 1 message duy nhất
    final_message = "\n\n".join(results)
    
    # Split nếu quá dài (Telegram limit 4096 chars)
    if len(final_message) > 4000:
        # Chia thành nhiều message nếu quá dài
        chunks = []
        current_chunk = []
        current_length = 0
        
        for result in results:
            result_length = len(result) + 2  # +2 cho \n\n
            if current_length + result_length > 4000:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = [result]
                current_length = result_length
            else:
                current_chunk.append(result)
                current_length += result_length
        
        if current_chunk:
            chunks.append("\n\n".join(current_chunk))
        
        # Gửi từng chunk
        for chunk in chunks:
            tg_send(user_id, chunk)
            time.sleep(0.5)
    else:
        # Gửi 1 message duy nhất
        tg_send(user_id, final_message, build_main_keyboard())

    # 5. Log (không hiển thị tổng kết cho user)
    success_count = sum(1 for r in results if not r.startswith("❌"))
    fail_count = len(results) - success_count
    
    if SHEET_READY and ws_log:
        try:
            ws_log.append_row([
                datetime.now(VIETNAM_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                str(user_id),
                username,
                "CHECK_VOUCHER",
                "0",
                f"Checked {len(vouchers)} vouchers: {success_count} OK, {fail_count} fail"
            ])
        except Exception as e:
            dprint(f"Log error: {e}")


def get_tool_pc_link():
    """
    Lấy link Tool PC từ cột 'toolpc' trong VoucherStock
    Trả về link hoặc None nếu không tìm thấy
    """
    if not SHEET_READY or ws_voucher is None:
        return None
    
    try:
        rows = ws_voucher.get_all_records()
        
        # Tìm link trong cột 'toolpc'
        for row in rows:
            # Flexible column mapping
            for key in row:
                if str(key).strip().lower() == "toolpc":
                    link = str(row[key]).strip()
                    if link and (link.startswith("http://") or link.startswith("https://")):
                        dprint(f"✅ Tìm thấy link Tool PC: {link}")
                        return link
        
        dprint("⚠️ Không tìm thấy link Tool PC trong cột 'toolpc'")
        return None
        
    except Exception as e:
        dprint(f"get_tool_pc_link error: {e}")
        return None

# =========================================================
# 🔥 QR LOGIN FUNCTIONS
# =========================================================
def create_qr_session(user_id):
    """Tạo QR session mới"""
    url = f"{QR_API_BASE}/api/qr/create"
    payload = {"user_id": user_id}
    
    dprint(f"[QR CREATE] URL: {url}")
    dprint(f"[QR CREATE] Payload: {payload}")
    
    try:
        response = requests.post(
            url,
            json=payload,
            timeout=10
        )
        
        dprint(f"[QR CREATE] Status code: {response.status_code}")
        dprint(f"[QR CREATE] Response text: {response.text[:200]}")

        if response.status_code != 200:
            return False, f"API error: {response.status_code}", ""

        data = response.json()
        dprint(f"[QR CREATE] Response JSON: {data}")

        if not data.get("success"):
            error_msg = data.get("error", "Unknown error")
            return False, f"Create QR failed: {error_msg}", ""

        session_id = data.get("session_id")
        qr_image = data.get("qr_image", "").replace("data:image/png;base64,", "")
        
        dprint(f"[QR CREATE] Session ID: {session_id}")
        dprint(f"[QR CREATE] QR image length: {len(qr_image)}")

        # Lưu session
        with qr_lock:
            qr_sessions[session_id] = {
                "user_id": user_id,
                "created": time.time(),
                "status": "waiting",
                "qr_image": qr_image,
                "cookie": "",
                "cancelled": False  # ← Thêm cancelled flag
            }

        return True, session_id, qr_image

    except Exception as e:
        dprint(f"[QR CREATE] Exception: {e}")
        dprint(f"[QR CREATE] Traceback: {traceback.format_exc()}")
        return False, f"Error: {str(e)}", ""

def check_qr_status(session_id):
    """Check QR status"""
    try:
        response = requests.get(
            f"{QR_API_BASE}/api/qr/status/{session_id}",
            timeout=10
        )

        if response.status_code != 200:
            return False, f"API error: {response.status_code}", False, None, None

        data = response.json()

        if not data.get("success"):
            return False, data.get("error", "Check failed"), False, None, None

        status = (data.get("status") or "").upper()
        has_token = data.get("has_token", False)
        cookie_st = data.get("cookie_st", "")
        cookie_f = data.get("cookie_f", "")

        return True, status, has_token, cookie_st, cookie_f

    except Exception as e:
        return False, f"Error: {str(e)}", False, None, None


def inline_qr_keyboard(session_id):
    """Inline keyboard - CHỈ NÚT HỦY (Bot tự động lấy cookie)"""
    return {
        "inline_keyboard": [
            [
                {"text": "❌ Hủy", "callback_data": f"qr_cancel:{session_id}"}
            ]
        ]
    }

def build_quick_save_keyboard():
    """
    Keyboard lưu voucher nhanh sau khi lấy cookie
    ✅ TỰ ĐỘNG LẤY TỪ SHEET (như keyboard chính)
    """
    if not SHEET_READY:
        # Fallback: Keyboard tĩnh nếu sheet lỗi
        return {
            "inline_keyboard": [
                [
                    {"text": "⭐ Mã 100k 0đ", "callback_data": "QUICK_SAVE:ma100k0d"},
                    {"text": "⭐ Mã 50% Max 100k", "callback_data": "QUICK_SAVE:ma50max100k"}
                ],
                [{"text": "🔙 Về menu chính", "callback_data": "QUICK_SAVE:back"}]
            ]
        }
    
    try:
        # Đọc từ VoucherStock sheet
        all_rows = ws_voucher.get_all_records()
        
        buttons = []
        button_row = []
        
        for row in all_rows:
            # Check Display
            display = ""
            for key in ["Display", "Show", "Visible", "Hiển thị", "Hiển Thị"]:
                if key in row:
                    display = str(row[key]).strip().upper()
                    if display:
                        break
            
            if display not in ["YES", "Y", "TRUE", "1"]:
                continue
            
            # Check Trạng Thái
            trang_thai = str(row.get("Trạng Thái", "")).strip()
            if trang_thai != "Còn Mã":
                continue
            
            # Lấy thông tin voucher
            ten_hien_thi = ""
            for key in ["Display Name", "Tên hiển thị", "Tên Hiển Thị", "display_name"]:
                if key in row:
                    ten_hien_thi = str(row[key]).strip()
                    if ten_hien_thi:
                        break
            
            if not ten_hien_thi:
                ten_hien_thi = str(row.get("Tên Mã", "")).strip()
            
            ten_ma = str(row.get("Tên Mã", "")).strip()
            
            if not ten_ma:
                continue
            
            # Tạo callback_data từ tên mã (normalize)
            callback_key = normalize_voucher_key(ten_ma)
            
            # Thêm vào button
            button_row.append({
                "text": f"⭐ {ten_hien_thi}",
                "callback_data": f"QUICK_SAVE:{callback_key}"
            })
            
            # 2 button mỗi row
            if len(button_row) == 2:
                buttons.append(button_row)
                button_row = []
        
        # Thêm row cuối nếu có button lẻ
        if button_row:
            buttons.append(button_row)
        
        # Nếu không có voucher nào
        if not buttons:
            buttons.append([
                {"text": "⚠️ Chưa có voucher", "callback_data": "QUICK_SAVE:back"}
            ])
        
        # Thêm nút Về menu
        buttons.append([{"text": "🔙 Về menu chính", "callback_data": "QUICK_SAVE:back"}])
        
        dprint(f"[QUICK_SAVE] Built keyboard with {len(buttons)-1} rows")
        
        return {"inline_keyboard": buttons}
        
    except Exception as e:
        dprint(f"[ERROR] build_quick_save_keyboard: {e}")
        # Fallback
        return {
            "inline_keyboard": [
                [
                    {"text": "⭐ Mã 100k 0đ", "callback_data": "QUICK_SAVE:ma100k0d"},
                    {"text": "⭐ Mã 50% Max 100k", "callback_data": "QUICK_SAVE:ma50max100k"}
                ],
                [{"text": "🔙 Về menu chính", "callback_data": "QUICK_SAVE:back"}]
            ]
        }

def track_qr_failure(user_id, username, chat_id):
    """
    Track QR failures và ban user nếu spam
    Returns: True nếu user bị ban, False nếu OK
    """
    with qr_failures_lock:
        now = time.time()
        
        if user_id not in qr_failures:
            qr_failures[user_id] = {"count": 1, "last_fail": now}
            return False
        
        # Reset nếu lần fail cuối cách xa hơn 5 phút
        if now - qr_failures[user_id]["last_fail"] > 300:
            qr_failures[user_id] = {"count": 1, "last_fail": now}
            return False
        
        # Tăng count
        qr_failures[user_id]["count"] += 1
        qr_failures[user_id]["last_fail"] = now
        
        fail_count = qr_failures[user_id]["count"]
        
        # Ban vĩnh viễn nếu >= 5 lần
        if fail_count >= MAX_QR_FAILURES:
            # Ban user trong PostgreSQL
            try:
                pg_exec("UPDATE wallet SET status='BANNED_QR_SPAM', updated_at=NOW() WHERE tele_id=%s", (int(user_id),))

                # Mirror Sheet (fire-and-forget)
                if SHEET_READY:
                    try:
                        row = get_user_row(user_id)
                        if row:
                            ws_money.update(f'D{row}', [["BANNED_QR_SPAM"]])
                    except Exception:
                        pass

                # Thông báo admin
                admin_msg = (
                    f"🚨 <b>BAN VĨNH VIỄN - QR SPAM</b>\n\n"
                    f"👤 <b>User ID:</b> <code>{user_id}</code>\n"
                    f"📝 <b>Username:</b> @{username or 'N/A'}\n"
                    f"🔢 <b>Số lần thất bại:</b> {fail_count}\n"
                    f"⏰ <b>Thời gian:</b> {now_str()}\n\n"
                    f"⚠️ <b>Lý do:</b> Get QR thất bại {fail_count} lần liên tục"
                )
                tg_send(ADMIN_ID, admin_msg)

                dprint(f"🚨 BANNED USER {user_id} for QR spam ({fail_count} failures)")
            except Exception as e:
                dprint(f"Error banning user {user_id}: {e}")
            
            return True
        
        return False

def save_user_cookie(user_id, cookie):
    """Lưu cookie của user để dùng cho voucher nhanh"""
    with user_cookies_lock:
        user_last_cookies[user_id] = {
            "cookie": cookie,
            "timestamp": time.time()
        }

def get_user_cookie(user_id):
    """Lấy cookie đã lưu của user (trong vòng 1 giờ)"""
    with user_cookies_lock:
        if user_id not in user_last_cookies:
            return None
        
        cookie_data = user_last_cookies[user_id]
        
        # Cookie hết hạn sau 1 giờ
        if time.time() - cookie_data["timestamp"] > 3600:
            del user_last_cookies[user_id]
            return None
        
        return cookie_data["cookie"]
def handle_get_cookie_qr(chat_id, user_id, username):
    """
    Xử lý lệnh Get Cookie QR
    ✅ TỰ ĐỘNG WATCH - Không cần bấm nút
    """
    # Check user tồn tại
    exists, balance, status = get_user_data(user_id)
    if not exists:
        send_message(chat_id, "❌ Vui lòng /start trước khi dùng chức năng này")
        return

    # Message đang tạo QR
    send_message(chat_id, "🔄 <b>Đang tạo mã QR đăng nhập Shopee...</b>")

    # Tạo QR session
    success, result, qr_image = create_qr_session(user_id)

    if not success:
        send_message(chat_id, f"❌ <b>Lỗi tạo QR:</b>\n{result}", build_main_keyboard())
        return

    session_id = result

    # Caption hướng dẫn
    caption = (
        "🔑 <b>QR LOGIN SHOPEE</b>\n\n"
        "📍 <b>Hướng dẫn:</b>\n"
        "1️⃣ <b>Mở app Shopee</b>\n"
        "2️⃣ <b>Trang Chủ → Góc trên trái → Ô Vuông (Scanner)</b>\n"
        "3️⃣ <b>Quét mã QR bên dưới</b>\n"
        "4️⃣ <b>Chờ bot tự động lấy cookie</b> (không cần bấm gì)\n\n"
        "⏰ Mã QR có hiệu lực trong <b>5 phút</b>\n"
        "🤖 <i>Bot sẽ tự động gửi cookie sau khi bạn quét xong</i>"
    )

    # ✅ GỬI QR VỚI NÚT HỦY (không có nút Lấy Cookie)
    cancel_keyboard = {
        "inline_keyboard": [
            [{"text": "❌ Hủy", "callback_data": f"qr_cancel:{session_id}"}]
        ]
    }

    # Gửi QR
    try:
        send_photo(chat_id, qr_image, caption=caption, reply_markup=cancel_keyboard)
    except Exception as e:
        dprint(f"[QR] Send photo error: {e}")
        send_message(chat_id, f"{caption}\n\n❌ <b>Không thể tạo ảnh QR, vui lòng thử lại sau.</b>")
        return

    # Lưu thông tin session
    with qr_lock:
        if session_id in qr_sessions:
            qr_sessions[session_id]["chat_id"] = chat_id
            qr_sessions[session_id]["username"] = username

    # ✅ START AUTO-WATCH THREAD
    watch_thread = threading.Thread(
        target=auto_watch_qr_and_send_cookie,
        args=(session_id, chat_id, user_id, username),
        daemon=True
    )
    watch_thread.start()
    
    dprint(f"[QR] Auto-watch started for session {session_id}")

# =========================================================
# 2. AUTO WATCH QR - TỰ ĐỘNG CHECK VÀ GỬI COOKIE
# =========================================================
def auto_watch_qr_and_send_cookie(session_id, chat_id, user_id, username):
    """
    ✅ TỰ ĐỘNG theo dõi QR và gửi cookie khi quét xong
    ✅ Không cần user bấm nút
    """
    dprint(f"[QR AUTO] Started watching session {session_id} for user {user_id}")
    
    # Delay 2s để user kịp thấy QR
    time.sleep(2)

    start_time = time.time()
    check_count = 0
    last_status = None
    
    while time.time() - start_time < QR_TIMEOUT:
        check_count += 1
        
        # ✅ CHECK CANCELLED
        with qr_lock:
            if session_id not in qr_sessions:
                dprint(f"[QR AUTO] Session {session_id} not found, stopping")
                return
            
            session = qr_sessions.get(session_id, {})
            if session.get("cancelled"):
                dprint(f"[QR AUTO] Session {session_id} cancelled by user")
                return
        
        # ✅ CHECK STATUS
        success, status, has_token, cookie_st, cookie_f = check_qr_status(session_id)

        if not success:
            time.sleep(QR_POLL_INTERVAL)
            continue

        # ✅ LOG STATUS CHANGE
        if status != last_status:
            dprint(f"[QR AUTO] Check #{check_count} - Status: {status}, has_token: {has_token}")
            last_status = status

        # ✅ QUÉT XONG - LẤY COOKIE NGAY
        if has_token:
            dprint(f"[QR AUTO] QR confirmed! Getting full cookie...")
            
            # Gửi message "Đang lấy cookie..."
            send_message(chat_id, "⏳ <b>Đang lấy cookie...</b>")
            
            # ✅ LẤY COOKIE - TRẢ 6 GIÁ TRỊ
            success_login, full_cookie, spc_st, spc_f, username, phone = get_qr_cookie(session_id)

            if success_login:
                dprint(f"[QR AUTO] Cookie retrieved successfully")
                
                # ✅ TRỪ 100Đ KHI GET QR THÀNH CÔNG
                QR_FEE = 100  # Phí Get QR
                success_deduct, new_balance = deduct_balance_atomic(user_id, QR_FEE)
                
                if not success_deduct:
                    # Không đủ tiền
                    send_message(
                        chat_id,
                        f"❌ <b>KHÔNG ĐỦ SỐ DƯ</b>\n\n"
                        f"💰 Cần: <b>{QR_FEE:,}đ</b>\n"
                        f"💼 Số dư: <b>{new_balance:,}đ</b>\n\n"
                        f"⚠️ Vui lòng nạp tiền để sử dụng tính năng Get QR",
                        reply_markup=build_main_keyboard()
                    )
                    
                    # Xóa session
                    with qr_lock:
                        if session_id in qr_sessions:
                            del qr_sessions[session_id]
                    
                    dprint(f"[QR AUTO] Insufficient balance for user {user_id}")
                    return
                
                # Ghi log trừ tiền
                log_row(user_id, username, "GET_QR", f"-{QR_FEE}", f"Phí Get Cookie QR | Balance: {new_balance:,}đ")
                dprint(f"[QR AUTO] Deducted {QR_FEE}đ from user {user_id}, new balance: {new_balance:,}đ")
                
                # Lưu cookie cho voucher nhanh
                save_user_cookie(user_id, full_cookie)

                # Tính ngày hết hạn
                expiry_date = now_datetime() + timedelta(days=COOKIE_VALIDITY_DAYS)

                # ✅ GỬI COOKIE - CHỈ HIỂN THỊ ST VÀ F RIÊNG 2 DÒNG
                msg = "🎉 <b>LẤY COOKIE THÀNH CÔNG!</b>\n\n"
                msg += f"💸 <b>Đã trừ:</b> {QR_FEE:,}đ\n"
                msg += f"💼 <b>Số dư:</b> {new_balance:,}đ\n\n"
                
                # Cookie ST
                if spc_st:
                    msg += f"🍪 <b>Cookie ST:</b>\n<code>SPC_ST={spc_st}</code>\n\n"
                else:
                    msg += f"⚠️ <b>Cookie ST:</b> Không tìm thấy\n\n"
                
                # Cookie F (format: SPC_F | username | SDT)
                if spc_f:
                    cookie_f_formatted = spc_f
                    if username:
                        cookie_f_formatted += f" | {username}"
                    if phone:
                        cookie_f_formatted += f" | {phone}"
                    
                    msg += f"🔐 <b>Cookie F:</b>\n<code>SPC_F={cookie_f_formatted}</code>\n\n"
                else:
                    msg += f"⚠️ <b>Cookie F:</b> Không tìm thấy\n\n"
                
                # Thông tin thêm
                msg += f"💡 <i>Tap vào cookie để auto copy</i>\n\n"
                msg += f"⏰ <b>Hiệu lực:</b> {COOKIE_VALIDITY_DAYS} ngày (đến {expiry_date.strftime('%d/%m/%Y')})\n"
                msg += f"⚠️ <b>Bảo mật tuyệt đối!</b>"
                
                send_message(chat_id, msg)
                
                # Gửi keyboard voucher nhanh
                time.sleep(0.5)
                send_message(
                    chat_id,
                    "⚡ <b>LƯU VOUCHER NHANH</b>\n\n"
                    "👇 Chọn voucher muốn lưu:",
                    reply_markup=build_quick_save_keyboard()
                )
                
                # Reset failure count
                with qr_failures_lock:
                    if user_id in qr_failures:
                        del qr_failures[user_id]
                
                # Xóa session
                with qr_lock:
                    if session_id in qr_sessions:
                        del qr_sessions[session_id]
                
                dprint(f"[QR AUTO] Success! Session {session_id} completed")
                return
            else:
                dprint(f"[QR AUTO] Failed to get cookie: {full_cookie}")
                send_message(
                    chat_id, 
                    f"❌ <b>Lỗi lấy cookie</b>\n\n{full_cookie}",
                    build_main_keyboard()
                )
                return

        time.sleep(QR_POLL_INTERVAL)

    # ✅ TIMEOUT
    dprint(f"[QR AUTO] Timeout for session {session_id}")
    
    # Track failure
    is_banned = track_qr_failure(user_id, username, chat_id)
    
    if is_banned:
        send_message(
            chat_id,
            "🚫 <b>TÀI KHOẢN BỊ KHÓA VĨNH VIỄN</b>\n\n"
            "⚠️ <b>Lý do:</b> Get QR thất bại quá nhiều lần\n\n"
            "📞 <b>Liên hệ Admin:</b> @BonBonxHPx"
        )
    else:
        fail_count = qr_failures.get(user_id, {}).get("count", 0)
        warning = ""
        if fail_count >= 3:
            warning = f"\n\n⚠️ <b>Cảnh báo:</b> {fail_count}/{MAX_QR_FAILURES} lần"
        
        send_message(
            chat_id,
            f"⏰ <b>HẾT THỜI GIAN</b>\n\n"
            f"Mã QR đã hết hạn (5 phút)\n"
            f"Vui lòng Get Cookie QR lại{warning}",
            reply_markup=build_main_keyboard()
        )
    
    # Xóa session
    with qr_lock:
        if session_id in qr_sessions:
            del qr_sessions[session_id]


# =========================================================
# 3. GET QR COOKIE - TRẢ FULL COOKIES (Bao gồm SPC_F)
# =========================================================
def get_qr_cookie(session_id):
    """
    Lấy cookie sau khi quét QR
    Returns: (success: bool, full_cookie: str, spc_st: str, spc_f: str, username: str, phone: str)
    
    ✅ Trả RIÊNG: full_cookie, SPC_ST, SPC_F, username, phone
    ✅ Priority: cookie_string → cookie → build from dict
    """
    dprint(f"[QR COOKIE] Getting cookie for session {session_id}")
    
    try:
        url = f"{QR_API_BASE}/api/qr/login/{session_id}"
        dprint(f"[QR COOKIE] URL: {url}")
        
        response = requests.post(url, timeout=10)
        
        dprint(f"[QR COOKIE] Status: {response.status_code}")

        if response.status_code != 200:
            dprint(f"[QR COOKIE] Error: HTTP {response.status_code}")
            return False, f"API error: {response.status_code}", "", "", "", ""

        data = response.json()
        dprint(f"[QR COOKIE] Response keys: {list(data.keys())}")

        if not data.get("success"):
            error_msg = data.get("error", "Login failed")
            dprint(f"[QR COOKIE] API error: {error_msg}")
            return False, f"Login failed: {error_msg}", "", "", "", ""

        # ✅ PRIORITY 1: cookie_string (full cookies)
        full_cookie = data.get("cookie_string", "")
        
        # ✅ PRIORITY 2: cookie
        if not full_cookie:
            full_cookie = data.get("cookie", "")
            dprint(f"[QR COOKIE] Using 'cookie' field")
        
        # ✅ PRIORITY 3: Build from cookies dict
        if not full_cookie and data.get("cookies"):
            try:
                cookies_dict = data.get("cookies", {})
                full_cookie = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
                dprint(f"[QR COOKIE] Built from dict: {len(cookies_dict)} cookies")
            except Exception as e:
                dprint(f"[QR COOKIE] Error building from dict: {e}")
        
        if not full_cookie:
            dprint(f"[QR COOKIE] No cookie in response")
            return False, "No cookie returned", "", "", "", ""
        
        # ✅ ENSURE SPC_F - Thêm SPC_F nếu chưa có
        if "SPC_F=" not in full_cookie:
            default_spc_f = "YPByHuJJks2b7GpDwIdZp6ONQwyaN4yv"
            full_cookie = f"{full_cookie}; SPC_F={default_spc_f}"
            dprint(f"[QR COOKIE] Added default SPC_F")
        
        # ✅ EXTRACT SPC_ST
        spc_st = ""
        match_st = re.search(r'SPC_ST=([^;]+)', full_cookie)
        if match_st:
            spc_st = match_st.group(1)
            dprint(f"[QR COOKIE] Extracted SPC_ST: {spc_st[:30]}...")
        
        # ✅ EXTRACT SPC_F
        spc_f = ""
        match_f = re.search(r'SPC_F=([^;]+)', full_cookie)
        if match_f:
            spc_f = match_f.group(1)
            dprint(f"[QR COOKIE] Extracted SPC_F: {spc_f}")
        
        # ✅ EXTRACT USERNAME VÀ PHONE từ API response
        username = data.get("username", "")
        phone = data.get("phone", "")
        
        # Fallback: nếu không có trong response, cố gắng decode từ cookie
        if not username or not phone:
            try:
                # Có thể có thông tin trong cookies dict
                cookies_dict = data.get("cookies", {})
                if not username:
                    username = cookies_dict.get("username", "")
                if not phone:
                    phone = cookies_dict.get("phone", "")
            except Exception:
                pass
        
        dprint(f"[QR COOKIE] Username: {username}, Phone: {phone}")
        
        # Stats
        cookie_count = full_cookie.count(";") + 1
        dprint(f"[QR COOKIE] Success: {len(full_cookie)} chars, {cookie_count} cookies")
        dprint(f"[QR COOKIE] SPC_ST: {len(spc_st)} chars, SPC_F: {len(spc_f)} chars")
        
        return True, full_cookie, spc_st, spc_f, username, phone

    except Exception as e:
        dprint(f"[QR COOKIE] Exception: {e}")
        import traceback
        dprint(f"[QR COOKIE] Traceback: {traceback.format_exc()}")
        return False, f"Error: {str(e)}", "", "", "", ""


# =========================================================
# 4. HANDLE QR CANCEL - GIỮ NGUYÊN
# =========================================================
def handle_qr_cancel(chat_id, session_id):
    """Xử lý callback hủy QR"""
    dprint(f"[QR CANCEL] User cancelled session {session_id}")
    
    with qr_lock:
        if session_id in qr_sessions:
            qr_sessions[session_id]["cancelled"] = True
            dprint(f"[QR CANCEL] Marked session {session_id} as cancelled")

    send_message(
        chat_id,
        "❌ <b>ĐÃ HỦY</b>\n\nBấm <b>🔑 Get Cookie QR</b> để tạo mã mới",
        reply_markup=build_main_keyboard()
    )



# =========================================================
# UTIL
# =========================================================
def now_str():
    return datetime.now(VIETNAM_TZ).strftime("%Y-%m-%d %H:%M:%S")

def now_datetime():
    return datetime.now(VIETNAM_TZ)

def get_all_user_ids():
    """
    🎯 V7 BROADCAST FIX:
    - Ưu tiên lấy TẤT CẢ user từ PostgreSQL (nguồn chính)
    - Cache kết quả 5 phút để giảm DB load
    - Fallback: Sheet (nếu PG fail) hoặc cache cũ
    
    ✅ FIX: Trước đây chỉ dùng USER_ROW_CACHE + Sheet
    → Bỏ sót user mới chưa chat với bot!
    """
    global BROADCAST_USER_CACHE, BROADCAST_USER_CACHE_TIME

    # ✅ CHECK CACHE TRƯỚC (TTL 5 phút)
    now = time.time()
    if (BROADCAST_USER_CACHE and
        now - BROADCAST_USER_CACHE_TIME < BROADCAST_USER_CACHE_TTL):
        dprint(f"✅ BROADCAST CACHE HIT: {len(BROADCAST_USER_CACHE)} users")
        return BROADCAST_USER_CACHE

    # ❌ Cache miss - cần fetch mới
    dprint("⚠️ BROADCAST CACHE MISS - Fetching from database...")

    try:
        # 🎯 PRIORITY 1: LẤY TỪ POSTGRESQL (NGUỒN CHÍNH)
        if PG_POOL is not None:
            rows = pg_exec(
                "SELECT tele_id FROM wallet WHERE status NOT IN ('banned', 'banned_qr_spam')",
                fetchall=True
            )
            if rows:
                user_ids = [int(r[0]) for r in rows]
                BROADCAST_USER_CACHE = user_ids
                BROADCAST_USER_CACHE_TIME = now
                dprint(f"✅ Loaded {len(user_ids)} users from PostgreSQL")
                return user_ids
            else:
                dprint("⚠️ PostgreSQL query returned 0 users")
        else:
            dprint("⚠️ PG_POOL is None, falling back to Sheet")

        # 🔄 FALLBACK 1: Dùng USER_ROW_CACHE nếu không có Sheet
        if not SHEET_READY:
            dprint("❌ Sheet not ready, using ROW_CACHE only")
            cached_users = list(USER_ROW_CACHE.keys())
            if len(cached_users) > 0:
                BROADCAST_USER_CACHE = cached_users
                BROADCAST_USER_CACHE_TIME = now
                return cached_users
            return []

        # 🔄 FALLBACK 2: Đọc từ Google Sheet
        dprint("⚠️ Reading all users from Sheet (fallback)...")
        all_values = ws_money.get_all_values()
        user_ids = set()
        for row in all_values[1:]:  # Skip header
            if row and row[0]:
                try:
                    user_id = int(row[0])
                    # Lọc status ban (cột 4)
                    status = row[3].strip().lower() if len(row) > 3 else ""
                    if status not in ("banned", "banned_qr_spam"):
                        user_ids.add(user_id)
                except:
                    continue

        result = list(user_ids)
        BROADCAST_USER_CACHE = result
        BROADCAST_USER_CACHE_TIME = now

        dprint(f"📊 Loaded {len(result)} users from Sheet")
        return result

    except Exception as e:
        dprint(f"❌ get_all_user_ids error: {e}")
        # 🔄 FALLBACK 3: Dùng cache cũ nếu có lỗi
        if BROADCAST_USER_CACHE:
            dprint(f"⚠️ Using stale cache ({len(BROADCAST_USER_CACHE)} users) due to error")
            return BROADCAST_USER_CACHE
        return []

def broadcast_message(message, exclude_admin=False):
    user_ids = get_all_user_ids()

    if not user_ids:
        dprint("❌ No users found for broadcast")
        return 0, 0

    dprint(f"📢 Starting broadcast to {len(user_ids)} users...")

    success = 0
    failed = 0
    sent_to = set()

    for user_id in user_ids:
        if user_id in sent_to:
            dprint(f"⚠️ Skipping duplicate user_id: {user_id}")
            continue

        if exclude_admin and user_id == ADMIN_ID:
            continue

        try:
            broadcast_text = f"📢 <b>THÔNG BÁO TỪ BOT</b>\n\n{message}"
            tg_send(user_id, broadcast_text)
            sent_to.add(user_id)
            success += 1
            time.sleep(0.05)
        except Exception as e:
            dprint(f"❌ Broadcast failed for {user_id}:", e)
            failed += 1

    dprint(f"✅ Broadcast completed: {success} success, {failed} failed")
    return success, failed

# =========================================================
# SHEET-BASED STATE
# =========================================================
def get_broadcast_sheet():
    if not SHEET_READY:
        return None
    try:
        try:
            return sh.worksheet("BroadcastState")
        except:
            ws = sh.add_worksheet("BroadcastState", 100, 4)
            ws.update('A1:D1', [['Timestamp', 'AdminID', 'Status', 'MessageID']])
            return ws
    except Exception as e:
        dprint(f"get_broadcast_sheet error: {e}")
        return None

def get_last_broadcast_time_from_sheet():
    ws = get_broadcast_sheet()
    if not ws:
        return None
    try:
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return None

        for row in reversed(all_values[1:]):
            if row[2] in ["STARTED", "COMPLETED"]:
                timestamp_str = row[0]
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                return dt.replace(tzinfo=VIETNAM_TZ).timestamp()

        return None
    except Exception as e:
        dprint(f"get_last_broadcast_time_from_sheet error: {e}")
        return None

def set_broadcast_state_to_sheet(admin_id, status, message_id=""):
    ws = get_broadcast_sheet()
    if not ws:
        return False
    try:
        ws.append_row([
            now_str(),
            str(admin_id),
            status,
            str(message_id)
        ])
        dprint(f"📝 Broadcast state saved: {status}")
        return True
    except Exception as e:
        dprint(f"set_broadcast_state_to_sheet error: {e}")
        return False

def is_broadcast_message_processed(message_id):
    if not message_id:
        return False

    ws = get_broadcast_sheet()
    if not ws:
        return False

    try:
        col_message_ids = ws.col_values(4)
        return str(message_id) in col_message_ids
    except Exception as e:
        dprint("is_broadcast_message_processed error:", e)
        return False

def check_broadcast_cooldown_from_sheet():
    last_time = get_last_broadcast_time_from_sheet()
    if not last_time:
        return True, 0

    current_time = time.time()
    time_since_last = current_time - last_time

    dprint(f"⏱️ Time since last broadcast: {time_since_last:.1f}s")

    if time_since_last < BROADCAST_COOLDOWN:
        wait_time = int(BROADCAST_COOLDOWN - time_since_last)
        return False, wait_time

    return True, 0

def log_row(user_id, username, action, value="", note=""):
    if not SHEET_READY:
        return
    try:
        ws_log.append_row([now_str(), str(user_id), username, action, value, note])
    except Exception as e:
        dprint("log_row error:", e)

def log_voucher_save(user_id, username, voucher_name, num_cookies, price, balance_after, status):
    """
    Log voucher save action
    
    Args:
        user_id: Telegram user ID
        username: Telegram username
        voucher_name: Tên voucher
        num_cookies: Số lượng cookie
        price: Tổng tiền
        balance_after: Số dư sau khi lưu
        status: Trạng thái (✅ hoặc ❌ + lỗi)
    """
    if not SHEET_READY:
        return
    try:
        ws_log.append_row([
            now_str(),
            str(user_id),
            username,
            f"SAVE_VOUCHER",
            f"{voucher_name} x{num_cookies}",
            f"{status} | Price: {price:,}đ | Balance: {balance_after:,}đ"
        ])
    except Exception as e:
        dprint(f"log_voucher_save error: {e}")

# =========================================================
# ✅ ANTI-SPAM SYSTEM
# =========================================================
def track_error(user_id, username="", reason=""):
    """
    ✅ Anti-spam (Redis ưu tiên)
    - 5 request / 20s -> Ban 1H
    - Tái phạm -> Ban vĩnh viễn
    """
    if reason not in ("SPAM_CALLBACK", "SPAM_COMMAND", "SPAM_TEXT"):
        return False

    user_id = int(user_id)

    # ✅ Redis mode (bền + scale)
    if RDS is not None:
        try:
            key = f"spam:{reason}:{user_id}"
            cnt = int(RDS.incr(key))
            if cnt == 1:
                RDS.expire(key, SPAM_WINDOW)

            if cnt >= SPAM_THRESHOLD:
                # ban_count để nâng cấp từ 1H -> PERMANENT
                bkey = f"ban_count:{user_id}"
                ban_count = int(RDS.get(bkey) or 0)

                if ban_count == 0:
                    apply_ban(user_id, "1H")
                    notify_admin_spam(user_id, username, "1H", cnt)
                    RDS.setex(bkey, 60*60*24*30, 1)  # nhớ 30 ngày
                    return True
                else:
                    apply_ban(user_id, "PERMANENT")
                    notify_admin_spam(user_id, username, "PERMANENT", cnt)
                    RDS.setex(bkey, 60*60*24*365, 2)  # nhớ 1 năm
                    return True

            return False
        except Exception as e:
            dprint(f"Redis spam error -> fallback RAM: {e}")

    # ✅ Fallback RAM (như cũ)
    now = time.time()

    if user_id not in SPAM_TRACKER:
        SPAM_TRACKER[user_id] = {"errors": [], "ban_count": 0}

    tracker = SPAM_TRACKER[user_id]
    tracker["errors"].append(now)
    tracker["errors"] = [t for t in tracker["errors"] if now - t < SPAM_WINDOW]

    if len(tracker["errors"]) >= SPAM_THRESHOLD:
        ban_count = tracker["ban_count"]
        error_count = len(tracker["errors"])

        if ban_count == 0:
            apply_ban(user_id, "1H")
            notify_admin_spam(user_id, username, "1H", error_count)
            tracker["ban_count"] = 1
            return True
        else:
            apply_ban(user_id, "PERMANENT")
            notify_admin_spam(user_id, username, "PERMANENT", error_count)
            return True

    return False

def check_ban_status(user_id):
    """
    ✅ V7: Đọc ban status từ cột 'status' trong PostgreSQL.
    - status = 'banned'     → Ban vĩnh viễn
    - status = 'ban_1h'     → Ban 1h, check thời gian từ notes
    - status = 'BANNED_QR_SPAM' → Ban QR spam (permanent)
    """
    user_id = int(user_id)

    if PG_POOL is None:
        return {"banned": False}

    r = pg_exec("SELECT status, notes FROM wallet WHERE tele_id=%s", (user_id,), fetchone=True)
    if not r:
        return {"banned": False}

    status = (r[0] or "").strip().lower()
    notes  = (r[1] or "").strip()

    try:
        # Ban vĩnh viễn
        if status in ("banned", "banned_qr_spam"):
            return {"banned": True, "type": "PERMANENT", "until": "Vĩnh viễn"}

        # Ban 1 giờ — thời gian lưu trong notes
        if status == "ban_1h":
            try:
                ban_until_str = notes.split("BAN 1H:")[1].strip() if "BAN 1H:" in notes else ""
                if ban_until_str:
                    ban_until = datetime.strptime(ban_until_str, "%Y-%m-%d %H:%M")
                    if now_datetime() < ban_until:
                        return {"banned": True, "type": "1H", "until": ban_until_str}
                    else:
                        # hết hạn → reset status + notes
                        pg_exec("UPDATE wallet SET status='active', notes='auto từ bot', updated_at=NOW() WHERE tele_id=%s", (user_id,))
                        # mirror sheet (fire-and-forget)
                        if SHEET_READY:
                            try:
                                row = get_user_row(user_id)
                                if row:
                                    ws_money.update_cell(row, 4, "active")
                                    ws_money.update_cell(row, 6, "auto từ bot")
                            except Exception:
                                pass
                        return {"banned": False}
                else:
                    # notes không có thời gian → treat as expired, reset
                    pg_exec("UPDATE wallet SET status='active', updated_at=NOW() WHERE tele_id=%s", (user_id,))
                    return {"banned": False}
            except Exception:
                return {"banned": False}

        return {"banned": False}

    except Exception as e:
        dprint("check_ban_status error:", e)
        return {"banned": False}

def notify_admin_spam(user_id, username, ban_type, error_count):
    if not ADMIN_ID or ADMIN_ID == 0:
        return

    try:
        exists, balance, status = get_user_data(user_id)

        if ban_type == "PERMANENT":
            ban_text = "🔨 Hành động: Ban vĩnh viễn"
            time_text = "⏰ Thời gian: Vĩnh viễn"
        else:
            ban_until = now_datetime() + timedelta(seconds=BAN_DURATION_1H)
            ban_text = "🔨 Hành động: Ban 1 giờ"
            time_text = f"⏰ Hết hạn: {ban_until.strftime('%Y-%m-%d %H:%M')}"

        if username:
            user_info = f"@{username}"
        else:
            user_info = f"ID: {user_id}"

        msg = (
            "🚨 <b>CẢNH BÁO SPAM</b>\n\n"
            f"👤 User: {user_info}\n"
            f"📱 Tele ID: <code>{user_id}</code>\n"
            f"⚠️ Số lỗi: <b>{error_count} lỗi trong 60 giây</b>\n\n"
            f"{ban_text}\n"
            f"{time_text}\n\n"
            "━━━━━━━━━━━━━━━\n"
            "📊 <b>Chi tiết:</b>\n"
            f"• Balance: {balance:,}đ\n"
            f"• Status: {status}\n\n"
            f"🔗 <a href='tg://user?id={user_id}'>Link user</a>"
        )

        tg_send(ADMIN_ID, msg)
        dprint(f"✅ Sent spam alert to admin: {user_id}")

    except Exception as e:
        dprint("notify_admin_spam error:", e)

def apply_ban(user_id, ban_type):
    """
    ✅ V7: Apply ban → ghi vào cột status.
    - PERMANENT: status = 'banned'
    - 1H:        status = 'ban_1h', notes = 'BAN 1H: <thời gian>'
    - Sheet mirror: fire-and-forget
    """
    user_id = int(user_id)
    ensure_user_exists(user_id, username="")

    try:
        if ban_type == "PERMANENT":
            new_status = "banned"
            note = "Ban vĩnh viễn: Spam"
        else:
            new_status = "ban_1h"
            ban_until = now_datetime() + timedelta(seconds=BAN_DURATION_1H)
            note = f"BAN 1H: {ban_until.strftime('%Y-%m-%d %H:%M')}"

        # ✅ update PG — status + notes
        if PG_POOL is not None:
            pg_exec("UPDATE wallet SET status=%s, notes=%s, updated_at=NOW() WHERE tele_id=%s",
                    (new_status, note, user_id))

        # ✅ mirror sheet (fire-and-forget)
        if SHEET_READY:
            try:
                row = get_user_row(user_id)
                if row:
                    ws_money.update_cell(row, 4, new_status)
                    ws_money.update_cell(row, 6, note)
            except Exception:
                pass

        log_row(user_id, "", "BAN_APPLIED", ban_type, f"status={new_status} | {note}")
        dprint(f"✅ Applied ban: {user_id} → {ban_type} (status={new_status})")

    except Exception as e:
        dprint("apply_ban error:", e)

def get_user_row(user_id):
    """
    ✅ V4: Cache-first, giảm 80% Sheet API calls
    """
    if not SHEET_READY:
        return None

    # ✅ CHECK CACHE TRƯỚC
    cached_row = get_cached_user_row(user_id)
    if cached_row:
        dprint(f"✅ ROW CACHE HIT: user {user_id} = row {cached_row}")
        return cached_row

    # ❌ Cache miss → gọi Sheet
    dprint(f"⚠️ ROW CACHE MISS: user {user_id}, calling Sheet...")
    try:
        ids = ws_money.col_values(1)
        row = ids.index(str(user_id)) + 1 if str(user_id) in ids else None

        # ✅ CACHE NGAY
        if row:
            cache_user_row(user_id, row)

        return row
    except Exception as e:
        import traceback
        dprint(f"❌ get_user_row FAILED for user {user_id}: {type(e).__name__}: {e}")
        dprint(f"   Traceback: {traceback.format_exc()}")
        return None

def ensure_user_exists(user_id, username=""):
    """
    ✅ V6 PG-PRIMARY:
    - PostgreSQL: tạo dòng wallet nếu chưa có (nguồn chính)
    - Google Sheet: mirror fire-and-forget (không block critical path)
    - ✅ User mới sẽ có status='new' và balance=0, cần kích hoạt để nhận 5100đ
    """
    user_id = int(user_id)

    if PG_POOL is None:
        return

    # 1) PG: INSERT mới với status='new', balance=0 hoặc update username nếu đã có
    pg_exec("""
        INSERT INTO wallet (tele_id, username, balance, status, notes, gift)
        VALUES (%s, %s, 0, 'new', 'Chưa kích hoạt', '')
        ON CONFLICT (tele_id) DO UPDATE SET
            username = CASE WHEN %s <> '' THEN %s ELSE wallet.username END,
            updated_at = NOW()
    """, (user_id, username or "", username or "", username or ""))

    # 2) Sheet mirror (fire-and-forget) — chỉ để theo dõi
    if SHEET_READY:
        try:
            row = get_user_row(user_id)
            if not row:
                ws_money.append_row([
                    str(user_id),
                    username or "",
                    0,
                    "new",
                    "Chưa kích hoạt",
                    "",
                    ""
                ])
                invalidate_user_row_cache(user_id)
        except Exception as e:
            dprint(f"ensure_user_exists sheet mirror error: {e}")

def get_user_data(user_id):
    """
    ✅ V6 PG-ONLY:
    - Đọc hoàn toàn từ PostgreSQL (nguồn chính)
    - Không phụ thuộc Google Sheet
    Returns: (exists: bool, balance: int, status: str)
    """
    if PG_POOL is None:
        return False, 0, ""

    r = pg_exec("SELECT balance, status FROM wallet WHERE tele_id=%s", (int(user_id),), fetchone=True)
    if not r:
        return False, 0, ""

    bal = int(r[0] or 0)
    status = (r[1] or "").strip()
    return True, bal, status

def get_balance_direct(user_id):
    """
    ✅ V6 PG-ONLY: Đọc balance từ PostgreSQL.
    """
    if PG_POOL is None:
        return 0

    r = pg_exec("SELECT balance FROM wallet WHERE tele_id=%s", (int(user_id),), fetchone=True)
    if not r:
        return 0
    try:
        return int(r[0] or 0)
    except:
        return 0

def update_balance_atomic(user_id, delta):
    """
    🔥 ATOMIC UPDATE BALANCE (PostgreSQL)
    - Không race-condition
    - Không lệch tiền khi nhiều request song song
    - Mirror ra Google Sheet (tuỳ chọn) để bạn theo dõi
    """
    if PG_POOL is None:
        dprint("⚠️ update_balance_atomic: PG_POOL is None")
        return False, 0

    ensure_user_exists(user_id, username="")

    r = pg_exec("""
        UPDATE wallet
        SET balance = GREATEST(balance + %s, 0),
            updated_at = NOW()
        WHERE tele_id=%s
        RETURNING balance
    """, (int(delta), int(user_id)), fetchone=True)

    if not r:
        return False, 0

    new_balance = int(r[0] or 0)

    # ✅ Mirror sheet để bạn theo dõi
    if SHEET_READY and SHEET_MIRROR_WALLET:
        try:
            row = get_user_row(user_id)
            if row:
                ws_money.update_cell(row, 3, new_balance)
        except Exception as e:
            dprint(f"mirror wallet to sheet error: {e}")

    return True, new_balance

def add_balance(user_id, amount):
    """
    DEPRECATED: Hàm này không atomic, dễ bị race condition
    → Dùng update_balance_atomic(user_id, +amount) thay thế
    """
    dprint(f"⚠️ WARNING: add_balance() is deprecated, use update_balance_atomic()")
    return update_balance_atomic(user_id, amount)

def deduct_balance_atomic(user_id, need_amount):
    """
    ✅ ATOMIC DEDUCT (PostgreSQL)
    Returns:
        (success: bool, new_balance: int)
    """
    need_amount = int(need_amount or 0)
    if need_amount <= 0:
        bal = get_balance_direct(user_id)
        return True, bal

    if PG_POOL is None:
        dprint("⚠️ deduct_balance_atomic: PG_POOL is None")
        return False, 0

    ensure_user_exists(user_id, username="")

    r = pg_exec("""
        UPDATE wallet
        SET balance = balance - %s,
            updated_at = NOW()
        WHERE tele_id=%s AND balance >= %s
        RETURNING balance
    """, (need_amount, int(user_id), need_amount), fetchone=True)

    if not r:
        # không đủ tiền -> trả balance hiện tại
        bal = get_balance_direct(user_id)
        return False, bal

    new_balance = int(r[0] or 0)

    # mirror sheet
    if SHEET_READY and SHEET_MIRROR_WALLET:
        try:
            row = get_user_row(user_id)
            if row:
                ws_money.update_cell(row, 3, new_balance)
        except Exception as e:
            dprint(f"mirror wallet to sheet error: {e}")

    return True, new_balance

def is_tx_exists(tx_id):
    """
    ✅ Check trùng tx_id
    Ưu tiên Postgres (UNIQUE), fallback Sheet nếu PG chưa cấu hình.
    """
    if not tx_id:
        return False

    if PG_POOL is not None:
        r = pg_exec("SELECT 1 FROM processed_tx WHERE tx_id=%s", (str(tx_id),), fetchone=True)
        return bool(r)

    # fallback sheet
    try:
        col = ws_nap_tien.col_values(6)
        return str(tx_id) in col
    except:
        return False

def save_topup_to_sheet(user_id, username, amount, loai, tx_id, note=""):
    if not SHEET_READY or ws_nap_tien is None:
        return

    try:
        ws_nap_tien.append_row([
            now_str(),
            str(user_id),
            username or "",
            int(amount),
            loai,
            str(tx_id),
            note
        ])
    except Exception as e:
        print("[SAVE_TOPUP_ERROR]", e)

def topup_history_text(user_id, limit=10):
    if not SHEET_READY or ws_nap_tien is None:
        return "❌ Hệ thống lịch sử nạp tiền đang lỗi."

    try:
        rows = ws_nap_tien.get_all_records()
    except Exception:
        return "❌ Không đọc được dữ liệu lịch sử nạp tiền."

    logs = []
    for r in rows:
        if str(r.get("Tele ID", "")) == str(user_id):
            logs.append(r)

    if not logs:
        return "📜 <b>Lịch sử nạp tiền</b>\nChưa có giao dịch nào."

    logs = logs[-limit:]

    out = ["📜 <b>Lịch sử nạp tiền (SEPAY)</b>"]
    for r in logs:
        out.append(
            f"- {r.get('time')} | "
            f"+{int(r.get('số tiền', 0)):,}đ | "
            f"{r.get('tx_id')}"
        )

    return "\n".join(out)

# =========================================================
# ⭐ MULTI-COOKIE PARSER ⭐
# =========================================================
def parse_cookies(text):
    """
    ✅ FIXED: Chỉ chấp nhận cookie hợp lệ (bắt đầu bằng SPC_ST= hoặc SPC_)
    Tránh tính nhầm dòng trống, text rác
    """
    cookies = []
    for line in text.splitlines():
        line = line.strip()
        
        # ✅ Chỉ chấp nhận cookie Shopee hợp lệ
        if line.startswith("SPC_ST=") or line.startswith("SPC_"):
            cookies.append(line)
    
    # ✅ Limit tối đa
    if len(cookies) > MAX_COOKIES_PER_REQUEST:
        cookies = cookies[:MAX_COOKIES_PER_REQUEST]
    
    return cookies

# =========================================================
# VOUCHER UTIL
# =========================================================
def get_voucher(cmd):
    """
    ✅ FIXED: Dùng cache thay vì get_all_records() mỗi lần
    """
    if not SHEET_READY:
        return None, "Hệ thống Sheet đang lỗi"

    rows = get_voucher_stock_cached()

    for r in rows:
        name = normalize_voucher_key(r.get("Tên Mã", ""))
        if name == normalize_voucher_key(cmd):
            if r.get("Trạng Thái") != "Còn Mã":
                return None, "Lưu thất Bại. Vui lòng kiểm tra lại cookie - mã"
            return r, None

    return None, "Không tìm thấy voucher"

def save_voucher_and_check(cookie, voucher):
    payload = {
        "voucher_identifiers": [{
            "promotion_id": int(voucher.get("Promotionid")),
            "voucher_code": voucher.get("CODE"),
            "signature": voucher.get("Signature"),
            "signature_source": 0
        }],
        "need_user_voucher_status": True
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://shopee.vn",
        "Referer": "https://shopee.vn/",
        "Cookie": cookie
    }

    try:
        r = requests.post(SAVE_URL, headers=headers, json=payload, timeout=15)

        if r.status_code != 200:
            return False, f"HTTP_{r.status_code}"

        js = r.json()
        if "responses" not in js or not js["responses"]:
            return False, "INVALID_RESPONSE"

        resp = js["responses"][0]
        error_code = resp.get("error", 0)

        # ✅ SUCCESS
        if error_code == 0:
            return True, "OK"
        
        # ❌ ERROR CODE 5: Không đủ điều kiện
        if error_code == 5:
            return False, "ACC_NOT_ELIGIBLE"
        
        # ❌ ERROR CODE 14: Đã lưu hoặc sử dụng trước đó
        if error_code == 14:
            return False, "VOUCHER_ALREADY_SAVED"

        # ❌ OTHER ERRORS
        return False, f"SHOPEE_{error_code}"

    except requests.exceptions.Timeout:
        return False, "TIMEOUT"
    except Exception as e:
        return False, f"EXCEPTION_{str(e)}"

def format_shopee_error(error_code):
    """
    Format lỗi Shopee thân thiện với user
    
    Args:
        error_code: String error code từ save_voucher_and_check
        
    Returns:
        Friendly error message
    """
    error_messages = {
        "ACC_NOT_ELIGIBLE": "❌ <b>ACC KHÔNG ĐỦ ĐIỀU KIỆN</b>\n\n⚠️ Tài khoản Shopee không đủ điều kiện để lưu voucher này (Error 5)",
        "VOUCHER_ALREADY_SAVED": "❌ <b>ĐÃ LƯU TRƯỚC ĐÓ</b>\n\n⚠️ ACC đã lưu hoặc sử dụng voucher này rồi (Error 14)",
        "TIMEOUT": "❌ <b>TIMEOUT</b>\n\n⚠️ Shopee phản hồi quá chậm, vui lòng thử lại",
        "INVALID_RESPONSE": "❌ <b>LỖI RESPONSE</b>\n\n⚠️ Shopee trả về dữ liệu không hợp lệ"
    }
    
    # Check exact match
    if error_code in error_messages:
        return error_messages[error_code]
    
    # Check SHOPEE_ prefix
    if error_code.startswith("SHOPEE_"):
        code_number = error_code.split("_")[1]
        return f"❌ <b>LỖI SHOPEE</b>\n\n⚠️ Mã lỗi: {code_number}"
    
    # Check HTTP_ prefix
    if error_code.startswith("HTTP_"):
        http_code = error_code.split("_")[1]
        return f"❌ <b>LỖI HTTP</b>\n\n⚠️ HTTP {http_code}"
    
    # Check EXCEPTION_ prefix
    if error_code.startswith("EXCEPTION_"):
        return f"❌ <b>LỖI HỆ THỐNG</b>\n\n⚠️ {error_code}"
    
    # Default
    return f"❌ <b>LỖI</b>\n\n⚠️ {error_code}"

# =========================================================
# ⭐ MULTI-COOKIE VOUCHER SAVER ⭐
# =========================================================
def save_voucher_multi_cookies(cookies, voucher):
    success_count = 0
    failed_details = []

    for idx, cookie in enumerate(cookies, 1):
        ok, reason = save_voucher_and_check(cookie, voucher)

        if ok:
            success_count += 1
            dprint(f"✅ Cookie #{idx}: SUCCESS")
        else:
            failed_details.append((idx, reason))
            dprint(f"❌ Cookie #{idx}: {reason}")

        if idx < len(cookies):
            time.sleep(0.1)

    return success_count, len(cookies), failed_details

# =========================================================
# COMBO UTIL
# =========================================================
def get_vouchers_by_combo(combo_key):
    """
    ✅ FIXED: Dùng cache thay vì get_all_records() mỗi lần
    """
    if not SHEET_READY:
        return [], "Hệ thống Sheet đang lỗi"

    rows = get_voucher_stock_cached()

    items = []
    for r in rows:
        c = str(r.get("Combo", "")).strip().lower()
        if c == combo_key.strip().lower():
            if r.get("Trạng Thái") == "Còn Mã":
                items.append(r)

    if not items:
        return [], "Combo hiện không có mã"

    return items, None

def calculate_combo_price(combo_key, num_cookies):
    """
    🔥 TÍNH GIÁ COMBO TRƯỚC - KHÔNG LƯU VOUCHER
    
    Dùng để check + trừ tiền TRƯỚC khi lưu voucher
    Tránh case: Lưu được voucher nhưng user không đủ tiền
    
    Args:
        combo_key: combo1, combo2, combo3, etc.
        num_cookies: Số lượng cookie
    
    Returns:
        (success: bool, total_price: int, error_message: str)
    """
    vouchers, err = get_vouchers_by_combo(combo_key)
    if err:
        return False, 0, err
    
    # Tính giá mỗi cookie = tổng giá các voucher trong combo
    price_per_cookie = sum(int(v.get("Giá", 0)) for v in vouchers)
    total_price = price_per_cookie * num_cookies
    
    dprint(f"💰 CALC {combo_key.upper()}: {price_per_cookie:,}đ/cookie × {num_cookies} = {total_price:,}đ")
    
    return True, total_price, None

def process_combo1(cookie):
    vouchers, err = get_vouchers_by_combo(COMBO1_KEY)
    if err:
        return False, err, 0, 0, []

    saved = []
    failed = []

    for v in vouchers:
        ok, reason = save_voucher_and_check(cookie, v)
        if ok:
            saved.append(v)
        else:
            failed.append((v.get("Tên Mã", "UNKNOWN"), reason))

    if not saved:
        return False, "Không lưu được voucher nào", 0, len(vouchers), failed

    total_price = 0
    for v in saved:
        try:
            total_price += int(v.get("Giá", 0))
        except Exception:
            pass

    return True, total_price, len(saved), len(vouchers), failed

def process_combo_multi_cookies(cookies, combo_key):
    """
    ✅ DYNAMIC COMBO PROCESSING
    Xử lý bất kỳ combo nào: combo1, combo2, combo3...
    """
    vouchers, err = get_vouchers_by_combo(combo_key)
    if err:
        return False, err, 0, len(cookies), 0, []

    price_per_cookie = sum(int(v.get("Giá", 0)) for v in vouchers)
    cookies_saved = 0
    failed_details = []

    for cookie_idx, cookie in enumerate(cookies, 1):
        cookie_success = True

        for voucher in vouchers:
            ok, reason = save_voucher_and_check(cookie, voucher)

            if not ok:
                cookie_success = False
                failed_details.append((
                    cookie_idx,
                    voucher.get("Tên Mã", "UNKNOWN"),
                    reason
                ))
                dprint(f"❌ Cookie #{cookie_idx} - {voucher.get('Tên Mã')}: {reason}")
            else:
                dprint(f"✅ Cookie #{cookie_idx} - {voucher.get('Tên Mã')}: OK")

            time.sleep(0.1)

        if cookie_success:
            cookies_saved += 1

        if cookie_idx < len(cookies):
            time.sleep(0.2)

    if cookies_saved == 0:
        return False, "Không lưu được cookie nào", 0, len(cookies), len(vouchers), failed_details

    total_price = cookies_saved * price_per_cookie

    return True, total_price, cookies_saved, len(cookies), len(vouchers), failed_details

# =========================================================
# ⭐ DYNAMIC VOUCHER KEYBOARD FROM SHEET ⭐
# =========================================================
VOUCHER_KEYBOARD_CACHE = {
    "keyboard": None,
    "info_text": None,
    "last_update": 0
}
KEYBOARD_CACHE_DURATION = 60

def apply_strikethrough(text):
    strikethrough_map = {
        'A': 'A̶', 'B': 'B̶', 'C': 'C̶', 'D': 'D̶', 'E': 'E̶', 'F': 'F̶', 'G': 'G̶', 'H': 'H̶',
        'I': 'I̶', 'J': 'J̶', 'K': 'K̶', 'L': 'L̶', 'M': 'M̶', 'N': 'N̶', 'O': 'O̶', 'P': 'P̶',
        'Q': 'Q̶', 'R': 'R̶', 'S': 'S̶', 'T': 'T̶', 'U': 'U̶', 'V': 'V̶', 'W': 'W̶', 'X': 'X̶',
        'Y': 'Y̶', 'Z': 'Z̶',
        'a': 'a̶', 'b': 'b̶', 'c': 'c̶', 'd': 'd̶', 'e': 'e̶', 'f': 'f̶', 'g': 'g̶', 'h': 'h̶',
        'i': 'i̶', 'j': 'j̶', 'k': 'k̶', 'l': 'l̶', 'm': 'm̶', 'n': 'n̶', 'o': 'o̶', 'p': 'p̶',
        'q': 'q̶', 'r': 'r̶', 's': 's̶', 't': 't̶', 'u': 'u̶', 'v': 'v̶', 'w': 'w̶', 'x': 'x̶',
        'y': 'y̶', 'z': 'z̶',
        '0': '0̶', '1': '1̶', '2': '2̶', '3': '3̶', '4': '4̶', '5': '5̶', '6': '6̶', '7': '7̶',
        '8': '8̶', '9': '9̶',
        '%': '%̶', '+': '+̶', '/': '/̶', ' ': ' ̶',
    }
    result = ""
    for char in text:
        result += strikethrough_map.get(char, char)
    return result

def parse_position(pos_str):
    """
    ✅ FIXED: Parse đúng 100%
    1A → (1, 'A')
    A1 → (1, 'A')
    2B → (2, 'B')
    B2 → (2, 'B')
    """
    if not pos_str or not isinstance(pos_str, str):
        return None

    pos_str = pos_str.strip().upper()

    # Kiểu 1A, 2B (số trước, chữ sau)
    m = re.match(r'^(\d+)([A-Z])$', pos_str)
    if m:
        return (int(m.group(1)), m.group(2))

    # Kiểu A1, B2 (chữ trước, số sau)
    m = re.match(r'^([A-Z])(\d+)$', pos_str)
    if m:
        return (int(m.group(2)), m.group(1))

    return None

def build_voucher_keyboard_from_sheet():
    if not SHEET_READY:
        dprint("❌ Sheet not ready, using static keyboard")
        return build_static_voucher_keyboard()

    try:
        dprint("📊 Reading VoucherStock sheet...")
        all_rows = ws_voucher.get_all_records()
        dprint(f"📊 Found {len(all_rows)} rows in VoucherStock")

        vouchers_by_position = {}
        
        # ✅ DYNAMIC COMBO DETECTION
        combos_data = {}  # {combo_key: {price, count, vouchers}}
        
        info_lines = ["🎊 <b>THÔNG BÁO : Đã Hỗ Trợ Get Cookie miễn phí !</b> 🎊\n━━━━━━━━━━━━━━━"]

        for idx, row in enumerate(all_rows, 1):
            display = ""
            for key in ["Display", "Show", "Visible", "Hiển thị", "Hiển Thị"]:
                if key in row:
                    display = str(row[key]).strip().upper()
                    if display:
                        break

            if display not in ["YES", "Y", "TRUE", "1"]:
                continue

            pos_str = str(row.get("Vị trí", "")).strip()
            if not pos_str:
                pos_str = str(row.get("Position", "")).strip()

            # ✅ Detect tất cả combo (combo1, combo2, combo3...)
            combo = str(row.get("Combo", "")).strip().lower()
            if combo.startswith("combo"):
                if combo not in combos_data:
                    combos_data[combo] = {
                        "price": 0,
                        "count": 0,
                        "vouchers": []
                    }
                try:
                    combos_data[combo]["price"] += int(row.get("Giá", 0))
                    combos_data[combo]["count"] += 1
                    combos_data[combo]["vouchers"].append(row)
                except:
                    pass

            if not pos_str:
                continue

            position = parse_position(pos_str)
            if not position:
                continue

            vouchers_by_position[position] = row

        if len(vouchers_by_position) == 0:
            return build_static_voucher_keyboard()

        keyboard_rows = []
        current_row_num = None
        current_row_buttons = []

        sorted_positions = sorted(vouchers_by_position.keys())

        for position in sorted_positions:
            row_num, col_letter = position
            voucher = vouchers_by_position[position]

            if current_row_num != row_num:
                if current_row_buttons:
                    keyboard_rows.append(current_row_buttons)
                current_row_buttons = []
                current_row_num = row_num

            # ✅ Hỗ trợ nhiều tên cột display name
            ten_hien_thi = ""
            for key in ["Display Name", "Tên hiển thị", "Tên Hiển Thị", "display_name"]:
                if key in voucher:
                    ten_hien_thi = str(voucher[key]).strip()
                    if ten_hien_thi:
                        break
            
            # Fallback nếu không có display name
            if not ten_hien_thi:
                ten_hien_thi = str(voucher.get("Tên Mã", "")).strip()

            trang_thai = str(voucher.get("Trạng Thái", "")).strip()
            ten_ma = str(voucher.get("Tên Mã", "")).strip()
            gia = int(voucher.get("Giá", 0))

            is_sold_out = trang_thai != "Còn Mã"

            if is_sold_out:
                # ✅ Giảm độ dài text - bỏ emoji, chỉ giữ "Hết"
                button_text = f"{ten_hien_thi} (Hết)"
                callback_data = f"SOLD_OUT:{ten_ma}"
            else:
                # ✅ Giảm emoji, text ngắn hơn cho mobile
                button_text = f"🎊 {ten_hien_thi}"
                callback_data = f"BUY:{ten_ma}"

            current_row_buttons.append({
                "text": button_text,
                "callback_data": callback_data
            })

            if not is_sold_out:
                info_lines.append(f"• {ten_hien_thi} — 💰Giá {gia:,} VNĐ")

        if current_row_buttons:
            keyboard_rows.append(current_row_buttons)

        # ✅ DYNAMIC COMBO BUTTONS - Tự động thêm tất cả combo từ Sheet
        if combos_data:
            info_lines.append(f"\n🟣 <b>COMBO ĐẶC BIỆT</b>")
            
            # Sort combo theo tên (combo1, combo2, combo3...)
            for combo_key in sorted(combos_data.keys()):
                combo_info = combos_data[combo_key]
                
                # ✅ Tên hiển thị NGẮN hơn cho mobile
                combo_display_names = {
                    "combo1": "🎆 COMBO1 | 100k+Ship",
                    "combo2": "🎆 COMBO2 | Giảm Giá",
                    "combo3": "🎆 COMBO3 | Freeship",
                }
                
                # Fallback: COMBO{N} nếu không có trong map
                combo_num = combo_key.replace("combo", "")
                display_name = combo_display_names.get(
                    combo_key,
                    f"🎆 COMBO{combo_num.upper()}"
                )
                
                # Thêm nút
                keyboard_rows.append([{
                    "text": display_name,
                    "callback_data": f"BUY:{combo_key}"
                }])
                
                # Thông tin combo
                info_lines.append(f"• {combo_key.upper()}: {combo_info['count']} mã")
                info_lines.append(f"  💰 {combo_info['price']:,} VNĐ")

        info_lines.append("\n⭐ <b>HỖ TRỢ LƯU TỐI ĐA 10 COOKIE</b>")
        info_lines.append("💡 Gửi mỗi cookie 1 dòng")
        info_lines.append("\n👇 <b>BẤM NÚT BÊN DƯỚI ĐỂ MUA</b>")

        keyboard = {"inline_keyboard": keyboard_rows}
        info_text = "\n".join(info_lines)

        return keyboard, info_text

    except Exception as e:
        dprint(f"❌ Error building keyboard from sheet: {e}")
        import traceback
        traceback.print_exc()
        return build_static_voucher_keyboard()

def build_static_voucher_keyboard():
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "🎉 Mã 100k 0đ", "callback_data": "BUY:voucher100k"},
                {"text": "✨ Mã 50% Max 200k", "callback_data": "BUY:voucher50max200"},
            ],
            [
                {"text": "🚀 Freeship Hỏa Tốc", "callback_data": "BUY:voucherHoaToc"},
            ],
            [
                {"text": "🎆 COMBO1 | Mã 100k + Ship HT 🎆", "callback_data": "BUY:combo1"}
            ]
        ]
    }

    info_text = (
        "🎊 <b>VOUCHER HIỆN CÓ - HAPPY NEW YEAR 2025!</b> 🎊\n"
        "━━━━━━━━━━━━━━━\n"
        "🟢 <b>Voucher đơn</b>\n"
        "• Mã 100k 0đ — 💰Giá 1.000 VNĐ\n"
        "• Mã 50% Max 200k — 💰Giá 1.000 VNĐ\n"
        "• Freeship Hỏa Tốc — 💰Giá 1.000 VNĐ\n\n"
        "🟣 <b>COMBO ĐẶC BIỆT</b>\n"
        "• COMBO1: 100k/0đ + Freeship Hỏa Tốc\n"
        "  💰 2.000 VNĐ | 🎫 2 mã\n\n"
        "⭐ <b>HỖ TRỢ LƯU TỐI ĐA 10 COOKIE</b>\n"
        "💡 Gửi mỗi cookie 1 dòng\n\n"
        "👇 <b>BẤM NÚT BÊN DƯỚI ĐỂ MUA</b>"
    )

    return keyboard, info_text

def get_voucher_keyboard_cached():
    global VOUCHER_KEYBOARD_CACHE

    now = time.time()

    if (VOUCHER_KEYBOARD_CACHE["keyboard"] and
        now - VOUCHER_KEYBOARD_CACHE["last_update"] < KEYBOARD_CACHE_DURATION):
        dprint("Using cached keyboard")
        return VOUCHER_KEYBOARD_CACHE["keyboard"], VOUCHER_KEYBOARD_CACHE["info_text"]

    dprint("Rebuilding keyboard from sheet...")
    keyboard, info_text = build_voucher_keyboard_from_sheet()

    VOUCHER_KEYBOARD_CACHE["keyboard"] = keyboard
    VOUCHER_KEYBOARD_CACHE["info_text"] = info_text
    VOUCHER_KEYBOARD_CACHE["last_update"] = now

    return keyboard, info_text

def build_voucher_info_text():
    _, info_text = get_voucher_keyboard_cached()
    return info_text

def build_quick_voucher_keyboard():
    keyboard, _ = get_voucher_keyboard_cached()
    return keyboard

def build_quick_buy_keyboard(cmd):
    MAP = {
        "voucher100k": "💸 Mã 100k 0đ",
        "voucher50max200": "💸 Mã 50% max 200k 0đ",
        "voucherHoaToc": "🚀 Freeship Hỏa Tốc",
        "combo1": "🎁 COMBO1 – Mã 100k + Ship HT 🔥"
    }

    text = MAP.get(cmd, f"🎁 {cmd}")

    return {
        "inline_keyboard": [[
            {"text": text, "callback_data": f"BUY:{cmd}"}
        ]]
    }

# =========================================================
# KÍCH HOẠT + TẶNG 5K
# =========================================================
def handle_active_gift_5k(user_id, username):
    """
    ✅ V6 PG-PRIMARY: Kích hoạt tài khoản + Tặng quà
    - Đọc/ghi status + balance từ PostgreSQL (nguồn chính)
    - Sheet mirror là fire-and-forget
    """
    if PG_POOL is None:
        return False, "❌ Hệ thống đang lỗi."

    user_id = int(user_id)
    ensure_user_exists(user_id, username)

    # Đọc status + balance từ PG
    r = pg_exec("SELECT balance, status FROM wallet WHERE tele_id=%s", (user_id,), fetchone=True)
    if not r:
        return False, "❌ Không tìm thấy tài khoản."

    current_balance = int(r[0] or 0)
    status = (r[1] or "").strip()

    # ✅ CHECK 1: Đã active rồi
    if status == "active":
        return False, "⚠️ Tài khoản đã kích hoạt, không thể nhận khuyến mãi."

    # ✅ CHECK 2: Status không được phép
    if status not in ALLOWED_GIFT_STATUS:
        dprint(f"⚠️ User {user_id} status '{status}' not allowed for gift")
        return False, (
            "❌ Tài khoản không đủ điều kiện nhận khuyến mãi.\n"
            "📞 Vui lòng liên hệ admin: @BonBonxHPx"
        )

    try:
        new_balance = current_balance + ACTIVE_GIFT_AMOUNT

        # ✅ Update PG (nguồn chính)
        pg_exec("""
            UPDATE wallet
            SET balance = %s, status = 'active', updated_at = NOW()
            WHERE tele_id = %s
        """, (new_balance, user_id))

        # ✅ Mirror Sheet (fire-and-forget)
        if SHEET_READY:
            try:
                row = get_user_row(user_id)
                if row:
                    ws_money.update(f'C{row}:D{row}', [[new_balance, "active"]])
            except Exception:
                pass

        # ✅ LOG
        log_row(
            user_id,
            username,
            "ACTIVE_GIFT_CLICK",
            str(ACTIVE_GIFT_AMOUNT),
            f"Kích hoạt thủ công + nhận {ACTIVE_GIFT_AMOUNT:,}đ"
        )

        dprint(f"✅ User {user_id} activated: +{ACTIVE_GIFT_AMOUNT:,}đ → {new_balance:,}đ")
        return True, new_balance

    except Exception as e:
        dprint("handle_active_gift_5k error:", e)
        return False, "❌ Lỗi khi kích hoạt"

# =========================================================
# CALLBACK QUERY HANDLER
# =========================================================
def handle_callback_query(cb):
    cb_id = cb.get("id")
    data = cb.get("data", "")
    from_user = cb.get("from", {})
    user_id = from_user.get("id")
    username = from_user.get("username", "")
    chat_id = cb.get("message", {}).get("chat", {}).get("id")

    # ===== QR CALLBACKS =====

    if data.startswith("qr_cancel:"):
        session_id = data.split(":", 1)[1]
        tg_answer_callback(cb_id)
        handle_qr_cancel(chat_id, session_id)
        return

    # ===== ACTIVATE GIFT CALLBACK =====
    if data == "activate_gift":
        tg_answer_callback(cb_id)
        success, result = handle_active_gift_5k(user_id, username)
        
        if success:
            # result là new_balance
            new_balance = result
            tg_send(
                chat_id,
                f"🎉 <b>KÍCH HOẠT THÀNH CÔNG!</b>\n\n"
                f"💰 Bạn đã nhận <b>{ACTIVE_GIFT_AMOUNT:,}đ</b>\n"
                f"💼 Số dư hiện tại: <b>{new_balance:,}đ</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🆕 <b>TÍNH NĂNG MỚI</b>\n\n"
                f"🔑 <b>Get Cookie QR</b>\n"
                f"├ Quét QR lấy Cookie Shopee\n"
                f"├ Không cần nhập thủ công\n"
                f"└ Cookie tự động lưu 7 ngày\n\n"
                f"🖥️ <b>Tool ADD Voucher PC</b>\n"
                f"├ Lưu voucher từ máy tính\n"
                f"├ Nhanh gấp 10 lần bot Telegram\n"
                f"├ Hỗ trợ nhiều tài khoản\n"
                f"└ Bấm nút bên dưới để tải\n\n"
                f"📊 <b>Check Voucher</b>\n"
                f"├ Kiểm tra voucher còn hạn không\n"
                f"├ Xem % đã dùng, lượt lưu\n"
                f"└ Cập nhật real-time\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🛒 <b>Bắt đầu mua voucher ngay!</b>",
                build_main_keyboard(is_active=True)
            )
        else:
            # result là error message
            tg_send(chat_id, result, build_main_keyboard(is_active=True))
        return

    # ===== QUICK SAVE VOUCHER CALLBACKS =====
    if data.startswith("QUICK_SAVE:"):
        voucher_key = data.split(":", 1)[1]
        voucher_key = normalize_voucher_key(voucher_key)
        
        if voucher_key == "back":
            tg_answer_callback(cb_id)
            tg_send(chat_id, "👋 Đã quay về menu chính", build_main_keyboard())
            return
        
        # Lấy cookie đã lưu
        cookie = get_user_cookie(user_id)
        
        if not cookie:
            tg_answer_callback(cb_id, "❌ Cookie đã hết hạn. Vui lòng Get Cookie QR lại!", True)
            return
        
        # Check balance
        exists, balance, status = get_user_data(user_id)
        if not exists:
            tg_answer_callback(cb_id, "❌ Bạn chưa có tài khoản", True)
            return
        
        if status != "active":
            tg_answer_callback(cb_id, "❌ Tài khoản chưa được kích hoạt", True)
            return
        
        # ✅ GỬI MESSAGE "ĐANG LƯU VOUCHER..."
        tg_answer_callback(cb_id)
        tg_send(chat_id, "⏳ <b>Đang lưu voucher...</b>")
        
        # ✅ TÌM VOUCHER ĐỘNG TỪ SHEET (không dùng voucher_map)
        # voucher_key đã normalize (no space, lowercase)
        voucher_info = None
        voucher_cmd = voucher_key  # ← FIX: Define voucher_cmd
        err_msg = None
        
        try:
            rows = get_voucher_stock_cached()
            
            for r in rows:
                ten_ma = normalize_voucher_key(r.get("Tên Mã", ""))
                if ten_ma == voucher_key:
                    # Check trạng thái
                    if r.get("Trạng Thái") != "Còn Mã":
                        err_msg = "Voucher này tạm hết mã"
                        break
                    voucher_info = r
                    voucher_cmd = r.get("Tên Mã", voucher_key)  # ← FIX: Lấy tên gốc
                    break
            
            if not voucher_info and not err_msg:
                err_msg = "Không tìm thấy voucher"
        except Exception as e:
            dprint(f"[ERROR] Finding voucher: {e}")
            dprint(f"[ERROR] Traceback: {traceback.format_exc()}")
            err_msg = f"Lỗi đọc sheet: {str(e)}"
        
        if not voucher_info:
            tg_send(
                chat_id,
                f"❌ <b>LƯU THẤT BẠI</b>\n\n"
                f"⚠️ Lỗi: {err_msg}"
            )
            return
        
        price = int(voucher_info.get("Giá", 0))
        display_name = voucher_info.get("Tên Mã", voucher_key)
        
        # Check balance
        if balance < price:
            tg_send(
                chat_id,
                f"❌ <b>KHÔNG ĐỦ SỐ DƯ</b>\n\n"
                f"💰 Cần: <b>{price:,}đ</b>\n"
                f"💼 Số dư: <b>{balance:,}đ</b>\n"
                f"💸 Thiếu: <b>{price - balance:,}đ</b>"
            )
            return
        
        # Trừ tiền trước
        success, new_balance = deduct_balance_atomic(user_id, price)
        
        if not success:
            tg_send(
                chat_id,
                f"❌ <b>TRỪ TIỀN THẤT BẠI</b>\n\n"
                f"💰 Cần: <b>{price:,}đ</b>\n"
                f"💼 Số dư: <b>{new_balance:,}đ</b>"
            )
            return
        
        # Lưu voucher
        try:
            ok, result = save_voucher_and_check(cookie, voucher_info)
            
            if ok:
                # Thành công
                real_balance = get_balance_direct(user_id)
                
                tg_send(
                    chat_id,
                    f"🎉 <b>LƯU THÀNH CÔNG</b>\n\n"
                    f"✅ <b>{voucher_info.get('Tên Mã', voucher_cmd)}</b>\n"
                    f"🍪 1 cookie\n"
                    f"💰 <b>-{price:,}đ</b>\n"
                    f"💼 Số dư: <b>{real_balance:,}đ</b>",
                    build_main_keyboard()
                )
                
                # Log
                log_voucher_save(user_id, username, voucher_cmd, 1, price, real_balance, "✅")
                
            else:
                # Thất bại → Hoàn tiền
                update_balance_atomic(user_id, price)
                real_balance = get_balance_direct(user_id)
                
                # Format lỗi thân thiện
                error_message = format_shopee_error(result)
                
                tg_send(
                    chat_id,
                    f"{error_message}\n\n"
                    f"💸 Đã hoàn tiền: <b>+{price:,}đ</b>\n"
                    f"💼 Số dư: <b>{real_balance:,}đ</b>",
                    build_main_keyboard()
                )
                
                # Log
                log_voucher_save(user_id, username, voucher_cmd, 1, 0, real_balance, f"❌ {result}")
        
        except Exception as e:
            # ❌ EXCEPTION → Hoàn tiền và báo lỗi
            dprint(f"[ERROR] Save voucher exception: {e}")
            dprint(f"[ERROR] Traceback: {traceback.format_exc()}")
            
            update_balance_atomic(user_id, price)
            real_balance = get_balance_direct(user_id)
            
            tg_send(
                chat_id,
                f"❌ <b>LỖI HỆ THỐNG</b>\n\n"
                f"⚠️ Exception: {str(e)[:200]}\n\n"
                f"💸 Đã hoàn tiền: <b>+{price:,}đ</b>\n"
                f"💼 Số dư: <b>{real_balance:,}đ</b>",
                build_main_keyboard()
            )
            
            log_voucher_save(user_id, username, voucher_cmd, 1, 0, real_balance, f"❌ EXCEPTION")
        
        return

    if data.startswith("SOLD_OUT:"):
        tg_answer_callback(cb_id, "⚠️ Voucher này tạm hết mã. Vui lòng quay lại sau!", True)
        return

    if data.startswith("BUY:"):
        cmd = data.split(":", 1)[1]

        # ✅ RATE LIMIT - Ngăn spam click BUY
        last_callback_time = CALLBACK_COOLDOWN.get(user_id, 0)
        if time.time() - last_callback_time < CALLBACK_COOLDOWN_SECONDS:
            tg_answer_callback(cb_id, "⏳ Chậm lại 1 chút", True)
            dprint(f"⏳ Callback rate-limited: user {user_id}")
            return
        
        CALLBACK_COOLDOWN[user_id] = time.time()

        exists, balance, status = get_user_data(user_id)
        if not exists:
            tg_answer_callback(cb_id, "❌ Bạn chưa có ID", True)
            return

        if status != "active":
            tg_answer_callback(cb_id, "❌ Tài khoản chưa được kích hoạt", True)
            return

        if user_id in PENDING_VOUCHER:
            old_pending = PENDING_VOUCHER[user_id]
            old_cmd = old_pending["cmd"] if isinstance(old_pending, dict) else old_pending
            dprint(f"Cleared old pending: {old_cmd}")

        # ✅ Lưu với timestamp
        PENDING_VOUCHER[user_id] = {
            "cmd": cmd,
            "ts": time.time()
        }

        tg_answer_callback(cb_id)
        tg_send(
            user_id,
            f"👉 Gửi <b>cookie</b> vào đây để lưu <b>{cmd}</b>\n\n"
            f"⭐ <b>Hỗ trợ lưu tối đa 10 cookie</b>\n"
            f"💡 Gửi mỗi cookie 1 dòng"
        )
        return

    # ===== SYSTEM MENU CALLBACKS =====
    if data.startswith("SYSTEM:"):
        action = data.split(":")[1]
        
        if action == "bot_list":
            bot_list_menu = {
                "inline_keyboard": [
                    [{"text": "🔴 Bot Lưu Voucher", "url": "https://t.me/nganmiu_bot"}],
                    [{"text": "📦 Bot Check Đơn Hàng", "url": "https://t.me/ShopeeXCheck_Bot"}],
                    [{"text": "📲 Bot Thuê Số (Sắp mở)", "callback_data": "SYSTEM:coming_soon"}],
                    [{"text": "🔙 Quay lại", "callback_data": "SYSTEM:back"}],
                ]
            }
            
            tg_answer_callback(cb_id)
            tg_edit_message(
                chat_id,
                cb_msg_id,
                "📱 <b>DANH SÁCH BOT NGÂNMIU</b>\n\n"
                "🤖 Hệ sinh thái bot của chúng tôi:\n\n"
                "🔴 <b>Bot Lưu Voucher</b>\n"
                "└ Lưu voucher Shopee tự động\n\n"
                "📦 <b>Bot Check Đơn Hàng</b>\n"
                "└ Kiểm tra trạng thái đơn hàng\n\n"
                "📲 <b>Bot Thuê Số</b> (Sắp ra mắt)\n"
                "└ Thuê số điện thoại nhận OTP",
                bot_list_menu
            )
            return
        
        if action == "coming_soon":
            tg_answer_callback(cb_id, "🚧 Tính năng đang phát triển!", True)
            return
        
        if action == "back":
            system_menu = {
                "inline_keyboard": [
                    [{"text": "👤 Admin hỗ trợ", "url": "https://t.me/BonBonxHPx"}],
                    [{"text": "👥 Group Hỗ Trợ", "url": "https://t.me/botxshopee"}],
                    [{"text": "📱 Danh sách Bot", "callback_data": "SYSTEM:bot_list"}],
                    [{"text": "🔴 Bot Lưu Voucher", "url": "https://t.me/nganmiu_bot"}],
                    [{"text": "📦 Bot Check Đơn Hàng", "url": "https://t.me/ShopeeXCheck_Bot"}],
                    [{"text": "📲 Bot Thuê Số", "callback_data": "SYSTEM:coming_soon"}],
                ]
            }
            
            tg_answer_callback(cb_id)
            tg_edit_message(
                chat_id,
                cb_msg_id,
                "🏠 <b>HỆ THỐNG BOT NGÂNMIU</b>\n\n"
                "👋 Chào mừng bạn đến với hệ sinh thái bot NgânMiu!\n\n"
                "📌 <b>Chọn một trong các dịch vụ bên dưới:</b>",
                system_menu
            )
            return

    tg_answer_callback(cb_id, "⚠️ Thao tác không hỗ trợ", True)

# =========================================================
# TỔNG KẾT KINH DOANH
# =========================================================
def parse_date_from_sheet(date_str):
    try:
        if isinstance(date_str, datetime):
            return date_str
        return datetime.strptime(str(date_str).strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(str(date_str).strip(), "%d/%m/%Y %H:%M:%S")
        except Exception:
            return None

def get_today_stats():
    if not SHEET_READY:
        return None

    today = datetime.now(VIETNAM_TZ).date()
    stats = {
        "napten_count": 0,
        "napten_amount": 0,
        "napten_bonus": 0,
        "napten_users": set(),
        "voucher_details": {},
        "total_usage": 0,
        "active_users": set(),
    }

    try:
        if ws_nap_tien:
            all_rows = ws_nap_tien.get_all_values()
            for row in all_rows[1:]:
                if len(row) < 7:
                    continue
                try:
                    row_date = parse_date_from_sheet(row[0])
                    if row_date and row_date.date() == today:
                        user_id = int(row[1])
                        amount = int(row[3]) if row[3] else 0
                        note = row[6]

                        stats["napten_count"] += 1
                        stats["napten_amount"] += amount
                        stats["napten_users"].add(user_id)
                        stats["active_users"].add(user_id)

                        if note and "=" in note:
                            try:
                                stats["napten_bonus"] += int(note.split("=")[1])
                            except:
                                pass
                except:
                    continue
    except Exception as e:
        dprint(f"Error reading Nap Tien: {e}")

    try:
        if ws_log:
            all_logs = ws_log.get_all_values()
            for row in all_logs[1:]:
                if len(row) < 6:
                    continue
                try:
                    row_date = parse_date_from_sheet(row[0])
                    if row_date and row_date.date() == today:
                        user_id = int(row[1])
                        action = row[3]
                        details = row[5]

                        stats["active_users"].add(user_id)

                        if action == "VOUCHER":
                            voucher_name = details
                            if voucher_name not in stats["voucher_details"]:
                                stats["voucher_details"][voucher_name] = 0
                            stats["voucher_details"][voucher_name] += 1
                            stats["total_usage"] += 1

                        elif action == "COMBO1":
                            if "COMBO1" not in stats["voucher_details"]:
                                stats["voucher_details"]["COMBO1"] = 0
                            stats["voucher_details"]["COMBO1"] += 1
                            stats["total_usage"] += 1
                except:
                    continue
    except Exception as e:
        dprint(f"Error reading Logs: {e}")

    stats["napten_users"] = len(stats["napten_users"])
    stats["active_users"] = len(stats["active_users"])

    return stats

def format_tongket_message(stats):
    if not stats:
        return "❌ Không thể lấy dữ liệu"

    today_str = datetime.now(VIETNAM_TZ).strftime("%d/%m/%Y")
    total_in = stats["napten_amount"] + stats["napten_bonus"]

    msg = f"""📊 <b>BÁO CÁO TỔNG KẾT</b>
📅 {today_str}

━━━━━━━━━━━━━━━━━━
💰 <b>NẠP TIỀN</b>
• Lượt nạp: <b>{stats['napten_count']}</b>
• User nạp: <b>{stats['napten_users']}</b>
• Tiền gốc: <b>{stats['napten_amount']:,}đ</b>
• Thưởng: <b>+{stats['napten_bonus']:,}đ</b>
• <b>Tổng vào: {total_in:,}đ</b>

━━━━━━━━━━━━━━━━━━
🎟️ <b>VOUCHER ĐÃ LƯU</b>"""

    grouped = {}

    for raw_key, count in stats["voucher_details"].items():
        raw = raw_key.lower()

        if "combo1" in raw:
            base = "COMBO1"
        elif "hoatoc" in raw:
            base = "voucherHoaToc"
        else:
            m = re.search(r"(voucher[a-z0-9]+)", raw)
            if m:
                base = m.group(1)
            else:
                base = raw_key

        grouped.setdefault(base, 0)
        grouped[base] += count

    DISPLAY_NAME = {
        "voucher100k": "💎 Mã 100k 0đ",
        "voucher30k": "🎁 Mã 30k",
        "voucher50max100": "🎁 Mã 50% Max 100k",
        "voucher50max200": "🎁 Mã 50% Max 200k",
        "voucherHoaToc": "🚀 Freeship Hỏa Tốc",
        "COMBO1": "🎆 COMBO1 | 100k + Ship HT",
    }

    total = 0
    for base, count in sorted(grouped.items(), key=lambda x: x[1], reverse=True):
        name = DISPLAY_NAME.get(base, base)
        msg += f"\n• {name}: <b>{count}</b> lượt"
        total += count

    msg += f"\n\n<b>━ Tổng: {total} lượt lưu</b>"

    msg += f"""

━━━━━━━━━━━━━━━━━━
👥 <b>USER HOẠT ĐỘNG</b>
• Tổng: <b>{stats['active_users']}</b> user
"""

    return msg

def handle_tongket_command(chat_id, user_id):
    if user_id != ADMIN_ID:
        tg_send(chat_id, "⛔ Chỉ admin")
        return

    tg_send(chat_id, "⏳ Đang tổng hợp dữ liệu...")
    stats = get_today_stats()

    if not stats:
        tg_send(chat_id, "❌ Lỗi khi đọc dữ liệu")
        return

    msg = format_tongket_message(stats)
    tg_send(chat_id, msg)

# =========================================================
# 🔥 STATS COMMAND - XEM CACHE STATISTICS
# =========================================================
def handle_stats_command(chat_id, user_id):
    """Admin command: xem cache stats"""
    if user_id != ADMIN_ID:
        tg_send(chat_id, "⛔ Chỉ admin")
        return

    stats = f"""📊 <b>CACHE STATISTICS</b>

🔢 <b>Row Cache:</b>
• Cached users: {len(USER_ROW_CACHE)}
• TTL: {USER_ROW_CACHE_TTL}s (1h)
• Memory: ~{len(USER_ROW_CACHE) * 8} bytes

📢 <b>Broadcast Cache:</b>
• Cached: {"Yes" if BROADCAST_USER_CACHE else "No"}
• Count: {len(BROADCAST_USER_CACHE) if BROADCAST_USER_CACHE else 0}
• Age: {int(time.time() - BROADCAST_USER_CACHE_TIME)}s

💬 <b>Message Dedup:</b>
• Tracked: {len(PROCESSED_MESSAGES)}

━━━━━━━━━━━━━━━━━━
<b>✅ Cache hit → Không gọi Sheet</b>
<b>❌ Cache miss → Gọi Sheet (hiếm)</b>

<b>Hiệu quả:</b> Giảm ~90% API calls!
"""
    tg_send(chat_id, stats)

# =========================================================
# CORE UPDATE HANDLER
# =========================================================
def handle_update(update):
    dprint("UPDATE:", update)

    # ✅ UPDATE_ID DEDUPLICATION - Tránh Telegram resend khi lag
    update_id = update.get("update_id")
    
    if update_id:
        if update_id in PROCESSED_UPDATE_IDS:
            dprint(f"⚠️ DUPLICATE UPDATE_ID DETECTED: {update_id} - SKIPPING")
            return
        
        # ✅ deque tự động drop oldest khi đầy (maxlen=2000)
        PROCESSED_UPDATE_IDS.append(update_id)

    # ✅ MESSAGE DEDUPLICATION
    global PROCESSED_MESSAGES
    msg = update.get("message", {})
    message_id = msg.get("message_id")

    if message_id:
        chat_id = msg.get("chat", {}).get("id")
        msg_key = f"{chat_id}_{message_id}"

        if msg_key in PROCESSED_MESSAGES:
            dprint(f"⚠️ DUPLICATE MESSAGE DETECTED: {msg_key} - SKIPPING")
            return

        PROCESSED_MESSAGES.add(msg_key)

        if len(PROCESSED_MESSAGES) > MAX_PROCESSED_MESSAGES:
            old_msgs = list(PROCESSED_MESSAGES)[:100]
            for old_msg in old_msgs:
                PROCESSED_MESSAGES.discard(old_msg)
            dprint(f"🗑️ Cleaned {len(old_msgs)} old messages from cache")

    # ✅ CHECK BAN STATUS
    msg = update.get("message") or update.get("callback_query", {}).get("message", {})
    from_user = msg.get("from") or update.get("callback_query", {}).get("from", {})
    user_id = from_user.get("id")

    if not user_id:
        return

    ban_status = check_ban_status(user_id)

    if ban_status["banned"]:
        ban_type = ban_status["type"]
        ban_until = ban_status["until"]

        msg_text = (
            "⛔ <b>TÀI KHOẢN BỊ KHÓA</b>\n\n"
            "🚫 <b>Lý do:</b> Spam hệ thống\n"
        )

        if ban_type == "PERMANENT":
            msg_text += "⏰ <b>Thời gian:</b> Vĩnh viễn\n\n"
        else:
            msg_text += (
                f"⏰ <b>Thời gian:</b> 1 giờ\n"
                f"⏱️ <b>Hết hạn:</b> {ban_until}\n\n"
            )

        msg_text += "📞 <b>Liên hệ:</b> @BonBonxHPx"

        chat_id = msg.get("chat", {}).get("id")
        if chat_id:
            tg_send(chat_id, msg_text)

        return

    # ===== CALLBACK QUERY =====
    if "callback_query" in update:
        handle_callback_query(update["callback_query"])
        return

    # ===== MESSAGE =====
    msg = update.get("message")
    if not msg:
        return

    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    username = msg["from"].get("username", "")
    text = (msg.get("text") or "").strip()

    # /tongket
    if text == "/tongket":
        handle_tongket_command(chat_id, user_id)
        return

    # /stats - XEM CACHE STATS
    if text == "/stats":
        handle_stats_command(chat_id, user_id)
        return

    # /update
    if text == "/update":
        if user_id != ADMIN_ID:
            tg_send(chat_id, "⛔ Chỉ admin")
            return

        global VOUCHER_KEYBOARD_CACHE
        VOUCHER_KEYBOARD_CACHE = {
            "keyboard": None,
            "info_text": None,
            "last_update": 0
        }

        voucher_keyboard, voucher_info = get_voucher_keyboard_cached()

        tg_send(
            chat_id,
            "✅ Đã cập nhật keyboard từ Sheet!\n\n"
            "🎊 <b>Menu đã được refresh</b>",
            build_main_keyboard(is_active=True)
        )

        tg_send(chat_id, voucher_info, voucher_keyboard)
        return

    if not text:
        if user_id not in PENDING_VOUCHER:
            return

    # ===== ADMIN: /thongbao =====
    if text and text.startswith("/thongbao"):
        if user_id != ADMIN_ID:
            tg_send(chat_id, "⛔ Lệnh này chỉ dành cho Admin")
            return

        message_id = msg.get("message_id", 0)

        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            tg_send(
                chat_id,
                "📢 <b>HƯỚNG DẪN BROADCAST</b>\n\n"
                "Sử dụng: <code>/thongbao [nội dung]</code>\n\n"
                "Ví dụ:\n"
                "<code>/thongbao Đêm qua server bị lỗi dẫn tới bot không hoạt động, "
                "Hiện tại BOT đã hoạt động bình thường trở lại.</code>"
            )
            return

        if is_broadcast_message_processed(message_id):
            tg_send(
                chat_id,
                "⚠️ <b>Thông báo này đã được gửi trước đó</b>\n"
                "Bot đã tự động bỏ qua để tránh gửi lặp."
            )
            dprint(f"⚠️ DUPLICATE BROADCAST BLOCKED: msg_id={message_id}")
            return

        can_broadcast, wait_time = check_broadcast_cooldown_from_sheet()
        if not can_broadcast:
            tg_send(
                chat_id,
                f"⏳ <b>VUI LÒNG ĐỢI {wait_time}s</b>\n\n"
                f"🔒 Broadcast gần đây chưa đủ thời gian cooldown\n\n"
                f"<i>Hệ thống tự động chống spam broadcast.</i>"
            )
            dprint(f"⏳ COOLDOWN BLOCKED: wait {wait_time}s")
            return

        message = parts[1].strip()

        global IS_BROADCASTING
        if IS_BROADCASTING:
            tg_send(
                chat_id,
                "⛔ <b>Đang có broadcast khác chạy</b>\n"
                "Vui lòng đợi broadcast trước hoàn tất."
            )
            return

        IS_BROADCASTING = True

        if not set_broadcast_state_to_sheet(user_id, "STARTED", message_id):
            IS_BROADCASTING = False
            tg_send(chat_id, "❌ Lỗi khi lưu trạng thái broadcast, vui lòng thử lại")
            return

        dprint(f"📝 Broadcast STARTED | admin={user_id} | msg_id={message_id}")

        tg_send(
            chat_id,
            "✅ <b>ĐÃ NHẬN LỆNH BROADCAST</b>\n\n"
            "⏳ Đang gửi thông báo...\n"
            "📊 Kết quả sẽ được trả về sau khi hoàn tất."
        )

        try:
            dprint(f"🔔 Broadcasting: {message[:40]}...")
            success, failed = broadcast_message(message, exclude_admin=False)

            log_row(user_id, username, "BROADCAST", str(success), message[:50])

            set_broadcast_state_to_sheet(user_id, "COMPLETED", message_id)

            tg_send(
                chat_id,
                f"✅ <b>BROADCAST HOÀN TẤT</b>\n\n"
                f"👥 Thành công: <b>{success}</b>\n"
                f"❌ Thất bại: <b>{failed}</b>"
            )

        except Exception as e:
            dprint(f"❌ Broadcast error: {e}")
            set_broadcast_state_to_sheet(user_id, "FAILED", message_id)
            tg_send(chat_id, f"❌ Lỗi khi broadcast: {str(e)}")

        finally:
            IS_BROADCASTING = False

    # ===== /start =====
    if text == "/start":
        # ✅ Check user mới (PG-based)
        r_check = pg_exec("SELECT tele_id FROM wallet WHERE tele_id=%s", (int(user_id),), fetchone=True) if PG_POOL else None
        is_new_user = r_check is None

        ensure_user_exists(user_id, username)
        exists, balance, status = get_user_data(user_id)

        # ✅ User chưa kích hoạt (status != 'active') → Hiển thị nút kích hoạt
        if status != "active":
            activate_button = {
                "inline_keyboard": [[
                    {"text": "🎁 Kích hoạt nhận 5,100đ", "callback_data": "activate_gift"}
                ]]
            }
            
            if is_new_user:
                # User mới
                tg_send(
                    chat_id,
                    f"🎉 <b>CHÀO MỪNG BẠN MỚI!</b>\n\n"
                    f"👋 Xin chào <b>{username or 'bạn'}</b>\n\n"
                    f"💼 Số dư hiện tại: <b>{balance:,}đ</b>\n"
                    f"📊 Trạng thái: <b>Chưa kích hoạt</b>\n\n"
                    f"🎁 <b>Nhấn nút bên dưới để kích hoạt và nhận {NEW_USER_BONUS:,}đ!</b>",
                    activate_button
                )
            else:
                # User cũ chưa active
                tg_send(
                    chat_id,
                    f"👋 <b>Chào mừng quay lại!</b>\n\n"
                    f"💼 Số dư hiện tại: <b>{balance:,}đ</b>\n"
                    f"📊 Trạng thái: <b>{status}</b>\n\n"
                    f"🎁 <b>Nhấn nút bên dưới để kích hoạt và nhận {ACTIVE_GIFT_AMOUNT:,}đ!</b>",
                    activate_button
                )
            return

        # ✅ User đã active - Không hiển thị nút kích hoạt
        tg_send(
            chat_id,
            f"👋 <b>Chào mừng quay lại!</b>\n\n"
            f"💼 Số dư: <b>{balance:,}đ</b>\n"
            f"📊 Trạng thái: <b>Đã kích hoạt ✅</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🆕 <b>TÍNH NĂNG MỊN</b>\n\n"
            f"🔑 <b>Get Cookie QR</b>\n"
            f"├ Quét mã QR để lấy Cookie Shopee\n"
            f"├ Không cần nhập thủ công\n"
            f"└ Cookie tự động lưu 7 ngày\n\n"
            f"🖥️ <b>Tool ADD Voucher PC</b>\n"
            f"├ Lưu voucher từ máy tính\n"
            f"├ Tốc độ nhanh hơn 10 lần\n"
            f"├ Hỗ trợ nhiều tài khoản cùng lúc\n"
            f"└ Tải ngay: Bấm nút bên dưới\n\n"
            f"📊 <b>Check Voucher</b>\n"
            f"├ Kiểm tra trạng thái voucher\n"
            f"├ Xem % đã dùng, lượt lưu\n"
            f"└ Cập nhật real-time\n"
            f"━━━━━━━━━━━━━━━━━━━━",
            build_main_keyboard(is_active=True)
        )
        return

    # ===== NẠP TIỀN =====
    if text in ("💎 Nạp tiền", "💳 Nạp tiền"):
        ensure_user_exists(user_id, username)

        qr = build_sepay_qr(user_id)

        caption = (
            "💳 <b>NẠP TIỀN TỰ ĐỘNG (SEPAY)</b>\n\n"
            "📌 <b>NỘI DUNG CHUYỂN KHOẢN (BẮT BUỘC)</b>\n"
            f"<code>SEVQR NAP {user_id}</code>\n\n"
            "⚠️ <b>LƯU Ý</b>\n"
            "• Nhập <b>ĐÚNG</b> nội dung để hệ thống tự cộng tiền\n"
            "• Không sửa – không thêm ký tự khác\n\n"
            "💰 <b>NẠP TỐI THIỂU:</b> <b>10.000đ</b>\n\n"
            "🎁 <b>ƯU ĐÃI NẠP TIỀN</b>\n"
            "• ≥ 20.000đ 🎁 +10%\n"
            "• ≥ 50.000đ 🎁 +15%\n"
            "• ≥ 100.000đ 🎁 +20%\n\n"
            "⚡ <i>Tiền vào tài khoản trong vòng 0–30 giây</i>"
        )

        tg_send_photo(chat_id, qr, caption)
        return

    # ===== GET COOKIE QR =====
    if text == "🔑 Get Cookie QR":
        handle_get_cookie_qr(chat_id, user_id, username)
        return

    # ===== USER DATA =====
    exists, balance, status = get_user_data(user_id)
    if not exists:
        tg_send(chat_id, "❌ Bạn chưa có ID. Bấm /start để kích hoạt.")
        return

    # ===== SỐ DƯ =====
    if text in ("💰 Số dư", "/balance"):
        # ✅ RATE LIMIT: 1 lần/3s per user
        last_balance_check = CALLBACK_COOLDOWN.get(f"balance_{user_id}", 0)
        if time.time() - last_balance_check < 3:
            dprint(f"⏳ Balance check rate-limited: user {user_id}")
            return  # Silent ignore (không spam user)
        
        CALLBACK_COOLDOWN[f"balance_{user_id}"] = time.time()
        
        exists, balance, status = get_user_data(user_id)

        if not exists:
            tg_send(chat_id, "❌ Không tìm thấy tài khoản. Bấm /start để kích hoạt.")
            return
        
        dprint(f"💰 Check balance for user {user_id}: {balance:,}đ (status: {status})")
        
        tg_send(
            chat_id,
            f"💰 <b>Số dư:</b> <b>{balance:,}đ</b>\n"
            f"📌 Trạng thái: <b>{status}</b>",
            build_main_keyboard(is_active=(status == "active"))
        )
        return

    # ===== LỊCH SỬ =====
    if text in ("📜 Lịch sử nạp tiền", "/topup_history"):
        tg_send(chat_id, topup_history_text(user_id))
        return

    # ===== TẢI & LẤY PASS TOOL ADD PC =====
    if text == "🖥️ Tải & Lấy Pass Tool ADD PC":
        if PG_POOL is None:
            tg_send(chat_id, "❌ Hệ thống đang lỗi. Thử lại sau.")
            return

        import secrets
        new_pass = secrets.token_hex(8)  # 16 ký tự hex ngẫu nhiên

        pg_exec("UPDATE wallet SET pass=%s, updated_at=NOW() WHERE tele_id=%s", (new_pass, int(user_id)))

        # mirror sheet (fire-and-forget)
        if SHEET_READY:
            try:
                row = get_user_row(user_id)
                if row:
                    # cột 7 = pass
                    ws_money.update_cell(row, 7, new_pass)
            except Exception:
                pass

        # ✅ LẤY LINK TOOL ĐỘNG TỪ VOUCHERSTOCK
        tool_link = get_tool_pc_link()
        
        if not tool_link:
            # Fallback link mặc định nếu không tìm thấy
            tool_link = "https://t.me/botxshopee/2580"
            dprint("⚠️ Dùng link Tool PC mặc định (không tìm thấy trong sheet)")

        tg_send(
            chat_id,
            f"🖥️ <b>TOOL ADD VOUCHER PC</b>\n\n"
            f"📋 <b>Telegram ID:</b> <code>{user_id}</code>\n"
            f"🔐 <b>Password:</b> <code>{new_pass}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📥 <b>TẢI TOOL:</b>\n"
            f"🔗 <a href='{tool_link}'>Tải ToolADDPC.exe</a>\n\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"📖 <b>HƯỚNG DẪN SỬ DỤNG:</b>\n"
            f"1️⃣ Bấm link bên trên để tải file\n"
            f"2️⃣ Chạy ToolADDPC.exe\n"
            f"3️⃣ Nhập Telegram ID + Password (copy bên trên)\n"
            f"4️⃣ Bấm LOGIN và bắt đầu lưu voucher\n\n"
            f"💡 <b>Tính năng:</b>\n"
            f"• Lưu nhiều voucher cùng lúc\n"
            f"• Hỗ trợ nhiều cookie\n"
            f"• Get Cookie QR ngay trong tool\n"
            f"• Tự động trừ tiền từ số dư bot\n\n"
            f"⚠️ <b>Lưu ý:</b>\n"
            f"• Windows có thể cảnh báo → Bấm 'Run anyway'\n"
            f"• Mỗi lần bấm nút sẽ tạo Password mới\n"
            f"• Tool chỉ chạy trên Windows 10/11\n\n"
            f"❓ Cần hỗ trợ? → @BonBonxHPx"
        )
        
        # Log download
        log_row(user_id, username, "GET_TOOL_INFO", "0", f"Lấy thông tin Tool PC | Pass: {new_pass[:4]}***")
        return

    # ===== HỆ THỐNG BOT =====
    if text == "🧩 Hệ Thống Bot":
        system_menu = {
            "inline_keyboard": [
                [
                    {"text": "👤 Admin hỗ trợ", "url": "https://t.me/BonBonxHPx"},
                    {"text": "👥 Group", "url": "https://t.me/botxshopee"}
                ],
                [
                    {"text": "🔴 Bot Lưu Voucher", "url": "https://t.me/nganmiu_bot"}
                ],
                [
                    {"text": "📦 Bot Check Đơn Hàng", "url": "https://t.me/ShopeeXCheck_Bot"}
                ],
                [
                    {"text": "📲 Bot Thuê Số", "callback_data": "SYSTEM:coming_soon"}
                ]
            ]
        }
        
        tg_send(
            chat_id,
            "🏠 <b>HỆ THỐNG BOT NGÂNMIU</b>\n\n"
            "👋 Chào mừng bạn đến với hệ sinh thái bot NgânMiu!\n\n"
            "📌 <b>Chọn một trong các dịch vụ bên dưới:</b>",
            system_menu
        )
        return

    # ===== VOUCHER =====
    if text in ("🎁 Lưu Voucher", "🎟️Lưu Voucher", "Voucher", "🎟️ Voucher"):
        tg_send(
            chat_id,
            build_voucher_info_text(),
            build_quick_voucher_keyboard()
        )
        return

    # ===== CHECK VOUCHER =====
    if text in ("📊 Check Voucher", "📊 Check voucher", "/checkvoucher"):
        handle_check_voucher(user_id, username)
        return

    # ===== CHẶN LƯU NẾU CHƯA ACTIVE =====
    if status != "active" and (
        text.startswith("/voucher")
        or text.startswith("/combo")
        or user_id in PENDING_VOUCHER
    ):
        tg_send(chat_id, "❌ Tài khoản chưa được kích hoạt.")
        # ✅ KHÔNG track_error - user thật có thể chưa active
        return

    # ===== ĐANG CHỜ COOKIE HOẶC LINK =====
    if user_id in PENDING_VOUCHER and not text.startswith("/"):
        pending_data = PENDING_VOUCHER.pop(user_id)
        
        # ✅ Check nếu là dict (có timestamp) hay string cũ
        if isinstance(pending_data, dict):
            cmd = pending_data["cmd"]
            pending_ts = pending_data["ts"]
            pre_saved_cookie = pending_data.get("cookie")  # ← Cookie từ QUICK_SAVE
            
            # ✅ Check expired (quá 120s)
            if time.time() - pending_ts > PENDING_VOUCHER_TTL:
                tg_send(
                    chat_id,
                    "⏱️ <b>Phiên mua đã hết hạn</b>\n\n"
                    "Vui lòng chọn voucher lại:",
                    build_quick_voucher_keyboard()
                )
                dprint(f"⏱️ PENDING expired for user {user_id} (>{PENDING_VOUCHER_TTL}s)")
                return
        else:
            # Fallback cho format cũ (string)
            cmd = pending_data
            pre_saved_cookie = None

        # ✅ Nếu có cookie sẵn (QUICK_SAVE) → Text là voucher link
        if pre_saved_cookie:
            dprint(f"[QUICK_SAVE] Using pre-saved cookie for user {user_id}")
            cookies = [pre_saved_cookie]
            # Text chính là voucher link, không cần parse cookie
        else:
            # Parse cookie từ text như bình thường
            cookies = parse_cookies(text)

            if not cookies:
                tg_send(chat_id, "❌ Không tìm thấy cookie hợp lệ")
                return

        num_cookies = len(cookies)
        dprint(f"📊 Received {num_cookies} cookies")

        # ✅ Đọc balance từ PostgreSQL
        exists, balance, status = get_user_data(user_id)
        if not exists:
            tg_send(chat_id, "❌ Không tìm thấy ID")
            return

        dprint(f"💰 Balance: {balance:,}đ")

        # ----- DYNAMIC COMBO -----
        if cmd.startswith("combo"):
            # 🔥 BƯỚC 1: TÍNH GIÁ TRƯỚC (không lưu voucher)
            ok, total_price, err_msg = calculate_combo_price(cmd, num_cookies)
            
            if not ok:
                tg_send(chat_id, f"❌ <b>{cmd.upper()} THẤT BẠI</b>\n{err_msg}")
                return
            
            # 🔥 BƯỚC 2: TRỪ TIỀN TRƯỚC
            success, new_bal = deduct_balance_atomic(user_id, total_price)
            
            if not success:
                tg_send(
                    chat_id,
                    f"❌ Không đủ số dư\n"
                    f"💰 Cần: {total_price:,}đ\n"
                    f"💼 Số dư hiện tại: {new_bal:,}đ"
                )
                return
            
            # 🔥 BƯỚC 3: ĐÃ TRỪ TIỀN - BÂY GIỜ MỚI LƯU VOUCHER
            ok, _, cookies_saved, total_cookies, vouchers_per_cookie, failed = process_combo_multi_cookies(cookies, cmd)
            
            if not ok:
                # Không lưu được → HOÀN TIỀN ATOMIC
                update_balance_atomic(user_id, total_price)  # ← ATOMIC
                
                # UI: Hiển thị balance TRỰC TIẾP từ Sheet
                real_balance = get_balance_direct(user_id)
                
                tg_send(
                    chat_id,
                    f"❌ <b>{cmd.upper()} THẤT BẠI</b>\n"
                    f"💸 Đã hoàn tiền: +{total_price:,}đ\n"
                    f"💰 Số dư: <b>{real_balance:,}đ</b>"
                )
                return

            log_row(user_id, username, cmd.upper(), str(total_price), f"Lưu {cmd.upper()} {cookies_saved}/{total_cookies} thành công")

            # ✅ UI: Luôn hiển thị balance TRỰC TIẾP từ Sheet
            real_balance = get_balance_direct(user_id)
            
            if cookies_saved == total_cookies:
                msg_text = f"✅ Lưu {cmd.upper()} <b>{cookies_saved}/{total_cookies}</b> thành công | -{total_price:,}đ | Còn: <b>{real_balance:,}đ</b>"
            else:
                msg_text = f"⚠️ Lưu {cmd.upper()} <b>{cookies_saved}/{total_cookies}</b> thành công | -{total_price:,}đ | Còn: <b>{real_balance:,}đ</b>"

            tg_send(chat_id, msg_text)
            tg_send(chat_id, "👉 <b>Bấm để lưu tiếp nhanh</b>", build_quick_buy_keyboard(cmd))
            return

        # ----- VOUCHER ĐƠN -----
        v, err = get_voucher(cmd)
        if err:
            tg_send(chat_id, f"❌ {err}")
            # ✅ KHÔNG track_error - voucher hết/lỗi là lỗi nghiệp vụ
            return

        price = int(v.get("Giá", 0))
        total_price = price * num_cookies

        # ✅ ATOMIC DEDUCT - Trừ tiền TRƯỚC khi lưu voucher
        success, new_bal = deduct_balance_atomic(user_id, total_price)
        
        if not success:
            tg_send(
                chat_id, 
                f"❌ Không đủ số dư\n"
                f"💰 Cần: {total_price:,}đ ({price:,}đ × {num_cookies})\n"
                f"💼 Số dư hiện tại: {new_bal:,}đ"
            )
            # ✅ KHÔNG track_error - không đủ tiền là lỗi nghiệp vụ
            return

        # ✅ ĐÃ TRỪ TIỀN - Bây giờ mới lưu voucher
        success_count, total_count, failed_details = save_voucher_multi_cookies(cookies, v)

        if success_count == 0:
            # ✅ HOÀN TIỀN ATOMIC vì không lưu được cookie nào
            update_balance_atomic(user_id, total_price)  # ← ATOMIC
            
            # UI: Hiển thị balance TRỰC TIẾP từ Sheet
            real_balance = get_balance_direct(user_id)
            
            tg_send(
                chat_id,
                f"❌ Không lưu được cookie nào\n"
                f"💸 Đã hoàn tiền: +{total_price:,}đ\n"
                f"💰 Số dư hiện tại: <b>{real_balance:,}đ</b>"
            )
            # ✅ KHÔNG track_error - cookie lỗi/Shopee lỗi là lỗi nghiệp vụ
            return

        # ✅ Lưu được một số cookie
        actual_price = price * success_count
        
        # ✅ Hoàn tiền ATOMIC cho cookie thất bại
        if success_count < num_cookies:
            refund = price * (num_cookies - success_count)
            update_balance_atomic(user_id, refund)  # ← ATOMIC
            
            dprint(f"💸 Refunded {refund:,}đ for {num_cookies - success_count} failed cookies")

        log_row(user_id, username, "VOUCHER", str(actual_price), f"Lưu {cmd} {success_count}/{total_count} thành công")
        
        # ✅ UI: Luôn hiển thị balance TRỰC TIẾP từ Sheet
        real_balance = get_balance_direct(user_id)

        if success_count == total_count:
            msg_text = f"✅ Lưu <b>{success_count}/{total_count}</b> thành công | -{actual_price:,}đ | Còn: <b>{real_balance:,}đ</b>"
        else:
            msg_text = f"⚠️ Lưu <b>{success_count}/{total_count}</b> thành công | -{actual_price:,}đ | Còn: <b>{real_balance:,}đ</b>"

        tg_send(chat_id, msg_text)
        tg_send(chat_id, "👉 <b>Bấm để lưu tiếp nhanh</b>", build_quick_buy_keyboard(cmd))
        return

    # ===== FALLBACK: Cookie không có pending (Vercel cold start) =====
    if not text.startswith("/") and "SPC_" in text:
        # User gửi cookie nhưng bot không nhớ đang mua gì
        tg_send(
            chat_id,
            "⚠️ <b>Phiên mua đã hết hạn</b>\n\n"
            "Vui lòng bấm chọn voucher lại:",
            build_quick_voucher_keyboard()
        )
        dprint(f"⚠️ PENDING_VOUCHER lost for user {user_id} (cold start?)")
        return

    # ===== LỆNH /combo1 /combo2 /combo3 <cookie> =====
    if not text:
        return

    parts = text.split(maxsplit=1)
    if not parts:
        return

    cmd = parts[0].replace("/", "")
    cookie_text = parts[1] if len(parts) > 1 else ""

    # ----- DYNAMIC COMBO -----
    if cmd.startswith("combo"):
        if not cookie_text:
            if user_id in PENDING_VOUCHER:
                old_pending = PENDING_VOUCHER[user_id]
                old_cmd = old_pending["cmd"] if isinstance(old_pending, dict) else old_pending
                dprint(f"Cleared old pending: {old_cmd}")

            # ✅ Lưu với timestamp
            PENDING_VOUCHER[user_id] = {
                "cmd": cmd,
                "ts": time.time()
            }
            
            tg_send(
                chat_id,
                f"👉 Gửi <b>cookie</b> để lưu {cmd}\n\n"
                "⭐ <b>Hỗ trợ lưu tối đa 10 cookie</b>\n"
                "💡 Gửi mỗi cookie 1 dòng"
            )
            return

        cookies = parse_cookies(cookie_text)

        if not cookies:
            tg_send(chat_id, "❌ Không tìm thấy cookie hợp lệ")
            return

        num_cookies = len(cookies)

        # 🔥 BƯỚC 1: TÍNH GIÁ TRƯỚC
        ok, total_price, err_msg = calculate_combo_price(cmd, num_cookies)
        
        if not ok:
            tg_send(chat_id, f"❌ {cmd.upper()} THẤT BẠI\n{err_msg}")
            return

        # 🔥 BƯỚC 2: TRỪ TIỀN TRƯỚC
        success, new_bal = deduct_balance_atomic(user_id, total_price)
        
        if not success:
            tg_send(
                chat_id,
                f"❌ Không đủ số dư\n"
                f"💰 Cần: {total_price:,}đ\n"
                f"💼 Số dư hiện tại: {new_bal:,}đ"
            )
            return
        
        # 🔥 BƯỚC 3: ĐÃ TRỪ TIỀN - BÂY GIỜ MỚI LƯU
        ok, _, cookies_saved, total_cookies, vouchers_per_cookie, failed = process_combo_multi_cookies(cookies, cmd)

        if not ok:
            # Không lưu được → HOÀN TIỀN ATOMIC
            update_balance_atomic(user_id, total_price)  # ← ATOMIC
            
            # UI: Hiển thị balance TRỰC TIẾP từ Sheet
            real_balance = get_balance_direct(user_id)
            
            tg_send(
                chat_id,
                f"❌ {cmd.upper()} THẤT BẠI\n"
                f"💸 Đã hoàn tiền: +{total_price:,}đ\n"
                f"💰 Số dư: <b>{real_balance:,}đ</b>"
            )
            return

        log_row(user_id, username, cmd.upper(), str(total_price), f"Lưu {cmd.upper()} {cookies_saved}/{total_cookies} thành công")

        # ✅ UI: Luôn hiển thị balance TRỰC TIẾP từ Sheet
        real_balance = get_balance_direct(user_id)
        
        if cookies_saved == total_cookies:
            msg_text = f"✅ Lưu {cmd.upper()} <b>{cookies_saved}/{total_cookies}</b> thành công | -{total_price:,}đ | Còn: <b>{real_balance:,}đ</b>"
        else:
            msg_text = f"⚠️ Lưu {cmd.upper()} <b>{cookies_saved}/{total_cookies}</b> thành công | -{total_price:,}đ | Còn: <b>{real_balance:,}đ</b>"

        tg_send(chat_id, msg_text, build_main_keyboard(is_active=True))
        return

    # ----- VOUCHER ĐƠN -----
    if cmd.startswith("voucher"):
        if not cookie_text:
            if user_id in PENDING_VOUCHER:
                old_pending = PENDING_VOUCHER[user_id]
                old_cmd = old_pending["cmd"] if isinstance(old_pending, dict) else old_pending
                dprint(f"Cleared old pending: {old_cmd}")

            # ✅ Lưu với timestamp
            PENDING_VOUCHER[user_id] = {
                "cmd": cmd,
                "ts": time.time()
            }
            
            tg_send(
                chat_id,
                f"👉 Gửi <b>cookie</b> để lưu {cmd}\n\n"
                f"⭐ <b>Hỗ trợ lưu tối đa 10 cookie</b>\n"
                f"💡 Gửi mỗi cookie 1 dòng"
            )
            return

        cookies = parse_cookies(cookie_text)

        if not cookies:
            tg_send(chat_id, "❌ Không tìm thấy cookie hợp lệ")
            return

        num_cookies = len(cookies)

        # ✅ Đọc balance từ PostgreSQL
        exists, balance, status = get_user_data(user_id)
        if not exists:
            tg_send(chat_id, "❌ Không tìm thấy ID")
            return

        dprint(f"💰 Balance: {balance:,}đ")

        v, err = get_voucher(cmd)
        if err:
            tg_send(chat_id, f"❌ {err}")
            # ✅ KHÔNG track_error - lỗi nghiệp vụ
            return

        price = int(v.get("Giá", 0))
        total_price = price * num_cookies

        # ✅ ATOMIC DEDUCT - Trừ tiền TRƯỚC
        success, new_bal = deduct_balance_atomic(user_id, total_price)
        
        if not success:
            tg_send(
                chat_id,
                f"❌ Không đủ số dư\n"
                f"💰 Cần: {total_price:,}đ\n"
                f"💼 Số dư hiện tại: {new_bal:,}đ"
            )
            # ✅ KHÔNG track_error - lỗi nghiệp vụ
            return

        # ✅ ĐÃ TRỪ TIỀN - Bây giờ lưu voucher
        success_count, total_count, failed_details = save_voucher_multi_cookies(cookies, v)

        if success_count == 0:
            # ✅ HOÀN TIỀN ATOMIC
            update_balance_atomic(user_id, total_price)  # ← ATOMIC
            
            # UI: Hiển thị balance TRỰC TIẾP từ Sheet
            real_balance = get_balance_direct(user_id)
            
            tg_send(
                chat_id,
                f"❌ Không lưu được cookie nào\n"
                f"💸 Đã hoàn tiền: +{total_price:,}đ\n"
                f"💰 Số dư hiện tại: <b>{real_balance:,}đ</b>"
            )
            # ✅ KHÔNG track_error - lỗi nghiệp vụ
            return

        # ✅ Hoàn tiền ATOMIC cho cookie thất bại
        actual_price = price * success_count
        if success_count < num_cookies:
            refund = price * (num_cookies - success_count)
            update_balance_atomic(user_id, refund)  # ← ATOMIC

        log_row(user_id, username, "VOUCHER", str(actual_price), f"Lưu {cmd} {success_count}/{total_count} thành công")

        # ✅ UI: Luôn hiển thị balance TRỰC TIẾP từ Sheet
        real_balance = get_balance_direct(user_id)
        
        if success_count == total_count:
            msg_text = f"✅ Lưu <b>{success_count}/{total_count}</b> thành công | -{actual_price:,}đ | Còn: <b>{real_balance:,}đ</b>"
        else:
            msg_text = f"⚠️ Lưu <b>{success_count}/{total_count}</b> thành công | -{actual_price:,}đ | Còn: <b>{real_balance:,}đ</b>"

        tg_send(chat_id, msg_text, build_main_keyboard(is_active=True))
        return

    # ===== FALLBACK =====
    tg_send(
        chat_id,
        "❌ <b>Lệnh không hợp lệ</b>\nDùng /start để xem menu.",
        build_main_keyboard(is_active=True)
    )

# =========================================================
# SEPAY WEBHOOK
# =========================================================
@app.route("/webhook-sepay", methods=["POST", "GET"])
def webhook_sepay():
    if request.method == "GET":
        return "OK", 200

    data = request.get_json(force=True, silent=True) or {}
    if not data:
        return "EMPTY", 200

    tx_id = str(
        data.get("id")
        or data.get("transaction_id")
        or data.get("tx_id")
        or data.get("referenceCode")
        or ""
    ).strip()

    try:
        amount = int(
            data.get("transferAmount")
            or data.get("amount")
            or data.get("amount_in")
            or 0
        )
    except Exception:
        amount = 0

    desc = " ".join([
        str(data.get("content") or ""),
        str(data.get("description") or ""),
        str(data.get("remark") or ""),
        str(data.get("note") or "")
    ]).strip()

    if not tx_id or amount <= 0:
        print("[SEPAY] INVALID DATA:", data)
        return "INVALID", 200

    if is_tx_exists(tx_id):
        print("[SEPAY] DUPLICATE TX:", tx_id)
        return "DUPLICATE", 200

    m = re.search(r"(?:SEVQR\s*)?NAP\s*(\d{6,})", desc, re.I)
    if not m:
        print("[SEPAY] NO USER FOUND | DESC =", desc)
        return "NO_USER", 200

    user_id = int(m.group(1))

    if amount < MIN_TOPUP_AMOUNT:
        tg_send(
            user_id,
            f"❌ <b>Nạp tối thiểu {MIN_TOPUP_AMOUNT:,}đ</b>"
        )
        return "TOO_SMALL", 200

    percent, bonus = calc_topup_bonus(amount)
    total_add = amount + bonus

    ensure_user_exists(user_id, "")
    
    # ✅ ATOMIC UPDATE - An toàn với concurrent webhooks
    new_balance = update_balance_atomic(user_id, total_add)

    note = f"+{int(percent * 100)}%={bonus}" if bonus > 0 else ""

    save_topup_to_sheet(
        user_id=user_id,
        username="",
        amount=amount,
        loai="SEPAY",
        tx_id=tx_id,
        note=note
    )

    log_row(user_id, "", "TOPUP_SEPAY", str(total_add), tx_id)

    # ✅ Đọc balance từ PG để hiển thị cho user
    real_balance = get_balance_direct(user_id)
    
    msg = (
        "💰 <b>NẠP TIỀN THÀNH CÔNG</b>\n"
        f"➕ Gốc: <b>{amount:,}đ</b>\n"
    )

    if bonus > 0:
        msg += f"🎁 Thưởng: <b>{bonus:,}đ</b>\n"

    msg += f"💼 Số dư: <b>{real_balance:,}đ</b>"

    tg_send(user_id, msg)

    return "OK", 200

# =========================================================
# TELEGRAM WEBHOOK
# =========================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    update = request.get_json(force=True)
    handle_update(update)
    return "ok"

@app.route("/", methods=["GET"])
def home():
    pg_ok = PG_POOL is not None
    sheet_status = "Sheet OK" if SHEET_READY else "Sheet DOWN (non-critical)"
    pg_status = "PG OK" if pg_ok else "PG DOWN (CRITICAL)"
    return f"Bot is running V6 | {pg_status} | {sheet_status}", 200 if pg_ok else 503

# =========================================================
# 🛠️ TOOL API — PC Tool đọc/ghi ví qua HTTP
# Bảo vệ bằng header X-Tool-Key
# =========================================================
TOOL_API_KEY = os.getenv("TOOL_API_KEY", "").strip()

def _tool_auth():
    """Verify X-Tool-Key. Returns (True, None) hoặc (False, error_response)"""
    if not TOOL_API_KEY:
        return False, ({"ok": False, "error": "TOOL_API_KEY not configured on server"}, 500)
    received = request.headers.get("X-Tool-Key", "").strip()
    if received != TOOL_API_KEY:
        print(f"[TOOL AUTH FAIL] received='{received}' ({len(received)}ch) expect='{TOOL_API_KEY}' ({len(TOOL_API_KEY)}ch) path={request.path}")
        return False, ({"ok": False, "error": f"Unauthorized — server key {len(TOOL_API_KEY)}ch, received {len(received)}ch"}, 401)
    return True, None

@app.route("/tool/debug", methods=["GET"])
def tool_debug():
    """Temp debug — xem key server đang hold"""
    k = TOOL_API_KEY
    return {
        "tool_api_key_len": len(k),
        "tool_api_key_first3": k[:3],
        "tool_api_key_last2": k[-2:] if len(k) >= 2 else k,
        "tool_api_key_repr": repr(k)
    }, 200


@app.route("/tool/vouchers", methods=["GET"])
def tool_get_vouchers():
    """
    GET /tool/vouchers
    → [{"source","code_name","code","price","status","promotion_id","signature"}, ...]
    Không cần pass — voucher list là public.
    """
    auth_ok, auth_err = _tool_auth()
    if not auth_ok:
        return auth_err

    rows = get_voucher_stock_cached()
    if not rows:
        return {"ok": True, "vouchers": []}, 200

    # Lấy thongbao từ dòng đầu có giá trị
    thongbao = ""
    for row in rows:
        def _get_tb(*keys):
            for k in keys:
                for rk in row:
                    if str(rk).strip().lower() == k.lower():
                        v = row[rk]
                        return str(v).strip() if v is not None else ""
            return ""
        tb = _get_tb("thongbao")
        if tb:
            thongbao = tb
            break

    # Normalize header keys (get_all_records trả dict với key = header text)
    items = []
    for row in rows:
        # Tìm các field linh hoạt giống tool cũ
        def _get(*keys):
            for k in keys:
                for rk in row:
                    if str(rk).strip().lower() == k.lower():
                        v = row[rk]
                        return str(v).strip() if v is not None else ""
            return ""

        code        = _get("code", "code_name", "voucher_code")
        display     = _get("display_name", "display name", "ten_ma", "tên mã", "ten ma")
        source      = _get("source", "nguon", "nguồn", "STT")
        price_str   = _get("price", "cost", "gia", "giá")
        status      = _get("status", "trang_thai", "trạng thái")
        promo_id    = _get("promotion_id", "promotionid")
        signature   = _get("signature", "chữ ký", "chu ky")

        if not code:
            continue

        try:
            price = int(price_str.replace(",", "")) if price_str else 1000
        except (ValueError, TypeError):
            price = 1000

        try:
            promo_id_int = int(promo_id) if promo_id else 0
        except (ValueError, TypeError):
            promo_id_int = 0

        items.append({
            "source":        source or "Sheet",
            "code_name":     display or code,
            "code":          code,
            "price":         price,
            "status":        status or "Sẵn sàng",
            "promotion_id":  promo_id_int,
            "signature":     signature
        })

    return {"ok": True, "vouchers": items, "thongbao": thongbao}, 200



@app.route("/tool/wallet", methods=["GET"])
def tool_get_wallet():
    """
    GET /tool/wallet?tele_id=123&pass=abc
    → {"ok": true, "balance": 5000, "username": "xxx"}
    """
    auth_ok, auth_err = _tool_auth()
    if not auth_ok:
        return auth_err

    tele_id  = request.args.get("tele_id", "").strip()
    password = request.args.get("pass", "").strip()

    if not tele_id:
        return {"ok": False, "error": "tele_id required"}, 400
    if PG_POOL is None:
        return {"ok": False, "error": "DB not ready"}, 503

    try:
        tele_id = int(tele_id)
    except ValueError:
        return {"ok": False, "error": "tele_id must be numeric"}, 400

    row = pg_exec(
        "SELECT username, balance, status, pass FROM wallet WHERE tele_id=%s",
        (tele_id,), fetchone=True
    )
    if not row:
        return {"ok": False, "error": "User not found"}, 404

    username, balance, status, stored_pass = row
    status_lower = (status or "").strip().lower()

    # Ban check
    if status_lower in ("banned", "banned_qr_spam", "ban_1h"):
        return {"ok": False, "error": "Account is banned"}, 403

    # Password: nếu DB có pass → phải match. Chưa set pass → bỏ qua.
    if stored_pass:
        if password != stored_pass:
            return {"ok": False, "error": "Wrong password"}, 401

    dprint(f"🛠️ TOOL GET WALLET: tele_id={tele_id} balance={balance}")
    return {"ok": True, "balance": int(balance or 0), "username": username or ""}, 200


@app.route("/tool/deduct", methods=["POST"])
def tool_deduct():
    """
    POST /tool/deduct  body: {"tele_id": 123, "pass": "abc", "amount": 5000}
    → {"ok": true, "balance": 3000}
    Atomic: WHERE balance >= amount → không race condition.
    """
    auth_ok, auth_err = _tool_auth()
    if not auth_ok:
        return auth_err

    body     = request.get_json(silent=True) or {}
    tele_id  = str(body.get("tele_id", "")).strip()
    password = str(body.get("pass", "")).strip()
    amount   = body.get("amount", 0)

    if not tele_id:
        return {"ok": False, "error": "tele_id required"}, 400
    if not amount or int(amount) <= 0:
        return {"ok": False, "error": "amount must be > 0"}, 400
    if PG_POOL is None:
        return {"ok": False, "error": "DB not ready"}, 503

    try:
        tele_id = int(tele_id)
        amount  = int(amount)
    except (ValueError, TypeError):
        return {"ok": False, "error": "Invalid tele_id or amount"}, 400

    # Read current state
    row = pg_exec(
        "SELECT balance, status, pass FROM wallet WHERE tele_id=%s",
        (tele_id,), fetchone=True
    )
    if not row:
        return {"ok": False, "error": "User not found"}, 404

    balance, status, stored_pass = row
    balance      = int(balance or 0)
    status_lower = (status or "").strip().lower()

    # Ban check
    if status_lower in ("banned", "banned_qr_spam", "ban_1h"):
        return {"ok": False, "error": "Account is banned"}, 403

    # Password check
    if stored_pass and password != stored_pass:
        return {"ok": False, "error": "Wrong password"}, 401

    # Balance check
    if balance < amount:
        return {"ok": False, "error": "Insufficient balance", "balance": balance}, 400

    # Atomic deduct — WHERE balance >= amount chống race condition
    result = pg_exec(
        "UPDATE wallet SET balance = balance - %s, updated_at = NOW() "
        "WHERE tele_id=%s AND balance >= %s RETURNING balance",
        (amount, tele_id, amount), fetchone=True
    )
    if not result:
        return {"ok": False, "error": "Deduct failed (concurrent request?)"}, 500

    new_balance = int(result[0])

    # Mirror Sheet (fire-and-forget)
    if SHEET_READY:
        try:
            row_num = get_user_row(tele_id)
            if row_num:
                ws_money.update_cell(row_num, 3, str(new_balance))
        except Exception:
            pass

    dprint(f"🛠️ TOOL DEDUCT: tele_id={tele_id} amount={amount} new_balance={new_balance}")
    return {"ok": True, "balance": new_balance}, 200


@app.route("/tool/log", methods=["POST"])
def tool_log():
    """
    POST /tool/log
    body: {"tele_id": 123, "username": "xxx", "voucher_name": "voucher100", "success": 2, "total": 2, "price": 2000, "balance_after": 96000}
    → ghi 1 dòng vào Sheet Logs
    """
    auth_ok, auth_err = _tool_auth()
    if not auth_ok:
        return auth_err

    body = request.get_json(silent=True) or {}
    tele_id      = str(body.get("tele_id", ""))
    username     = str(body.get("username", ""))
    voucher_name = str(body.get("voucher_name", ""))
    success      = int(body.get("success", 0))
    total        = int(body.get("total", 0))
    price        = int(body.get("price", 0))
    balance_after= int(body.get("balance_after", 0))

    if not SHEET_READY:
        return {"ok": False, "error": "Sheet not ready"}, 503

    try:
        # Format: "Tool PC : Lưu voucher100 2/2 thành công"
        note = f"Tool PC : Lưu {voucher_name} {success}/{total} thành công"

        ws_log.append_row([
            now_str(),
            tele_id,
            username,
            "VOUCHER",
            str(price),
            note
        ])
        dprint(f"🛠️ TOOL LOG: {note} | tele_id={tele_id}")
        return {"ok": True}, 200
    except Exception as e:
        dprint(f"🛠️ TOOL LOG error: {e}")
        return {"ok": False, "error": str(e)}, 500


# =========================================================
# LOCAL RUNNER
# =========================================================
if __name__ == "__main__":
    print("=" * 60)
    print(" NgânMiu.Store Telegram Bot")
    print(" V7 - PG PRIMARY | Ban→status | Pass Tool PC")
    print("=" * 60)
    print("ADMIN_ID:", ADMIN_ID)
    print("SHEET_READY:", SHEET_READY)
    print("MAX_COOKIES_PER_REQUEST:", MAX_COOKIES_PER_REQUEST)
    print("CACHE ENABLED: ROW_CACHE + BROADCAST_CACHE")
    print("=" * 60)

    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
