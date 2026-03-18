import hashlib
import math
import os
import secrets
import threading
import time
import traceback
import urllib.request
from datetime import date
from functools import wraps

import psycopg
from psycopg.rows import dict_row
from flask import (
    Flask, request, jsonify, render_template,
    session, redirect, url_for, abort
)
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    PostbackEvent, FlexSendMessage
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN', '')
LINE_CHANNEL_SECRET       = os.environ.get('LINE_CHANNEL_SECRET', '')
ADMIN_PASSWORD            = os.environ.get('ADMIN_PASSWORD', 'admin123')
_raw_db_url               = os.environ.get('DATABASE_URL', '')
# Render issues postgres:// scheme — psycopg3 requires postgresql://
DATABASE_URL              = _raw_db_url.replace('postgres://', 'postgresql://', 1) if _raw_db_url.startswith('postgres://') else _raw_db_url
# Render automatically injects RENDER_EXTERNAL_URL as https://<n>.onrender.com
RENDER_EXTERNAL_URL       = os.environ.get('RENDER_EXTERNAL_URL', '')

print(f"[startup] DATABASE_URL prefix: {DATABASE_URL[:20] if DATABASE_URL else 'NOT SET'}")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler      = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── Field Definitions ────────────────────────────────────────────────────────

FIELDS = [
    {'key': 'breakfast_total',    'label': '早餐點收',        'subtract': False},
    {'key': 'breakfast_cash',     'label': '早餐餐-現金',      'subtract': False},
    {'key': 'breakfast_card',     'label': '早餐餐-刷卡合庫',  'subtract': False},
    {'key': 'breakfast_linepay',  'label': '早餐餐-LINE Pay',  'subtract': False},
    {'key': 'breakfast_transfer', 'label': '早餐餐-轉帳',      'subtract': False},
    {'key': 'counter_expense',    'label': '櫃檯支出',         'subtract': True},
    {'key': 'panda',              'label': '熊貓',             'subtract': False},
    {'key': 'ubereats',           'label': 'Uber Eats',        'subtract': False},
    {'key': 'tips',               'label': '小費',             'subtract': False},
    {'key': 'surplus',            'label': '溢收',             'subtract': False},
    {'key': 'pos_total',          'label': 'POS機總額',        'subtract': False},
]

FIELD_KEYS = [f['key'] for f in FIELDS]
FIELD_MAP  = {f['key']: f for f in FIELDS}

# ─── PostgreSQL ───────────────────────────────────────────────────────────────

def get_db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    if not DATABASE_URL:
        print("[WARNING] DATABASE_URL not set — skipping init_db()")
        return
    try:
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS records (
                    id                  SERIAL PRIMARY KEY,
                    record_date         DATE NOT NULL UNIQUE,
                    breakfast_total     NUMERIC(12,2) DEFAULT 0,
                    breakfast_cash      NUMERIC(12,2) DEFAULT 0,
                    breakfast_card      NUMERIC(12,2) DEFAULT 0,
                    breakfast_linepay   NUMERIC(12,2) DEFAULT 0,
                    breakfast_transfer  NUMERIC(12,2) DEFAULT 0,
                    counter_expense     NUMERIC(12,2) DEFAULT 0,
                    panda               NUMERIC(12,2) DEFAULT 0,
                    ubereats            NUMERIC(12,2) DEFAULT 0,
                    tips                NUMERIC(12,2) DEFAULT 0,
                    surplus             NUMERIC(12,2) DEFAULT 0,
                    pos_total           NUMERIC(12,2) DEFAULT 0,
                    total_income        NUMERIC(12,2) DEFAULT 0,
                    created_at          TIMESTAMPTZ DEFAULT NOW(),
                    updated_at          TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ocr_records (
                    id          SERIAL PRIMARY KEY,
                    filename    TEXT,
                    items       JSONB DEFAULT '[]',
                    total       NUMERIC(12,2) DEFAULT 0,
                    scanned_at  TIMESTAMPTZ DEFAULT NOW(),
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inv_items (
                    id               SERIAL PRIMARY KEY,
                    code             TEXT UNIQUE NOT NULL,
                    name             TEXT UNIQUE NOT NULL,
                    category         TEXT DEFAULT '',
                    unit_label       TEXT DEFAULT '單位',
                    unit_size        NUMERIC(10,2) DEFAULT 1,
                    unit_piece_label TEXT DEFAULT '',
                    unit_cost        NUMERIC(12,2) DEFAULT 0,
                    min_stock        NUMERIC(10,2) DEFAULT 0,
                    vendor           TEXT DEFAULT '',
                    current_stock    NUMERIC(10,2) DEFAULT 0,
                    created_at       TIMESTAMPTZ DEFAULT NOW(),
                    updated_at       TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inv_transactions (
                    id          SERIAL PRIMARY KEY,
                    item_id     INT REFERENCES inv_items(id) ON DELETE CASCADE,
                    txn_type    TEXT NOT NULL,
                    quantity    NUMERIC(10,2) NOT NULL,
                    note        TEXT DEFAULT '',
                    staff       TEXT DEFAULT '',
                    recipe_id   INT,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inv_recipes (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    batch_yield INT DEFAULT 1,
                    created_at  TIMESTAMPTZ DEFAULT NOW(),
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inv_recipe_items (
                    recipe_id   INT REFERENCES inv_recipes(id) ON DELETE CASCADE,
                    item_id     INT REFERENCES inv_items(id) ON DELETE CASCADE,
                    quantity    NUMERIC(10,2) NOT NULL DEFAULT 1,
                    PRIMARY KEY (recipe_id, item_id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inv_orders (
                    id          SERIAL PRIMARY KEY,
                    item_id     INT REFERENCES inv_items(id) ON DELETE SET NULL,
                    item_name   TEXT NOT NULL,
                    vendor      TEXT DEFAULT '',
                    quantity    TEXT DEFAULT '',
                    note        TEXT DEFAULT '',
                    staff       TEXT DEFAULT '',
                    status      TEXT DEFAULT 'pending',
                    ordered_at  TIMESTAMPTZ,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inv_settlements (
                    id           SERIAL PRIMARY KEY,
                    month        TEXT NOT NULL,
                    settled_by   TEXT DEFAULT '',
                    note         TEXT DEFAULT '',
                    snapshot     JSONB DEFAULT '[]',
                    settled_at   TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS punch_staff (
                    id              SERIAL PRIMARY KEY,
                    name            TEXT NOT NULL UNIQUE,
                    username        TEXT UNIQUE,
                    password_hash   TEXT DEFAULT '',
                    role            TEXT DEFAULT '',
                    active          BOOLEAN DEFAULT TRUE,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS punch_records (
                    id            SERIAL PRIMARY KEY,
                    staff_id      INT REFERENCES punch_staff(id) ON DELETE CASCADE,
                    punch_type    TEXT NOT NULL,
                    punched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    note          TEXT DEFAULT '',
                    is_manual     BOOLEAN DEFAULT FALSE,
                    manual_by     TEXT DEFAULT '',
                    latitude      NUMERIC(10,6),
                    longitude     NUMERIC(10,6),
                    gps_distance  INT,
                    created_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS punch_settings (
                    id          INT PRIMARY KEY DEFAULT 1,
                    lat         NUMERIC(10,6),
                    lng         NUMERIC(10,6),
                    radius_m    INT DEFAULT 100,
                    location_name TEXT DEFAULT '',
                    gps_required BOOLEAN DEFAULT FALSE,
                    updated_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Ensure default settings row exists
            conn.execute("""
                INSERT INTO punch_settings (id, gps_required)
                VALUES (1, FALSE)
                ON CONFLICT (id) DO NOTHING
            """)
        print("[OK] Database tables created")
    except Exception as e:
        print(f"[ERROR] init_db failed: {e}")
        raise

    # ── Schema migrations — each in its OWN connection/transaction ──
    # PostgreSQL aborts the entire transaction on error, so we MUST
    # run each ALTER TABLE in a separate connection & transaction.
    migrations = [
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS username TEXT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS password_hash TEXT DEFAULT ''",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS latitude NUMERIC(10,6)",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS longitude NUMERIC(10,6)",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS gps_distance INT",
    ]
    for sql in migrations:
        try:
            with get_db() as mc:
                mc.execute(sql)
            print(f"[MIGRATION OK] {sql[:70]}")
        except Exception as me:
            print(f"[MIGRATION SKIP] {sql[:70]}: {me}")

    print("[OK] Database initialised")


def row_to_dict(row):
    if not row:
        return None
    d = dict(row)
    if isinstance(d.get('record_date'), date):
        d['record_date'] = d['record_date'].isoformat()
    for k in FIELD_KEYS + ['total_income']:
        if k in d and d[k] is not None:
            d[k] = float(d[k])
    return d


def calculate_total(data: dict) -> float:
    total = 0.0
    for f in FIELDS:
        val = float(data.get(f['key']) or 0)
        total += -val if f['subtract'] else val
    return total


init_db()

# ─── User State with TTL ──────────────────────────────────────────────────────
# Stored as { uid: { 'step': ..., 'date': ..., 'field': ..., 'pending': {...}, 'ts': time() } }
# 'pending' holds unsaved field values until user presses confirm.
# Expires after 30 minutes so stale states don't block users after restarts.

_user_states: dict = {}
STATE_TTL = 1800  # 30 minutes


def get_state(uid: str) -> dict:
    entry = _user_states.get(uid)
    if not entry:
        return {}
    if time.time() - entry.get('ts', 0) > STATE_TTL:
        _user_states.pop(uid, None)
        return {}
    return entry


def set_state(uid: str, data: dict):
    data['ts'] = time.time()
    _user_states[uid] = data


def clear_state(uid: str):
    _user_states.pop(uid, None)


def get_pending(uid: str, record_date: str) -> dict:
    """Return pending (unsaved) field values for this user+date."""
    state = get_state(uid)
    if state.get('pending_date') == record_date:
        return state.get('pending', {})
    return {}


def set_pending(uid: str, record_date: str, field_key: str, amount: float):
    """Store a field value in pending without writing to DB."""
    state = get_state(uid)
    if state.get('pending_date') != record_date:
        state = {}
    pending = state.get('pending', {})
    pending[field_key] = amount
    set_state(uid, {**state, 'pending_date': record_date, 'pending': pending})


def clear_pending(uid: str):
    state = get_state(uid)
    state.pop('pending', None)
    state.pop('pending_date', None)
    if state:
        set_state(uid, state)


def merge_pending_to_record(pending: dict, record: dict) -> dict:
    """Overlay pending values onto the saved DB record for display."""
    merged = dict(record or {})
    merged.update(pending)
    return merged

# ─── Flex Builders ────────────────────────────────────────────────────────────

def make_start_flex():
    return {
        "type": "bubble", "size": "kilo",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1a2744", "paddingAll": "20px",
            "contents": [
                {"type": "text", "text": "財務記帳系統", "color": "#ffffff",
                 "size": "xl", "weight": "bold"},
                {"type": "text", "text": "請選擇要記帳的日期", "color": "#9eb3d8",
                 "size": "sm", "margin": "sm"}
            ]
        },
        "body": {
            "type": "box", "layout": "vertical", "paddingAll": "20px",
            "contents": [{
                "type": "button",
                "action": {"type": "datetimepicker", "label": "選擇日期",
                           "data": "action=select_date", "mode": "date"},
                "style": "primary", "color": "#1a2744", "height": "sm"
            }]
        }
    }


def make_field_flex(record_date, record=None):
    record = record or {}

    def val_text(key):
        v = record.get(key, 0) or 0
        return f"${int(float(v)):,}" if v else "未填"

    rows = []
    for i in range(0, len(FIELDS), 2):
        pair = FIELDS[i:i+2]
        cols = []
        for f in pair:
            cols.append({
                "type": "box", "layout": "vertical", "flex": 1, "spacing": "xs",
                "contents": [
                    {"type": "text", "text": f['label'], "size": "xs",
                     "color": "#666666", "wrap": True},
                    {"type": "text", "text": val_text(f['key']), "size": "sm",
                     "weight": "bold",
                     "color": "#c0392b" if f['subtract'] else "#1a2744"},
                    {"type": "button", "action": {
                        "type": "postback", "label": "輸入",
                        "data": f"action=input_field&date={record_date}&field={f['key']}"
                    }, "style": "secondary", "height": "sm", "color": "#f0f4ff"}
                ]
            })
        if len(pair) == 1:
            cols.append({"type": "box", "layout": "vertical", "flex": 1, "contents": []})
        rows.append({
            "type": "box", "layout": "horizontal",
            "spacing": "md", "margin": "md", "contents": cols
        })

    total = calculate_total(record)
    # Count how many fields have been filled
    filled = sum(1 for f in FIELDS if float(record.get(f['key']) or 0) != 0)
    confirm_label = f"確認送出（已填 {filled} 項）" if filled > 0 else "確認送出"
    rows += [
        {"type": "separator", "margin": "lg"},
        {"type": "box", "layout": "horizontal", "margin": "lg", "contents": [
            {"type": "text", "text": "當日總收入", "size": "md", "weight": "bold",
             "color": "#333333", "flex": 1},
            {"type": "text", "text": f"${int(total):,}", "size": "xl", "weight": "bold",
             "color": "#1a2744", "align": "end", "flex": 1}
        ]},
        {"type": "button", "margin": "lg",
         "action": {"type": "postback", "label": confirm_label,
                    "data": f"action=confirm&date={record_date}"},
         "style": "primary", "color": "#1a2744", "height": "sm"}
    ]
    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1a2744", "paddingAll": "18px",
            "contents": [
                {"type": "text", "text": record_date, "color": "#ffffff",
                 "size": "lg", "weight": "bold"},
                {"type": "text", "text": "點選欄位輸入金額", "color": "#9eb3d8",
                 "size": "xs", "margin": "xs"}
            ]
        },
        "body": {"type": "box", "layout": "vertical", "paddingAll": "16px", "contents": rows}
    }


def make_confirm_flex(record_date, field_label, amount, record=None):
    total = calculate_total(record or {})
    return {
        "type": "bubble", "size": "kilo",
        "body": {
            "type": "box", "layout": "vertical", "paddingAll": "20px",
            "contents": [
                {"type": "text", "text": "記帳成功", "weight": "bold",
                 "size": "lg", "color": "#1a2744"},
                {"type": "separator", "margin": "md"},
                {"type": "box", "layout": "horizontal", "margin": "md", "contents": [
                    {"type": "text", "text": field_label, "size": "sm",
                     "color": "#666666", "flex": 1},
                    {"type": "text", "text": f"${int(amount):,}", "size": "sm",
                     "weight": "bold", "color": "#1a2744", "align": "end", "flex": 1}
                ]},
                {"type": "box", "layout": "horizontal", "margin": "sm", "contents": [
                    {"type": "text", "text": "當日總收入", "size": "sm",
                     "color": "#666666", "flex": 1},
                    {"type": "text", "text": f"${int(total):,}", "size": "sm",
                     "weight": "bold", "color": "#1a2744", "align": "end", "flex": 1}
                ]},
                {"type": "separator", "margin": "md"},
                {"type": "button", "margin": "md",
                 "action": {"type": "postback", "label": "繼續記帳",
                            "data": f"action=continue&date={record_date}"},
                 "style": "secondary", "height": "sm"},
                {"type": "button", "margin": "sm",
                 "action": {"type": "postback", "label": "完成",
                            "data": f"action=done&date={record_date}"},
                 "style": "primary", "color": "#1a2744", "height": "sm"}
            ]
        }
    }


def make_summary_flex(record_date, record):
    total = calculate_total(record)
    rows = []
    for f in FIELDS:
        val = float(record.get(f['key']) or 0)
        if val == 0:
            continue
        rows.append({
            "type": "box", "layout": "horizontal", "margin": "sm",
            "contents": [
                {"type": "text", "text": f['label'], "size": "sm",
                 "color": "#666666", "flex": 2},
                {"type": "text",
                 "text": f"-${int(val):,}" if f['subtract'] else f"${int(val):,}",
                 "size": "sm", "weight": "bold",
                 "color": "#c0392b" if f['subtract'] else "#2c3e50",
                 "align": "end", "flex": 1}
            ]
        })
    if not rows:
        rows.append({"type": "text", "text": "尚無資料",
                     "size": "sm", "color": "#aaaaaa", "align": "center"})
    rows += [
        {"type": "separator", "margin": "lg"},
        {"type": "box", "layout": "horizontal", "margin": "lg", "contents": [
            {"type": "text", "text": "總收入", "size": "lg", "weight": "bold", "flex": 1},
            {"type": "text", "text": f"${int(total):,}", "size": "lg", "weight": "bold",
             "color": "#1a2744", "align": "end", "flex": 1}
        ]},
        {"type": "separator", "margin": "md"},
        {"type": "button", "margin": "md",
         "action": {"type": "postback", "label": "繼續記帳",
                    "data": f"action=continue&date={record_date}"},
         "style": "secondary", "height": "sm"},
    ]
    return {
        "type": "bubble", "size": "kilo",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1a2744", "paddingAll": "18px",
            "contents": [{"type": "text", "text": f"{record_date} 總覽",
                          "color": "#ffffff", "size": "lg", "weight": "bold"}]
        },
        "body": {"type": "box", "layout": "vertical", "paddingAll": "16px", "contents": rows}
    }

# ─── DB Helpers ───────────────────────────────────────────────────────────────

def get_or_create_record(record_date):
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM records WHERE record_date=%s', (record_date,)
        ).fetchone()
        if not row:
            row = conn.execute(
                'INSERT INTO records (record_date) VALUES (%s) RETURNING *',
                (record_date,)
            ).fetchone()
    return row_to_dict(row)


def update_record_field(record_date, field_key, amount):
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM records WHERE record_date=%s', (record_date,)
        ).fetchone()
        if not row:
            row = conn.execute(
                'INSERT INTO records (record_date) VALUES (%s) RETURNING *',
                (record_date,)
            ).fetchone()
        record = row_to_dict(row)
        record[field_key] = float(amount)
        total = calculate_total(record)
        row = conn.execute(
            f'UPDATE records SET {field_key}=%s, total_income=%s, updated_at=NOW() '
            f'WHERE record_date=%s RETURNING *',
            (amount, total, record_date)
        ).fetchone()
    return row_to_dict(row)

# ─── Keep-Alive ───────────────────────────────────────────────────────────────

def keep_alive():
    """Ping /health every 14 min to prevent Render free-tier sleep."""
    time.sleep(10)  # short wait — just enough for gunicorn to bind
    while True:
        try:
            base = RENDER_EXTERNAL_URL.rstrip('/') if RENDER_EXTERNAL_URL else 'http://localhost:5000'
            urllib.request.urlopen(
                urllib.request.Request(
                    f'{base}/health',
                    headers={'User-Agent': 'KeepAlive/1.0'}
                ),
                timeout=10
            )
            print(f"[keep-alive] pinged {base}/health")
        except Exception as e:
            print(f"[keep-alive] ping failed: {e}")
        time.sleep(14 * 60)


threading.Thread(target=keep_alive, daemon=True).start()

# ─── Health Check ─────────────────────────────────────────────────────────────

@app.route('/health')
def health():
    try:
        with get_db() as conn:
            conn.execute('SELECT 1')
        return jsonify({'status': 'ok', 'db': 'connected'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'detail': str(e)}), 500

# ─── LINE Bot ─────────────────────────────────────────────────────────────────

@app.route('/webhook', methods=['POST'])
def webhook():
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    uid   = event.source.user_id
    text  = event.message.text.strip()
    state = get_state(uid)

    # User is in the middle of entering an amount
    if state.get('step') == 'input_amount':
        try:
            amount = float(text.replace(',', '').replace('$', ''))
            if amount < 0:
                raise ValueError
        except ValueError:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="請輸入有效的數字金額，例如：1500")
            )
            return

        record_date = state['date']
        field_key   = state['field']
        field_label = FIELD_MAP[field_key]['label']
        try:
            # Store in pending — don't write to DB yet
            set_pending(uid, record_date, field_key, amount)
            # Clear the input step but keep pending
            state2 = get_state(uid)
            state2.pop('step', None)
            state2.pop('field', None)
            set_state(uid, state2)

            # Build display record: saved DB values merged with pending
            db_record = get_or_create_record(record_date)
            pending   = get_pending(uid, record_date)
            display   = merge_pending_to_record(pending, db_record)

            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(
                    alt_text=f"{field_label} 已填 ${int(amount):,}，繼續填寫或確認送出",
                    contents=make_field_flex(record_date, display)
                )
            )
        except Exception as e:
            print(f"[ERROR] handle input: {e}\n{traceback.format_exc()}")
            clear_state(uid)
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="操作失敗，請重新操作")
            )
        return

    # Default: show start menu with date picker
    line_bot_api.reply_message(
        event.reply_token,
        FlexSendMessage(alt_text="財務記帳系統", contents=make_start_flex())
    )


@handler.add(PostbackEvent)
def handle_postback(event):
    uid    = event.source.user_id
    data   = dict(p.split('=', 1) for p in event.postback.data.split('&') if '=' in p)
    action = data.get('action')

    try:
        if action == 'select_date':
            params = event.postback.params or {}
            record_date = params.get('date') or str(date.today())
            # Clear any stale pending from a different date
            state = get_state(uid)
            if state.get('pending_date') != record_date:
                clear_pending(uid)
            db_record = get_or_create_record(record_date)
            pending   = get_pending(uid, record_date)
            display   = merge_pending_to_record(pending, db_record)
            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=f"{record_date} 記帳",
                                contents=make_field_flex(record_date, display))
            )

        elif action == 'input_field':
            record_date = data.get('date')
            field_key   = data.get('field')
            if not record_date or field_key not in FIELD_MAP:
                raise ValueError(f"invalid field or date: {data}")
            # Preserve existing pending when setting input step
            state = get_state(uid)
            set_state(uid, {**state, 'step': 'input_amount', 'date': record_date, 'field': field_key})
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"請輸入【{FIELD_MAP[field_key]['label']}】的金額：")
            )

        elif action == 'continue':
            record_date = data.get('date')
            db_record = get_or_create_record(record_date)
            pending   = get_pending(uid, record_date)
            display   = merge_pending_to_record(pending, db_record)
            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=f"{record_date} 記帳",
                                contents=make_field_flex(record_date, display))
            )

        elif action == 'confirm':
            # Write all pending values to DB at once, then show summary
            record_date = data.get('date')
            db_record = get_or_create_record(record_date)
            pending   = get_pending(uid, record_date)
            if pending:
                # Merge pending into DB record and save everything
                merged = merge_pending_to_record(pending, db_record)
                with get_db() as conn:
                    set_clause = ', '.join([f'{k}=%s' for k in FIELD_KEYS])
                    vals  = [float(merged.get(k) or 0) for k in FIELD_KEYS]
                    total = calculate_total(merged)
                    row   = conn.execute(
                        f'UPDATE records SET {set_clause}, total_income=%s, updated_at=NOW() '
                        f'WHERE record_date=%s RETURNING *',
                        vals + [total, record_date]
                    ).fetchone()
                saved_record = row_to_dict(row)
                clear_pending(uid)
            else:
                saved_record = db_record

            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=f"{record_date} 記帳完成",
                                contents=make_summary_flex(record_date, saved_record))
            )

        elif action == 'done':
            # Legacy fallback — same as confirm
            record_date = data.get('date')
            db_record = get_or_create_record(record_date)
            pending   = get_pending(uid, record_date)
            display   = merge_pending_to_record(pending, db_record)
            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=f"{record_date} 記帳完成",
                                contents=make_summary_flex(record_date, display))
            )

    except Exception as e:
        print(f"[ERROR] handle_postback action={action}: {e}\n{traceback.format_exc()}")
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="操作失敗，請重新傳訊息開始")
            )
        except Exception:
            pass

# ─── Admin Auth ───────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        if request.form.get('password', '') == ADMIN_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        error = '密碼錯誤'
    return render_template('login.html', error=error)


@app.route('/admin/logout')
def admin_logout():
    session.clear()
    return redirect(url_for('admin_login'))


@app.route('/admin')
@app.route('/admin/')
@login_required
def admin_dashboard():
    return render_template('admin.html', fields=FIELDS)

# ─── Admin API ────────────────────────────────────────────────────────────────

@app.route('/api/records', methods=['GET'])
@login_required
def api_list_records():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM records ORDER BY record_date DESC'
        ).fetchall()
    return jsonify([row_to_dict(r) for r in rows])


@app.route('/api/records/<record_date>', methods=['GET'])
@login_required
def api_get_record(record_date):
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM records WHERE record_date=%s', (record_date,)
        ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(row_to_dict(row))


@app.route('/api/records', methods=['POST'])
@login_required
def api_create_record():
    body = request.get_json(force=True)
    record_date = body.get('record_date')
    if not record_date:
        return jsonify({'error': 'record_date required'}), 400

    vals  = {k: float(body.get(k) or 0) for k in FIELD_KEYS}
    total = calculate_total(vals)
    cols  = ', '.join(FIELD_KEYS)
    phs   = ', '.join(['%s'] * len(FIELD_KEYS))

    try:
        with get_db() as conn:
            row = conn.execute(
                f'INSERT INTO records (record_date, {cols}, total_income) '
                f'VALUES (%s, {phs}, %s) RETURNING *',
                [record_date] + [vals[k] for k in FIELD_KEYS] + [total]
            ).fetchone()
        return jsonify(row_to_dict(row)), 201
    except psycopg.errors.UniqueViolation:
        return jsonify({'error': '該日期已存在'}), 409


@app.route('/api/records/<record_date>', methods=['PUT'])
@login_required
def api_update_record(record_date):
    body  = request.get_json(force=True)
    vals  = {k: float(body.get(k) or 0) for k in FIELD_KEYS}
    total = calculate_total(vals)
    set_clause = ', '.join([f'{k}=%s' for k in FIELD_KEYS])

    with get_db() as conn:
        row = conn.execute(
            f'UPDATE records SET {set_clause}, total_income=%s, updated_at=NOW() '
            f'WHERE record_date=%s RETURNING *',
            [vals[k] for k in FIELD_KEYS] + [total, record_date]
        ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(row_to_dict(row))


@app.route('/api/records/<record_date>', methods=['DELETE'])
@login_required
def api_delete_record(record_date):
    with get_db() as conn:
        conn.execute('DELETE FROM records WHERE record_date=%s', (record_date,))
    return jsonify({'deleted': record_date})


@app.route('/api/summary', methods=['GET'])
@login_required
def api_summary():
    with get_db() as conn:
        row = conn.execute('''
            SELECT COUNT(*)                        AS days,
                   COALESCE(SUM(total_income), 0)  AS total,
                   COALESCE(AVG(total_income), 0)  AS avg_daily
            FROM records
        ''').fetchone()
    return jsonify({k: float(v) if v is not None else 0 for k, v in dict(row).items()})


@app.route('/api/ocr', methods=['POST'])
@login_required
def api_ocr():
    """
    Accept one or more images + an OpenAI API key.
    Returns: { results: [{ filename, items:[{name,qty,amount}], total, error }] }
    """
    import base64, json as _json, urllib.error

    openai_key = os.environ.get('OPENAI_API_KEY', '').strip()
    if not openai_key:
        return jsonify({'error': 'OPENAI_API_KEY 尚未設定，請至 Render 環境變數新增'}), 400

    files = request.files.getlist('files[]')
    if not files:
        return jsonify({'error': 'No files uploaded'}), 400

    SYSTEM_PROMPT = (
        "你是一個收據/點單辨識助手。"
        "請從圖片中擷取所有品項，回傳純 JSON（不要加 markdown 代碼區塊），格式如下：\n"
        '{"items":[{"name":"品名","qty":"數量","amount":金額數字}],"total":總金額數字}\n'
        "若某欄位無法辨識，qty 填 null，amount 填 0。"
        "total 為所有 amount 加總（負數折扣請保留負號）。"
        "只回傳 JSON，不要有任何其他文字。"
    )

    results = []
    for f in files:
        filename = f.filename or 'unknown'
        raw = f.read()
        b64 = base64.b64encode(raw).decode()
        mime = f.content_type or 'image/jpeg'

        payload = _json.dumps({
            "model": "gpt-4o",
            "max_tokens": 2000,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": SYSTEM_PROMPT},
                    {"type": "image_url", "image_url": {
                        "url": f"data:{mime};base64,{b64}",
                        "detail": "high"
                    }}
                ]
            }]
        }).encode('utf-8')

        req = urllib.request.Request(
            'https://api.openai.com/v1/chat/completions',
            data=payload,
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {openai_key}'
            }
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                resp_data = _json.loads(resp.read())
            content = resp_data['choices'][0]['message']['content'].strip()
            # Strip possible markdown fences
            if content.startswith('```'):
                content = content.split('\n', 1)[-1]
                content = content.rsplit('```', 1)[0].strip()
            parsed = _json.loads(content)
            results.append({
                'filename': filename,
                'items': parsed.get('items', []),
                'total': parsed.get('total', 0),
                'error': None
            })
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='replace')
            results.append({'filename': filename, 'items': [], 'total': 0,
                            'error': f'OpenAI HTTP {e.code}: {err_body[:200]}'})
        except Exception as e:
            results.append({'filename': filename, 'items': [], 'total': 0,
                            'error': str(e)})

    # Persist to DB
    import json as _json2
    saved_ids = []
    if DATABASE_URL:
        for r in results:
            try:
                items_json = _json2.dumps(r.get('items', []), ensure_ascii=False)
                with get_db() as conn:
                    row = conn.execute(
                        "INSERT INTO ocr_records (filename, items, total) VALUES (%s, %s::jsonb, %s) RETURNING id",
                        (r['filename'], items_json, float(r.get('total') or 0))
                    ).fetchone()
                r['db_id'] = row['id']
                saved_ids.append(row['id'])
            except Exception as e:
                r['save_error'] = str(e)

    return jsonify({'results': results, 'saved_ids': saved_ids})



# ─── OCR Records CRUD ─────────────────────────────────────────────────────────

def ocr_row_to_dict(row):
    import json as _j
    if not row:
        return None
    d = dict(row)
    if d.get('total') is not None:
        d['total'] = float(d['total'])
    if d.get('scanned_at'):
        d['scanned_at'] = d['scanned_at'].isoformat()
    if d.get('updated_at'):
        d['updated_at'] = d['updated_at'].isoformat()
    # items is already parsed by psycopg3 JSONB → Python list
    if isinstance(d.get('items'), str):
        try:
            d['items'] = _j.loads(d['items'])
        except Exception:
            d['items'] = []
    return d


@app.route('/api/ocr-records', methods=['GET'])
@login_required
def api_ocr_list():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM ocr_records ORDER BY scanned_at DESC'
        ).fetchall()
    return jsonify([ocr_row_to_dict(r) for r in rows])


@app.route('/api/ocr-records/<int:record_id>', methods=['GET'])
@login_required
def api_ocr_get(record_id):
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM ocr_records WHERE id=%s', (record_id,)
        ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(ocr_row_to_dict(row))


@app.route('/api/ocr-records/<int:record_id>', methods=['PUT'])
@login_required
def api_ocr_update(record_id):
    import json as _j
    body = request.get_json(force=True)
    items = body.get('items', [])
    # Recalculate total from items
    total = sum(float(it.get('amount') or 0) for it in items)
    filename = body.get('filename')

    items_json = _j.dumps(items, ensure_ascii=False)
    with get_db() as conn:
        row = conn.execute(
            'UPDATE ocr_records SET items=%s::jsonb, total=%s, filename=%s, updated_at=NOW() WHERE id=%s RETURNING *',
            (items_json, total, filename, record_id)
        ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(ocr_row_to_dict(row))


@app.route('/api/ocr-records/<int:record_id>', methods=['DELETE'])
@login_required
def api_ocr_delete(record_id):
    with get_db() as conn:
        conn.execute('DELETE FROM ocr_records WHERE id=%s', (record_id,))
    return jsonify({'deleted': record_id})



# ═══════════════════════════════════════════════════════════════════
# Inventory API
# ═══════════════════════════════════════════════════════════════════

def inv_item_row(row):
    if not row: return None
    d = dict(row)
    for f in ['unit_size','unit_cost','min_stock','current_stock']:
        if d.get(f) is not None: d[f] = float(d[f])
    for f in ['created_at','updated_at']:
        if d.get(f): d[f] = d[f].isoformat()
    return d

def inv_txn_row(row):
    if not row: return None
    d = dict(row)
    if d.get('quantity') is not None: d['quantity'] = float(d['quantity'])
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d

def inv_order_row(row):
    if not row: return None
    d = dict(row)
    for f in ['ordered_at','created_at']:
        if d.get(f): d[f] = d[f].isoformat()
    return d

# ── Items ──────────────────────────────────────────────────────────

@app.route('/api/inv/items', methods=['GET'])
@login_required
def api_inv_items_list():
    q = request.args.get('q','').strip()
    with get_db() as conn:
        if q:
            rows = conn.execute(
                "SELECT * FROM inv_items WHERE code ILIKE %s OR name ILIKE %s ORDER BY code",
                (f'%{q}%', f'%{q}%')
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM inv_items ORDER BY code").fetchall()
    return jsonify([inv_item_row(r) for r in rows])


@app.route('/api/inv/items', methods=['POST'])
@login_required
def api_inv_items_create():
    b = request.get_json(force=True)
    try:
        with get_db() as conn:
            row = conn.execute("""
                INSERT INTO inv_items (code,name,category,unit_label,unit_size,unit_piece_label,unit_cost,min_stock,vendor)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
            """, (b['code'],b['name'],b.get('category',''),
                  b.get('unit_label','單位'),float(b.get('unit_size') or 1),
                  b.get('unit_piece_label',''),float(b.get('unit_cost') or 0),
                  float(b.get('min_stock') or 0),b.get('vendor',''))
            ).fetchone()
        return jsonify(inv_item_row(row)), 201
    except psycopg.errors.UniqueViolation:
        return jsonify({'error': '品項代碼或名稱已存在'}), 409


@app.route('/api/inv/items/<int:item_id>', methods=['GET'])
@login_required
def api_inv_item_get(item_id):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM inv_items WHERE id=%s",(item_id,)).fetchone()
    return jsonify(inv_item_row(row)) if row else ('',404)


@app.route('/api/inv/items/<int:item_id>', methods=['PUT'])
@login_required
def api_inv_item_update(item_id):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE inv_items SET code=%s,name=%s,category=%s,unit_label=%s,unit_size=%s,
            unit_piece_label=%s,unit_cost=%s,min_stock=%s,vendor=%s,updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (b['code'],b['name'],b.get('category',''),
              b.get('unit_label','單位'),float(b.get('unit_size') or 1),
              b.get('unit_piece_label',''),float(b.get('unit_cost') or 0),
              float(b.get('min_stock') or 0),b.get('vendor',''),item_id)
        ).fetchone()
    return jsonify(inv_item_row(row)) if row else ('',404)


@app.route('/api/inv/items/<int:item_id>', methods=['DELETE'])
@login_required
def api_inv_item_delete(item_id):
    with get_db() as conn:
        conn.execute("DELETE FROM inv_items WHERE id=%s",(item_id,))
    return jsonify({'deleted': item_id})


@app.route('/api/inv/items/<int:item_id>/stock', methods=['POST'])
@login_required
def api_inv_stock_adjust(item_id):
    b = request.get_json(force=True)
    txn_type = b.get('txn_type','in')   # 'in' | 'out' | 'adjust'
    qty      = float(b.get('quantity') or 0)
    note     = b.get('note','')
    staff    = b.get('staff','')

    delta = qty if txn_type in ('in','adjust') else -qty
    with get_db() as conn:
        conn.execute(
            "UPDATE inv_items SET current_stock=current_stock+%s, updated_at=NOW() WHERE id=%s",
            (delta, item_id)
        )
        txn = conn.execute("""
            INSERT INTO inv_transactions (item_id,txn_type,quantity,note,staff)
            VALUES (%s,%s,%s,%s,%s) RETURNING *
        """, (item_id, txn_type, delta, note, staff)).fetchone()
    return jsonify(inv_txn_row(txn))

# ── Transactions ───────────────────────────────────────────────────

@app.route('/api/inv/transactions', methods=['GET'])
@login_required
def api_inv_txns():
    item_id   = request.args.get('item_id')
    month     = request.args.get('month')      # YYYY-MM
    date_from = request.args.get('date_from')  # YYYY-MM-DD
    date_to   = request.args.get('date_to')
    txn_type  = request.args.get('txn_type')

    conds  = ["TRUE"]
    params = []
    if item_id:
        conds.append("t.item_id = %s"); params.append(int(item_id))
    if month:
        conds.append("TO_CHAR(t.created_at, 'YYYY-MM') = %s"); params.append(month)
    elif date_from:
        conds.append("t.created_at::date >= %s"); params.append(date_from)
        if date_to:
            conds.append("t.created_at::date <= %s"); params.append(date_to)
    if txn_type:
        conds.append("t.txn_type = %s"); params.append(txn_type)

    where = " AND ".join(conds)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT t.*, i.name as item_name, i.unit_label
            FROM inv_transactions t JOIN inv_items i ON i.id=t.item_id
            WHERE {where}
            ORDER BY t.created_at DESC LIMIT 500
        """, params).fetchall()
    return jsonify([inv_txn_row(r) for r in rows])

# ── Recipes ────────────────────────────────────────────────────────

def recipe_with_items(conn, recipe_id):
    rec = conn.execute("SELECT * FROM inv_recipes WHERE id=%s",(recipe_id,)).fetchone()
    if not rec: return None
    d = dict(rec)
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    items = conn.execute("""
        SELECT ri.quantity, i.id as item_id, i.name, i.unit_label,
               i.current_stock, i.unit_cost, i.unit_size, i.unit_piece_label
        FROM inv_recipe_items ri JOIN inv_items i ON i.id=ri.item_id
        WHERE ri.recipe_id=%s ORDER BY i.name
    """, (recipe_id,)).fetchall()
    item_list = []
    total_batch_cost = 0.0
    for r in items:
        qty       = float(r['quantity'])
        unit_cost = float(r['unit_cost'])
        line_cost = qty * unit_cost
        total_batch_cost += line_cost
        item_list.append({
            'item_id':          r['item_id'],
            'name':             r['name'],
            'quantity':         qty,
            'unit_label':       r['unit_label'],
            'unit_cost':        unit_cost,
            'unit_size':        float(r['unit_size']),
            'unit_piece_label': r['unit_piece_label'],
            'current_stock':    float(r['current_stock']),
            'line_cost':        round(line_cost, 2),
        })
    batch_yield = int(d.get('batch_yield') or 1)
    d['items']            = item_list
    d['total_batch_cost'] = round(total_batch_cost, 2)
    d['cost_per_serving'] = round(total_batch_cost / batch_yield, 2) if batch_yield > 0 else 0
    return d


@app.route('/api/inv/recipes', methods=['GET'])
@login_required
def api_inv_recipes_list():
    with get_db() as conn:
        rows = conn.execute("SELECT id FROM inv_recipes ORDER BY name").fetchall()
        return jsonify([recipe_with_items(conn, r['id']) for r in rows])


@app.route('/api/inv/recipes', methods=['POST'])
@login_required
def api_inv_recipes_create():
    import json as _j
    b = request.get_json(force=True)
    with get_db() as conn:
        rec = conn.execute("""
            INSERT INTO inv_recipes (name,description,batch_yield) VALUES (%s,%s,%s) RETURNING id
        """, (b['name'],b.get('description',''),int(b.get('batch_yield') or 1))).fetchone()
        rid = rec['id']
        for it in b.get('items',[]):
            conn.execute("INSERT INTO inv_recipe_items (recipe_id,item_id,quantity) VALUES (%s,%s,%s)",
                         (rid, it['item_id'], float(it['quantity'])))
        return jsonify(recipe_with_items(conn, rid)), 201


@app.route('/api/inv/recipes/<int:recipe_id>', methods=['PUT'])
@login_required
def api_inv_recipe_update(recipe_id):
    b = request.get_json(force=True)
    with get_db() as conn:
        conn.execute("""
            UPDATE inv_recipes SET name=%s,description=%s,batch_yield=%s,updated_at=NOW()
            WHERE id=%s
        """, (b['name'],b.get('description',''),int(b.get('batch_yield') or 1),recipe_id))
        conn.execute("DELETE FROM inv_recipe_items WHERE recipe_id=%s",(recipe_id,))
        for it in b.get('items',[]):
            conn.execute("INSERT INTO inv_recipe_items (recipe_id,item_id,quantity) VALUES (%s,%s,%s)",
                         (recipe_id, it['item_id'], float(it['quantity'])))
        return jsonify(recipe_with_items(conn, recipe_id))


@app.route('/api/inv/recipes/<int:recipe_id>', methods=['DELETE'])
@login_required
def api_inv_recipe_delete(recipe_id):
    with get_db() as conn:
        conn.execute("DELETE FROM inv_recipes WHERE id=%s",(recipe_id,))
    return jsonify({'deleted': recipe_id})


@app.route('/api/inv/recipes/<int:recipe_id>/produce', methods=['POST'])
@login_required
def api_inv_recipe_produce(recipe_id):
    b      = request.get_json(force=True)
    batches= float(b.get('batches') or 1)
    staff  = b.get('staff','')
    with get_db() as conn:
        items = conn.execute("""
            SELECT ri.quantity, i.id as item_id, i.name, i.current_stock, i.unit_label
            FROM inv_recipe_items ri JOIN inv_items i ON i.id=ri.item_id
            WHERE ri.recipe_id=%s
        """, (recipe_id,)).fetchall()
        # Check stock
        shortages = []
        for it in items:
            needed = float(it['quantity']) * batches
            if float(it['current_stock']) < needed:
                shortages.append({'name':it['name'],'needed':needed,'available':float(it['current_stock'])})
        if shortages:
            return jsonify({'error':'庫存不足','shortages':shortages}), 422
        # Deduct
        for it in items:
            needed = float(it['quantity']) * batches
            conn.execute("UPDATE inv_items SET current_stock=current_stock-%s,updated_at=NOW() WHERE id=%s",
                         (needed, it['item_id']))
            conn.execute("INSERT INTO inv_transactions (item_id,txn_type,quantity,note,staff,recipe_id) VALUES (%s,'produce',%s,%s,%s,%s)",
                         (it['item_id'], -needed, f'生產 {batches} 批', staff, recipe_id))
        rec = conn.execute("SELECT name,batch_yield FROM inv_recipes WHERE id=%s",(recipe_id,)).fetchone()
        return jsonify({'ok': True, 'batches': batches,
                       'servings': batches * rec['batch_yield'],
                       'recipe': rec['name']})

# ── Orders ─────────────────────────────────────────────────────────

@app.route('/api/inv/orders', methods=['GET'])
@login_required
def api_inv_orders_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM inv_orders ORDER BY created_at DESC").fetchall()
    return jsonify([inv_order_row(r) for r in rows])


@app.route('/api/inv/orders', methods=['POST'])
@login_required
def api_inv_orders_create():
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO inv_orders (item_id,item_name,vendor,quantity,note,staff)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING *
        """, (b.get('item_id'), b['item_name'], b.get('vendor',''),
              b.get('quantity',''), b.get('note',''), b.get('staff',''))
        ).fetchone()
    return jsonify(inv_order_row(row)), 201


@app.route('/api/inv/orders/<int:order_id>', methods=['PUT'])
@login_required
def api_inv_order_update(order_id):
    b = request.get_json(force=True)
    # Mark as ordered
    if b.get('status') == 'ordered':
        with get_db() as conn:
            row = conn.execute("""
                UPDATE inv_orders SET status='ordered', staff=%s, note=%s, ordered_at=NOW()
                WHERE id=%s RETURNING *
            """, (b.get('staff',''), b.get('note',''), order_id)).fetchone()
    else:
        with get_db() as conn:
            row = conn.execute("""
                UPDATE inv_orders SET item_name=%s,vendor=%s,quantity=%s,note=%s,staff=%s
                WHERE id=%s RETURNING *
            """, (b['item_name'],b.get('vendor',''),b.get('quantity',''),
                  b.get('note',''),b.get('staff',''),order_id)).fetchone()
    return jsonify(inv_order_row(row)) if row else ('',404)


@app.route('/api/inv/orders/<int:order_id>', methods=['DELETE'])
@login_required
def api_inv_order_delete(order_id):
    with get_db() as conn:
        conn.execute("DELETE FROM inv_orders WHERE id=%s",(order_id,))
    return jsonify({'deleted': order_id})

# ── Low stock ──────────────────────────────────────────────────────

@app.route('/api/inv/low-stock', methods=['GET'])
@login_required
def api_inv_low_stock():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM inv_items
            WHERE min_stock > 0 AND current_stock <= min_stock
            ORDER BY (current_stock - min_stock)
        """).fetchall()
    return jsonify([inv_item_row(r) for r in rows])

# ── Profit report ──────────────────────────────────────────────────

@app.route('/api/inv/profit', methods=['GET'])
@login_required
def api_inv_profit():
    with get_db() as conn:
        # Total spent on purchasing (in transactions with positive qty)
        spent = conn.execute("""
            SELECT COALESCE(SUM(t.quantity * i.unit_cost), 0) as total_cost
            FROM inv_transactions t JOIN inv_items i ON i.id=t.item_id
            WHERE t.txn_type='in'
        """).fetchone()
        # Current inventory value
        inv_value = conn.execute("""
            SELECT COALESCE(SUM(current_stock * unit_cost), 0) as total_value
            FROM inv_items
        """).fetchone()
        # Per item summary
        items = conn.execute("""
            SELECT i.code, i.name, i.category, i.unit_cost, i.current_stock,
                   i.unit_label,
                   COALESCE(SUM(CASE WHEN t.txn_type='in' THEN t.quantity ELSE 0 END),0) as total_in,
                   COALESCE(SUM(CASE WHEN t.txn_type IN ('out','produce') THEN ABS(t.quantity) ELSE 0 END),0) as total_out
            FROM inv_items i
            LEFT JOIN inv_transactions t ON t.item_id=i.id
            GROUP BY i.id ORDER BY i.code
        """).fetchall()

    result = {
        'total_purchase_cost': float(spent['total_cost']),
        'current_inventory_value': float(inv_value['total_value']),
        'items': [{
            'code': r['code'], 'name': r['name'], 'category': r['category'],
            'unit_cost': float(r['unit_cost']), 'current_stock': float(r['current_stock']),
            'unit_label': r['unit_label'],
            'total_in': float(r['total_in']), 'total_out': float(r['total_out']),
            'cost_in': float(r['total_in']) * float(r['unit_cost']),
            'cost_out': float(r['total_out']) * float(r['unit_cost']),
        } for r in items]
    }
    return jsonify(result)



# ═══════════════════════════════════════════════════════════════════
# Punch Clock API
# ═══════════════════════════════════════════════════════════════════
def _hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def _gps_distance(lat1, lng1, lat2, lng2):
    """Haversine distance in metres."""
    R = 6371000
    p = math.pi / 180
    a = (math.sin((lat2-lat1)*p/2)**2 +
         math.cos(lat1*p) * math.cos(lat2*p) *
         math.sin((lng2-lng1)*p/2)**2)
    return int(2 * R * math.asin(math.sqrt(a)))

def punch_staff_row(row):
    if not row: return None
    d = dict(row)
    d.pop('password_hash', None)   # never expose hash
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d

def punch_record_row(row):
    if not row: return None
    d = dict(row)
    for f in ['latitude','longitude']:
        if d.get(f) is not None: d[f] = float(d[f])
    if d.get('punched_at'): d['punched_at'] = d['punched_at'].isoformat()
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d

# ── Employee punch session (separate from admin session) ───────────

@app.route('/punch')
def punch_page():
    return render_template('punch.html')

@app.route('/api/punch/login', methods=['POST'])
def api_punch_login():
    b        = request.get_json(force=True)
    username = b.get('username','').strip()
    password = b.get('password','').strip()
    if not username or not password:
        return jsonify({'error': '請輸入帳號及密碼'}), 400
    with get_db() as conn:
        staff = conn.execute(
            "SELECT * FROM punch_staff WHERE username=%s AND active=TRUE",
            (username,)
        ).fetchone()
    if not staff or staff['password_hash'] != _hash_pw(password):
        return jsonify({'error': '帳號或密碼錯誤'}), 401
    # Store staff id in session under a separate key
    session['punch_staff_id']   = staff['id']
    session['punch_staff_name'] = staff['name']
    return jsonify({'id': staff['id'], 'name': staff['name'], 'role': staff['role']})

@app.route('/api/punch/logout', methods=['POST'])
def api_punch_logout():
    session.pop('punch_staff_id', None)
    session.pop('punch_staff_name', None)
    return jsonify({'ok': True})

@app.route('/api/punch/me', methods=['GET'])
def api_punch_me():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        staff = conn.execute(
            "SELECT id,name,role FROM punch_staff WHERE id=%s AND active=TRUE", (sid,)
        ).fetchone()
    if not staff:
        session.pop('punch_staff_id', None)
        return jsonify({'error': 'not logged in'}), 401
    return jsonify(dict(staff))

# ── GPS settings (public read for punch page) ──────────────────────

@app.route('/api/punch/settings', methods=['GET'])
def api_punch_settings_get():
    with get_db() as conn:
        row = conn.execute("SELECT * FROM punch_settings WHERE id=1").fetchone()
    if not row:
        return jsonify({'gps_required': False, 'radius_m': 100})
    d = dict(row)
    for f in ['lat','lng']:
        if d.get(f) is not None: d[f] = float(d[f])
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    return jsonify(d)

@app.route('/api/punch/settings', methods=['PUT'])
@login_required
def api_punch_settings_update():
    b = request.get_json(force=True)
    lat          = float(b['lat'])          if b.get('lat')          else None
    lng          = float(b['lng'])          if b.get('lng')          else None
    radius_m     = int(b.get('radius_m') or 100)
    location_name= b.get('location_name','').strip()
    gps_required = bool(b.get('gps_required', False))
    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_settings
            SET lat=%s, lng=%s, radius_m=%s, location_name=%s,
                gps_required=%s, updated_at=NOW()
            WHERE id=1 RETURNING *
        """, (lat, lng, radius_m, location_name, gps_required)).fetchone()
    d = dict(row)
    for f in ['lat','lng']:
        if d.get(f) is not None: d[f] = float(d[f])
    return jsonify(d)

# ── Clock in/out (requires punch session + GPS check) ─────────────

@app.route('/api/punch/clock', methods=['POST'])
def api_punch_clock():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': '請先登入'}), 401

    b          = request.get_json(force=True)
    punch_type = b.get('punch_type')
    lat        = b.get('lat')
    lng        = b.get('lng')

    if punch_type not in ('in','out','break_out','break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400

    with get_db() as conn:
        staff = conn.execute(
            "SELECT * FROM punch_staff WHERE id=%s AND active=TRUE", (sid,)
        ).fetchone()
        if not staff:
            return jsonify({'error': '員工不存在'}), 404

        settings = conn.execute("SELECT * FROM punch_settings WHERE id=1").fetchone()

    # GPS check
    gps_distance = None
    if settings and settings['gps_required']:
        if lat is None or lng is None:
            return jsonify({'error': '無法取得 GPS，請允許定位權限後重試'}), 403
        if settings['lat'] is None:
            return jsonify({'error': '管理員尚未設定打卡地點'}), 403
        dist = _gps_distance(lat, lng, float(settings['lat']), float(settings['lng']))
        gps_distance = dist
        if dist > int(settings['radius_m']):
            return jsonify({
                'error': f'您距離打卡地點 {dist} 公尺，超出允許範圍（{settings["radius_m"]} 公尺）',
                'distance': dist,
                'radius': settings['radius_m']
            }), 403
    elif lat is not None and lng is not None and settings and settings['lat']:
        # GPS not required but record distance anyway
        gps_distance = _gps_distance(lat, lng, float(settings['lat']), float(settings['lng']))

    # Prevent duplicate within 1 min
    with get_db() as conn:
        recent = conn.execute("""
            SELECT id FROM punch_records
            WHERE staff_id=%s AND punch_type=%s
              AND punched_at > NOW() - INTERVAL '1 minute'
        """, (sid, punch_type)).fetchone()
        if recent:
            return jsonify({'error': '1 分鐘內已打過卡'}), 429

        row = conn.execute("""
            INSERT INTO punch_records
              (staff_id, punch_type, latitude, longitude, gps_distance)
            VALUES (%s, %s, %s, %s, %s) RETURNING *
        """, (sid, punch_type,
              lat if lat is not None else None,
              lng if lng is not None else None,
              gps_distance)).fetchone()

    d = punch_record_row(row)
    d['staff_name']   = staff['name']
    d['gps_distance'] = gps_distance
    return jsonify(d), 201

@app.route('/api/punch/today', methods=['GET'])
def api_punch_today():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify([])
    with get_db() as conn:
        rows = conn.execute("""
            SELECT pr.*, ps.name as staff_name
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE pr.staff_id=%s
              AND pr.punched_at::date = NOW()::date
            ORDER BY pr.punched_at ASC
        """, (sid,)).fetchall()
    return jsonify([punch_record_row(r) for r in rows])

# ── Admin: staff CRUD ──────────────────────────────────────────────

@app.route('/api/punch/staff', methods=['GET'])
@login_required
def api_punch_staff_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM punch_staff ORDER BY name").fetchall()
    return jsonify([punch_staff_row(r) for r in rows])

@app.route('/api/punch/staff', methods=['POST'])
@login_required
def api_punch_staff_create():
    b        = request.get_json(force=True)
    name     = b.get('name','').strip()
    username = b.get('username','').strip()
    password = b.get('password','').strip()
    if not name:
        return jsonify({'error': '姓名為必填'}), 400
    if not username:
        return jsonify({'error': '帳號為必填'}), 400
    if not password or len(password) < 4:
        return jsonify({'error': '密碼至少 4 個字元'}), 400
    try:
        with get_db() as conn:
            row = conn.execute("""
                INSERT INTO punch_staff (name, username, password_hash, role)
                VALUES (%s, %s, %s, %s) RETURNING *
            """, (name, username, _hash_pw(password), b.get('role','').strip())
            ).fetchone()
        return jsonify(punch_staff_row(row)), 201
    except psycopg.errors.UniqueViolation:
        return jsonify({'error': '姓名或帳號已存在'}), 409

@app.route('/api/punch/staff/<int:sid>', methods=['PUT'])
@login_required
def api_punch_staff_update(sid):
    b        = request.get_json(force=True)
    name     = b.get('name','').strip()
    username = b.get('username','').strip()
    password = b.get('password','').strip()   # empty = don't change
    role     = b.get('role','').strip()
    active   = bool(b.get('active', True))
    if not name or not username:
        return jsonify({'error': '姓名和帳號為必填'}), 400
    with get_db() as conn:
        if password:
            if len(password) < 4:
                return jsonify({'error': '密碼至少 4 個字元'}), 400
            row = conn.execute("""
                UPDATE punch_staff
                SET name=%s, username=%s, password_hash=%s, role=%s, active=%s
                WHERE id=%s RETURNING *
            """, (name, username, _hash_pw(password), role, active, sid)).fetchone()
        else:
            row = conn.execute("""
                UPDATE punch_staff
                SET name=%s, username=%s, role=%s, active=%s
                WHERE id=%s RETURNING *
            """, (name, username, role, active, sid)).fetchone()
    return jsonify(punch_staff_row(row)) if row else ('', 404)

@app.route('/api/punch/staff/<int:sid>', methods=['DELETE'])
@login_required
def api_punch_staff_delete(sid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_staff WHERE id=%s", (sid,))
    return jsonify({'deleted': sid})

# ── Admin: records CRUD ────────────────────────────────────────────

@app.route('/api/punch/records', methods=['GET'])
@login_required
def api_punch_records():
    staff_id  = request.args.get('staff_id')
    date_from = request.args.get('date_from')
    date_to   = request.args.get('date_to')
    month     = request.args.get('month')

    conds, params = ["TRUE"], []
    if staff_id:
        conds.append("pr.staff_id=%s"); params.append(int(staff_id))
    if month:
        conds.append("TO_CHAR(pr.punched_at,'YYYY-MM')=%s"); params.append(month)
    elif date_from:
        conds.append("pr.punched_at::date>=%s"); params.append(date_from)
        if date_to:
            conds.append("pr.punched_at::date<=%s"); params.append(date_to)

    where = " AND ".join(conds)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT pr.*, ps.name as staff_name, ps.role as staff_role
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE {where}
            ORDER BY pr.punched_at DESC LIMIT 500
        """, params).fetchall()
    return jsonify([punch_record_row(r) for r in rows])

@app.route('/api/punch/records', methods=['POST'])
@login_required
def api_punch_record_manual():
    b          = request.get_json(force=True)
    staff_id   = b.get('staff_id')
    punch_type = b.get('punch_type')
    punched_at = b.get('punched_at')
    note       = b.get('note','').strip()
    manual_by  = b.get('manual_by','').strip()
    if not all([staff_id, punch_type, punched_at]):
        return jsonify({'error': '缺少必要欄位'}), 400
    if punch_type not in ('in','out','break_out','break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO punch_records
              (staff_id, punch_type, punched_at, note, is_manual, manual_by)
            VALUES (%s,%s,%s,%s,TRUE,%s) RETURNING *
        """, (staff_id, punch_type, punched_at, note, manual_by)).fetchone()
        staff = conn.execute("SELECT name FROM punch_staff WHERE id=%s",(staff_id,)).fetchone()
    d = punch_record_row(row)
    if staff: d['staff_name'] = staff['name']
    return jsonify(d), 201

@app.route('/api/punch/records/<int:rid>', methods=['PUT'])
@login_required
def api_punch_record_update(rid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_records
            SET punch_type=%s, punched_at=%s, note=%s, is_manual=TRUE, manual_by=%s
            WHERE id=%s RETURNING *
        """, (b.get('punch_type'), b.get('punched_at'),
              b.get('note',''), b.get('manual_by',''), rid)).fetchone()
    return jsonify(punch_record_row(row)) if row else ('', 404)

@app.route('/api/punch/records/<int:rid>', methods=['DELETE'])
@login_required
def api_punch_record_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_records WHERE id=%s",(rid,))
    return jsonify({'deleted': rid})

@app.route('/api/punch/summary', methods=['GET'])
@login_required
def api_punch_summary():
    from datetime import datetime
    month = request.args.get('month') or datetime.now().strftime('%Y-%m')
    with get_db() as conn:
        rows = conn.execute("""
            SELECT ps.id as staff_id, ps.name as staff_name,
                   pr.punched_at::date as work_date,
                   MIN(CASE WHEN pr.punch_type='in'  THEN pr.punched_at END) as clock_in,
                   MAX(CASE WHEN pr.punch_type='out' THEN pr.punched_at END) as clock_out,
                   COUNT(*) as punch_count,
                   BOOL_OR(pr.is_manual) as has_manual
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE TO_CHAR(pr.punched_at,'YYYY-MM')=%s
            GROUP BY ps.id, ps.name, pr.punched_at::date
            ORDER BY pr.punched_at::date DESC, ps.name
        """, (month,)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['work_date']  = d['work_date'].isoformat()  if d['work_date']  else None
        d['clock_in']   = d['clock_in'].isoformat()   if d['clock_in']   else None
        d['clock_out']  = d['clock_out'].isoformat()  if d['clock_out']  else None
        if d['clock_in'] and d['clock_out']:
            from datetime import datetime as _dt
            ci = _dt.fromisoformat(d['clock_in'].replace('Z',''))
            co = _dt.fromisoformat(d['clock_out'].replace('Z',''))
            d['duration_min'] = max(0, int((co-ci).total_seconds()/60))
        else:
            d['duration_min'] = None
        result.append(d)
    return jsonify(result)


@app.route('/')
def index():
    return redirect(url_for('admin_login'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)