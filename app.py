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
    PostbackEvent, FlexSendMessage, LocationMessage
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
    {'key': 'breakfast_total',    'label': '交班點收',        'subtract': False},
    {'key': 'breakfast_cash',     'label': '現金',             'subtract': False},
    {'key': 'breakfast_card',     'label': '刷卡',             'subtract': False},
    {'key': 'breakfast_linepay',  'label': 'Line pay',         'subtract': False},
    {'key': 'breakfast_transfer', 'label': '轉帳',             'subtract': False},
    {'key': 'counter_expense',    'label': '支出',             'subtract': True},
    {'key': 'panda',              'label': '入金',             'subtract': False},
    {'key': 'ubereats',           'label': 'Uber Eats',        'subtract': False},
    {'key': 'tips',               'label': '小費',             'subtract': False},
    {'key': 'surplus',            'label': '溢收',             'subtract': False},
    {'key': 'pos_total',          'label': '當日營業總額',     'subtract': False},
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
                CREATE TABLE IF NOT EXISTS punch_locations (
                    id            SERIAL PRIMARY KEY,
                    location_name TEXT NOT NULL DEFAULT '打卡地點',
                    lat           NUMERIC(10,6) NOT NULL,
                    lng           NUMERIC(10,6) NOT NULL,
                    radius_m      INT DEFAULT 100,
                    active        BOOLEAN DEFAULT TRUE,
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    updated_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS punch_config (
                    id           INT PRIMARY KEY DEFAULT 1,
                    gps_required BOOLEAN DEFAULT FALSE,
                    updated_at   TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                INSERT INTO punch_config (id, gps_required)
                VALUES (1, FALSE)
                ON CONFLICT (id) DO NOTHING
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS line_punch_config (
                    id                  INT PRIMARY KEY DEFAULT 1,
                    channel_access_token TEXT DEFAULT '',
                    channel_secret       TEXT DEFAULT '',
                    enabled              BOOLEAN DEFAULT FALSE,
                    updated_at           TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                INSERT INTO line_punch_config (id)
                VALUES (1)
                ON CONFLICT (id) DO NOTHING
            """)
            # ── Scheduling tables ─────────────────────────────────────────
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule_config (
                    month           TEXT PRIMARY KEY,
                    max_off_per_day INT DEFAULT 2,
                    vacation_quota  INT DEFAULT 8,
                    notes           TEXT DEFAULT '',
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule_requests (
                    id           SERIAL PRIMARY KEY,
                    staff_id     INT REFERENCES punch_staff(id) ON DELETE CASCADE,
                    month        TEXT NOT NULL,
                    dates        JSONB NOT NULL DEFAULT '[]',
                    status       TEXT DEFAULT 'pending',
                    submit_note  TEXT DEFAULT '',
                    reviewed_by  TEXT DEFAULT '',
                    reviewed_at  TIMESTAMPTZ,
                    review_note  TEXT DEFAULT '',
                    created_at   TIMESTAMPTZ DEFAULT NOW(),
                    updated_at   TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(staff_id, month)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS salary_employees (
                    id              SERIAL PRIMARY KEY,
                    staff_id        INT REFERENCES punch_staff(id) ON DELETE SET NULL,
                    employee_code   TEXT UNIQUE NOT NULL,
                    name            TEXT NOT NULL,
                    department      TEXT DEFAULT '',
                    position        TEXT DEFAULT '',
                    hire_date       DATE,
                    birth_date      DATE,
                    base_salary     NUMERIC(12,2) DEFAULT 0,
                    insured_salary  NUMERIC(12,2) DEFAULT 0,
                    active          BOOLEAN DEFAULT TRUE,
                    notes           TEXT DEFAULT '',
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS salary_components (
                    id              SERIAL PRIMARY KEY,
                    name            TEXT NOT NULL,
                    comp_type       TEXT NOT NULL,
                    calc_type       TEXT DEFAULT 'fixed',
                    formula         TEXT DEFAULT '',
                    default_amount  NUMERIC(12,2) DEFAULT 0,
                    sort_order      INT DEFAULT 0,
                    is_birthday     BOOLEAN DEFAULT FALSE,
                    active          BOOLEAN DEFAULT TRUE,
                    description     TEXT DEFAULT '',
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS salary_records (
                    id               SERIAL PRIMARY KEY,
                    employee_id      INT REFERENCES salary_employees(id) ON DELETE CASCADE,
                    month            TEXT NOT NULL,
                    gross_pay        NUMERIC(12,2) DEFAULT 0,
                    total_deductions NUMERIC(12,2) DEFAULT 0,
                    net_pay          NUMERIC(12,2) DEFAULT 0,
                    pay_date         DATE,
                    status           TEXT DEFAULT 'draft',
                    notes            TEXT DEFAULT '',
                    created_at       TIMESTAMPTZ DEFAULT NOW(),
                    updated_at       TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(employee_id, month)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS salary_record_items (
                    id              SERIAL PRIMARY KEY,
                    record_id       INT REFERENCES salary_records(id) ON DELETE CASCADE,
                    component_id    INT,
                    component_name  TEXT NOT NULL,
                    comp_type       TEXT NOT NULL,
                    amount          NUMERIC(12,2) DEFAULT 0,
                    note            TEXT DEFAULT ''
                )
            """)
            # Seed default components if empty
            existing = conn.execute("SELECT COUNT(*) as cnt FROM salary_components").fetchone()
            if existing['cnt'] == 0:
                defaults = [
                    ('本薪',     'allowance', 'formula', 'base_salary',     0, 1),
                    ('伙食津貼', 'allowance', 'fixed',   '',              2400, 2),
                    ('職務加給', 'allowance', 'fixed',   '',                 0, 3),
                    ('全勤獎金', 'allowance', 'fixed',   '',                 0, 4),
                    ('加班費',   'allowance', 'fixed',   '',                 0, 5),
                    ('獎金',     'allowance', 'fixed',   '',                 0, 6),
                    ('勞保費',   'deduction', 'formula', 'insured_salary*0.011', 0, 10),
                    ('健保費',   'deduction', 'formula', 'insured_salary*0.0517/6*2', 0, 11),
                    ('勞退6%',   'allowance', 'formula', 'base_salary*0.06', 0, 12),
                ]
                for name, ctype, cmode, formula, amt, sort in defaults:
                    conn.execute("""
                        INSERT INTO salary_components
                          (name,comp_type,calc_type,formula,default_amount,sort_order)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (name, ctype, cmode, formula, amt, sort))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS punch_requests (
                    id            SERIAL PRIMARY KEY,
                    staff_id      INT REFERENCES punch_staff(id) ON DELETE CASCADE,
                    punch_type    TEXT NOT NULL,
                    requested_at  TIMESTAMPTZ NOT NULL,
                    reason        TEXT DEFAULT '',
                    status        TEXT DEFAULT 'pending',
                    reviewed_by   TEXT DEFAULT '',
                    review_note   TEXT DEFAULT '',
                    reviewed_at   TIMESTAMPTZ,
                    created_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS shift_types (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT NOT NULL,
                    start_time  TIME NOT NULL,
                    end_time    TIME NOT NULL,
                    color       TEXT DEFAULT '#4a7bda',
                    departments TEXT DEFAULT '',
                    active      BOOLEAN DEFAULT TRUE,
                    sort_order  INT DEFAULT 0,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS shift_assignments (
                    id            SERIAL PRIMARY KEY,
                    staff_id      INT REFERENCES punch_staff(id) ON DELETE CASCADE,
                    shift_type_id INT REFERENCES shift_types(id) ON DELETE CASCADE,
                    shift_date    DATE NOT NULL,
                    note          TEXT DEFAULT '',
                    created_at    TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(staff_id, shift_date)
                )
            """)
            # Seed default shifts
            existing_shifts = conn.execute("SELECT COUNT(*) as cnt FROM shift_types").fetchone()
            if existing_shifts['cnt'] == 0:
                defaults = [
                    ('吧台班',  '08:00', '16:00', '#8b5cf6', '吧台',   1),
                    ('外場A班', '09:00', '17:00', '#2e9e6b', '外場',   2),
                    ('外場B班', '14:00', '22:00', '#0ea5e9', '外場',   3),
                    ('廚房A班', '08:00', '16:00', '#e07b2a', '廚房',   4),
                    ('廚房B班', '12:00', '20:00', '#d64242', '廚房',   5),
                    ('廚房C班', '16:00', '00:00', '#6366f1', '廚房',   6),
                ]
                for name, st, et, color, dept, sort in defaults:
                    conn.execute("""
                        INSERT INTO shift_types (name,start_time,end_time,color,departments,sort_order)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (name, st, et, color, dept, sort))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS overtime_requests (
                    id              SERIAL PRIMARY KEY,
                    staff_id        INT REFERENCES punch_staff(id) ON DELETE CASCADE,
                    request_date    DATE NOT NULL,
                    start_time      TIME NOT NULL,
                    end_time        TIME NOT NULL,
                    ot_hours        NUMERIC(5,2),
                    reason          TEXT DEFAULT '',
                    status          TEXT DEFAULT 'pending',
                    reviewed_by     TEXT DEFAULT '',
                    review_note     TEXT DEFAULT '',
                    ot_pay          NUMERIC(12,2) DEFAULT 0,
                    reviewed_at     TIMESTAMPTZ,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                )
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
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS location_name TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS line_user_id TEXT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS bind_code TEXT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS employee_code TEXT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS department TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS position_title TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hire_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS birth_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS base_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS insured_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_notes TEXT DEFAULT ''",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS staff_id INT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS daily_hours NUMERIC(4,1) DEFAULT 8",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate1 NUMERIC(4,2) DEFAULT 1.33",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate2 NUMERIC(4,2) DEFAULT 1.67",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_type TEXT DEFAULT 'monthly'",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hourly_rate NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS day_type TEXT DEFAULT 'weekday'",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS vacation_quota INT DEFAULT NULL",
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

    # Trigger accounting only on keyword "交班"
    TRIGGER_KEYWORDS = ['交班', '記帳', '交班記帳']
    if text in TRIGGER_KEYWORDS:
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text="財務記帳系統", contents=make_start_flex())
        )
    # Other messages: ignore (don't reply, avoid interfering with punch bot)


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
@app.route('/staff')
def punch_page():
    return render_template('staff.html')

@app.route('/schedule')
def schedule_page_redirect():
    return render_template('staff.html')

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

# ── GPS locations + config ─────────────────────────────────────────

def loc_row(row):
    if not row: return None
    d = dict(row)
    for f in ['lat','lng']: d[f] = float(d[f]) if d.get(f) is not None else None
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    return d

@app.route('/api/punch/settings', methods=['GET'])
def api_punch_settings_get():
    """Return config + all active locations for punch page."""
    with get_db() as conn:
        cfg  = conn.execute("SELECT * FROM punch_config WHERE id=1").fetchone()
        locs = conn.execute(
            "SELECT * FROM punch_locations WHERE active=TRUE ORDER BY id"
        ).fetchall()
    result = {
        'gps_required': cfg['gps_required'] if cfg else False,
        'locations': [loc_row(r) for r in locs]
    }
    return jsonify(result)

@app.route('/api/punch/config', methods=['PUT'])
@login_required
def api_punch_config_update():
    b = request.get_json(force=True)
    gps_required = bool(b.get('gps_required', False))
    with get_db() as conn:
        conn.execute(
            "UPDATE punch_config SET gps_required=%s, updated_at=NOW() WHERE id=1",
            (gps_required,)
        )
    return jsonify({'gps_required': gps_required})

# Locations CRUD
@app.route('/api/punch/locations', methods=['GET'])
@login_required
def api_punch_locations_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM punch_locations ORDER BY id").fetchall()
    return jsonify([loc_row(r) for r in rows])

@app.route('/api/punch/locations', methods=['POST'])
@login_required
def api_punch_locations_create():
    b = request.get_json(force=True)
    name = b.get('location_name','').strip() or '打卡地點'
    try:
        lat = float(b['lat']); lng = float(b['lng'])
    except (KeyError, TypeError, ValueError):
        return jsonify({'error': '請填入有效的緯度和經度'}), 400
    radius_m = int(b.get('radius_m') or 100)
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO punch_locations (location_name, lat, lng, radius_m)
            VALUES (%s, %s, %s, %s) RETURNING *
        """, (name, lat, lng, radius_m)).fetchone()
    return jsonify(loc_row(row)), 201

@app.route('/api/punch/locations/<int:lid>', methods=['PUT'])
@login_required
def api_punch_locations_update(lid):
    b = request.get_json(force=True)
    name = b.get('location_name','').strip() or '打卡地點'
    try:
        lat = float(b['lat']); lng = float(b['lng'])
    except (KeyError, TypeError, ValueError):
        return jsonify({'error': '請填入有效的緯度和經度'}), 400
    radius_m = int(b.get('radius_m') or 100)
    active   = bool(b.get('active', True))
    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_locations
            SET location_name=%s, lat=%s, lng=%s, radius_m=%s, active=%s, updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (name, lat, lng, radius_m, active, lid)).fetchone()
    return jsonify(loc_row(row)) if row else ('', 404)

@app.route('/api/punch/locations/<int:lid>', methods=['DELETE'])
@login_required
def api_punch_locations_delete(lid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_locations WHERE id=%s", (lid,))
    return jsonify({'deleted': lid})

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

        cfg  = conn.execute("SELECT * FROM punch_config WHERE id=1").fetchone()
        locs = conn.execute(
            "SELECT * FROM punch_locations WHERE active=TRUE"
        ).fetchall()

    gps_required = cfg['gps_required'] if cfg else False

    # GPS check — pass if within range of ANY active location
    gps_distance  = None
    matched_loc   = None
    if lat is not None and lng is not None and locs:
        for loc in locs:
            d = _gps_distance(lat, lng, float(loc['lat']), float(loc['lng']))
            if gps_distance is None or d < gps_distance:
                gps_distance = d
                matched_loc  = loc

    if gps_required:
        if lat is None or lng is None:
            return jsonify({'error': '無法取得 GPS，請允許定位權限後重試'}), 403
        if not locs:
            return jsonify({'error': '管理員尚未設定任何打卡地點'}), 403
        if gps_distance is None or gps_distance > int(matched_loc['radius_m']):
            nearest_name = matched_loc['location_name'] if matched_loc else '打卡地點'
            nearest_dist = gps_distance or 0
            nearest_r    = int(matched_loc['radius_m']) if matched_loc else 100
            return jsonify({
                'error': f'距離最近地點「{nearest_name}」{nearest_dist} 公尺，超出允許範圍（{nearest_r} 公尺）',
                'distance': nearest_dist,
                'radius': nearest_r
            }), 403

    # Prevent duplicate within 1 min
    with get_db() as conn:
        recent = conn.execute("""
            SELECT id FROM punch_records
            WHERE staff_id=%s AND punch_type=%s
              AND punched_at > NOW() - INTERVAL '1 minute'
        """, (sid, punch_type)).fetchone()
        if recent:
            return jsonify({'error': '1 分鐘內已打過卡'}), 429

        matched_name = matched_loc['location_name'] if matched_loc else ''
        row = conn.execute("""
            INSERT INTO punch_records
              (staff_id, punch_type, latitude, longitude, gps_distance, location_name)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING *
        """, (sid, punch_type,
              lat if lat is not None else None,
              lng if lng is not None else None,
              gps_distance,
              matched_name)).fetchone()

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
              AND (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date
                = (NOW() AT TIME ZONE 'Asia/Taipei')::date
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
        conds.append("(pr.punched_at AT TIME ZONE 'Asia/Taipei')::date>=%s"); params.append(date_from)
        if date_to:
            conds.append("(pr.punched_at AT TIME ZONE 'Asia/Taipei')::date<=%s"); params.append(date_to)

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
                   (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date as work_date,
                   MIN(CASE WHEN pr.punch_type='in'  THEN pr.punched_at AT TIME ZONE 'Asia/Taipei' END) as clock_in,
                   MAX(CASE WHEN pr.punch_type='out' THEN pr.punched_at AT TIME ZONE 'Asia/Taipei' END) as clock_out,
                   COUNT(*) as punch_count,
                   BOOL_OR(pr.is_manual) as has_manual
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE TO_CHAR(pr.punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s
            GROUP BY ps.id, ps.name, (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date
            ORDER BY (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date DESC, ps.name
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



# ═══════════════════════════════════════════════════════════════════
# Schedule API
# ═══════════════════════════════════════════════════════════════════
import json as _json
from datetime import datetime as _dt, timedelta as _td

WEEKDAY_ZH = ['一','二','三','四','五','六','日']

def sched_req_row(row):
    if not row: return None
    d = dict(row)
    if isinstance(d.get('dates'), str):
        try: d['dates'] = _json.loads(d['dates'])
        except: d['dates'] = []
    if d.get('reviewed_at'): d['reviewed_at'] = d['reviewed_at'].isoformat()
    if d.get('created_at'):  d['created_at']  = d['created_at'].isoformat()
    if d.get('updated_at'):  d['updated_at']  = d['updated_at'].isoformat()
    return d

def get_schedule_config(conn, month):
    row = conn.execute(
        "SELECT * FROM schedule_config WHERE month=%s", (month,)
    ).fetchone()
    if not row:
        return {'month': month, 'max_off_per_day': 2, 'vacation_quota': 8, 'notes': ''}
    return dict(row)

def get_off_counts(conn, month):
    """Return {date_str: count} of approved off days in a month."""
    rows = conn.execute("""
        SELECT unnest(dates::text[]) as d, COUNT(*) as cnt
        FROM schedule_requests
        WHERE month=%s AND status IN ('approved','pending')
        GROUP BY d
    """, (month,)).fetchall()
    # Use jsonb approach instead
    rows2 = conn.execute("""
        SELECT elem as d, COUNT(*) as cnt
        FROM schedule_requests,
             jsonb_array_elements_text(dates) as elem
        WHERE month=%s AND status IN ('approved','pending')
        GROUP BY elem
    """, (month,)).fetchall()
    return {r['d']: int(r['cnt']) for r in rows2}

# ── Employee-facing (punch session) ───────────────────────────────



@app.route('/api/schedule/config/<month>', methods=['GET'])
def api_sched_config_get(month):
    """Public: get month config + off-count map.
    If employee is logged in, returns their personal vacation_quota (if set)
    overriding the monthly default.
    """
    sid = session.get('punch_staff_id')
    with get_db() as conn:
        cfg    = dict(get_schedule_config(conn, month))
        counts = get_off_counts(conn, month)
        # Per-employee quota override
        if sid:
            row = conn.execute(
                "SELECT vacation_quota FROM punch_staff WHERE id=%s", (sid,)
            ).fetchone()
            if row and row['vacation_quota'] is not None:
                cfg['vacation_quota'] = int(row['vacation_quota'])
                cfg['quota_personal'] = True
    return jsonify({**cfg, 'off_counts': counts})

@app.route('/api/schedule/my-request/<month>', methods=['GET'])
def api_sched_my_request(month):
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        row = conn.execute(
            "SELECT sr.*, ps.name as staff_name FROM schedule_requests sr "
            "JOIN punch_staff ps ON ps.id=sr.staff_id "
            "WHERE sr.staff_id=%s AND sr.month=%s",
            (sid, month)
        ).fetchone()
    return jsonify(sched_req_row(row)) if row else jsonify(None)

@app.route('/api/schedule/my-request', methods=['POST'])
def api_sched_submit():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b     = request.get_json(force=True)
    month = b.get('month','').strip()
    dates = b.get('dates', [])
    note  = b.get('submit_note','').strip()

    if not month: return jsonify({'error': '請選擇月份'}), 400
    if not isinstance(dates, list):
        return jsonify({'error': '日期格式錯誤'}), 400

    # Validate dates belong to the month
    for d in dates:
        if not d.startswith(month):
            return jsonify({'error': f'日期 {d} 不屬於 {month}'}), 400

    try:
        with get_db() as conn:
            cfg = get_schedule_config(conn, month)

            # Get effective quota: per-employee > monthly default
            staff_row = conn.execute(
                "SELECT vacation_quota FROM punch_staff WHERE id=%s", (sid,)
            ).fetchone()
            personal_quota = staff_row['vacation_quota'] if staff_row and staff_row['vacation_quota'] is not None else None
            effective_quota = personal_quota if personal_quota is not None else cfg['vacation_quota']

            # Check quota
            if len(dates) > effective_quota:
                quota_source = '個人配額' if personal_quota is not None else '月份預設配額'
                return jsonify({
                    'error': f'申請天數（{len(dates)}天）超過{quota_source}（{effective_quota}天）'
                }), 422

            # Check per-day max (exclude self)
            overcrowded = []
            for d in dates:
                try:
                    others = conn.execute("""
                        SELECT COUNT(*) as cnt
                        FROM schedule_requests,
                             jsonb_array_elements_text(dates) as elem
                        WHERE month=%s AND status IN ('approved','pending')
                          AND staff_id != %s AND elem=%s
                    """, (month, sid, d)).fetchone()
                    others_count = int(others['cnt']) if others else 0
                except Exception:
                    others_count = 0
                if others_count >= cfg['max_off_per_day']:
                    dt_obj = _dt.strptime(d, '%Y-%m-%d')
                    overcrowded.append({
                        'date': d,
                        'weekday': WEEKDAY_ZH[dt_obj.weekday()],
                        'count': others_count,
                        'max': cfg['max_off_per_day']
                    })
            if overcrowded:
                msgs = [f"{x['date']}（{x['weekday']}）已有 {x['count']} 人排休" for x in overcrowded]
                return jsonify({
                    'error': '以下日期休假人數已達上限：' + '、'.join(msgs),
                    'overcrowded': overcrowded
                }), 422

            # Determine status
            prev = conn.execute(
                "SELECT status FROM schedule_requests WHERE staff_id=%s AND month=%s",
                (sid, month)
            ).fetchone()
            new_status = 'modified_pending' if prev and prev['status'] == 'approved' else 'pending'
            dates_json = _json.dumps(dates, ensure_ascii=False)

            # Ensure updated_at column exists (migration guard)
            try:
                conn.execute("ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
            except Exception:
                pass

            row = conn.execute("""
                INSERT INTO schedule_requests
                  (staff_id, month, dates, status, submit_note, updated_at)
                VALUES (%s, %s, %s::jsonb, %s, %s, NOW())
                ON CONFLICT (staff_id, month) DO UPDATE
                  SET dates        = EXCLUDED.dates,
                      status       = EXCLUDED.status,
                      submit_note  = EXCLUDED.submit_note,
                      updated_at   = NOW()
                RETURNING *
            """, (sid, month, dates_json, new_status, note)).fetchone()

            result = sched_req_row(row)

        # Build date detail list (outside db context is fine)
        d_objs = []
        for ds in dates:
            dt_obj = _dt.strptime(ds, '%Y-%m-%d')
            d_objs.append({
                'date': ds,
                'weekday': WEEKDAY_ZH[dt_obj.weekday()],
                'day': dt_obj.day
            })
        result['date_details'] = d_objs
        return jsonify(result), 201

    except Exception as e:
        import traceback as _tb
        print(f"[SCHED SUBMIT ERROR] {e}\n{_tb.format_exc()}")
        return jsonify({'error': f'系統錯誤：{str(e)}'}), 500

# ── Admin schedule API ─────────────────────────────────────────────

@app.route('/api/schedule/admin/config/<month>', methods=['GET'])
@login_required
def api_sched_admin_config_get(month):
    with get_db() as conn:
        cfg    = get_schedule_config(conn, month)
        counts = get_off_counts(conn, month)
    return jsonify({**cfg, 'off_counts': counts})

@app.route('/api/schedule/admin/config/<month>', methods=['PUT'])
@login_required
def api_sched_admin_config_put(month):
    b = request.get_json(force=True)
    max_off   = int(b.get('max_off_per_day') or 2)
    quota     = int(b.get('vacation_quota') or 8)
    notes     = b.get('notes','').strip()
    with get_db() as conn:
        conn.execute("""
            INSERT INTO schedule_config (month, max_off_per_day, vacation_quota, notes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (month) DO UPDATE
              SET max_off_per_day=%s, vacation_quota=%s, notes=%s, updated_at=NOW()
        """, (month, max_off, quota, notes, max_off, quota, notes))
    return jsonify({'month': month, 'max_off_per_day': max_off,
                    'vacation_quota': quota, 'notes': notes})

@app.route('/api/schedule/admin/requests', methods=['GET'])
@login_required
def api_sched_admin_requests():
    month  = request.args.get('month','')
    status = request.args.get('status','')
    conds, params = ['TRUE'], []
    if month:  conds.append('sr.month=%s');  params.append(month)
    if status: conds.append('sr.status=%s'); params.append(status)
    where = ' AND '.join(conds)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT sr.*, ps.name as staff_name, ps.role as staff_role
            FROM schedule_requests sr
            JOIN punch_staff ps ON ps.id=sr.staff_id
            WHERE {where}
            ORDER BY sr.month DESC, ps.name
        """, params).fetchall()
    return jsonify([sched_req_row(r) for r in rows])

@app.route('/api/schedule/admin/requests/<int:rid>', methods=['PUT'])
@login_required
def api_sched_admin_review(rid):
    b           = request.get_json(force=True)
    action      = b.get('action')       # 'approve' | 'reject'
    reviewed_by = b.get('reviewed_by','').strip()
    review_note = b.get('review_note','').strip()
    if action not in ('approve','reject','revoke'):
        return jsonify({'error': 'action must be approve / reject / revoke'}), 400

    if action == 'revoke':
        # Revoke approved request → back to pending, employee can resubmit
        with get_db() as conn:
            row = conn.execute("""
                UPDATE schedule_requests
                SET status='pending', reviewed_by='', review_note=%s,
                    reviewed_at=NULL, updated_at=NOW()
                WHERE id=%s RETURNING *
            """, (review_note or '主管已撤銷核准', rid)).fetchone()
        return jsonify(sched_req_row(row)) if row else ('', 404)

    new_status = 'approved' if action == 'approve' else 'rejected'
    with get_db() as conn:
        row = conn.execute("""
            UPDATE schedule_requests
            SET status=%s, reviewed_by=%s, review_note=%s,
                reviewed_at=NOW(), updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (new_status, reviewed_by, review_note, rid)).fetchone()
    return jsonify(sched_req_row(row)) if row else ('', 404)

@app.route('/api/schedule/admin/requests/<int:rid>', methods=['DELETE'])
@login_required
def api_sched_admin_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM schedule_requests WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})

@app.route('/api/schedule/admin/calendar/<month>', methods=['GET'])
@login_required
def api_sched_admin_calendar(month):
    """Return per-day breakdown: who is off, how many working."""
    with get_db() as conn:
        cfg   = get_schedule_config(conn, month)
        # All staff
        staff = conn.execute(
            "SELECT id, name, role FROM punch_staff WHERE active=TRUE ORDER BY name"
        ).fetchall()
        # All requests for this month
        reqs  = conn.execute("""
            SELECT sr.staff_id, sr.dates, sr.status, ps.name
            FROM schedule_requests sr
            JOIN punch_staff ps ON ps.id=sr.staff_id
            WHERE sr.month=%s AND sr.status IN ('approved','pending','modified_pending')
        """, (month,)).fetchall()

    # Build calendar
    year_int, month_int = int(month[:4]), int(month[5:])
    import calendar as _cal
    days_in_month = _cal.monthrange(year_int, month_int)[1]

    # Map staff_id → off dates
    staff_off = {}  # staff_id → {date: status}
    for r in reqs:
        dates_val = r['dates']
        if isinstance(dates_val, str):
            try: dates_val = _json.loads(dates_val)
            except: dates_val = []
        for d in (dates_val or []):
            if r['staff_id'] not in staff_off:
                staff_off[r['staff_id']] = {}
            staff_off[r['staff_id']][d] = r['status']

    days = []
    for day in range(1, days_in_month+1):
        date_str = f"{month}-{str(day).padStart(2,'0')}" if False else f"{month}-{day:02d}"
        dt = _dt(year_int, month_int, day)
        off_list = []
        for s in staff:
            st = staff_off.get(s['id'], {}).get(date_str)
            if st:
                off_list.append({'staff_id': s['id'], 'name': s['name'],
                                 'role': s['role'], 'status': st})
        days.append({
            'date': date_str,
            'day': day,
            'weekday': WEEKDAY_ZH[dt.weekday()],
            'is_weekend': dt.weekday() >= 5,
            'off_count': len(off_list),
            'off_list': off_list,
            'working_count': len(staff) - len(off_list),
            'over_limit': len(off_list) > cfg['max_off_per_day']
        })

    return jsonify({
        'month': month,
        'config': cfg,
        'staff_count': len(staff),
        'days': days
    })

@app.route('/api/schedule/admin/summary/<month>', methods=['GET'])
@login_required
def api_sched_admin_summary(month):
    """Per-staff summary for the month."""
    with get_db() as conn:
        cfg   = get_schedule_config(conn, month)
        staff = conn.execute(
            "SELECT id, name, role FROM punch_staff WHERE active=TRUE ORDER BY name"
        ).fetchall()
        reqs  = conn.execute("""
            SELECT sr.*
            FROM schedule_requests sr
            WHERE sr.month=%s
        """, (month,)).fetchall()

    req_map = {r['staff_id']: sched_req_row(r) for r in reqs}
    result  = []
    for s in staff:
        req = req_map.get(s['id'])
        result.append({
            'staff_id':   s['id'],
            'name':       s['name'],
            'role':       s['role'],
            'status':     req['status']  if req else 'not_submitted',
            'days_off':   len(req['dates']) if req else 0,
            'quota':      cfg['vacation_quota'],
            'dates':      req['dates']   if req else [],
            'request_id': req['id']      if req else None,
        })
    return jsonify({'config': cfg, 'staff': result})


# ═══════════════════════════════════════════════════════════════════
# LINE Punch Clock — Webhook + Admin API
# ═══════════════════════════════════════════════════════════════════

def get_line_punch_config():
    """Return LINE punch config dict."""
    if not DATABASE_URL:
        return None
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM line_punch_config WHERE id=1").fetchone()
        return dict(row) if row else None
    except Exception:
        return None

def _line_punch_api():
    """Return a LineBotApi instance for punch channel, or None."""
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('enabled') or not cfg.get('channel_access_token'):
        return None
    return LineBotApi(cfg['channel_access_token'])

def _send_line_punch(user_id, text):
    """Send a text reply to a LINE user via punch channel."""
    api = _line_punch_api()
    if not api:
        return
    try:
        api.push_message(user_id, TextSendMessage(text=text))
    except Exception as e:
        print(f"[LINE PUNCH] push_message error: {e}")


def _send_line_punch_with_location_reply(user_id, title, subtitle):
    """Send message with Quick Reply location button."""
    import json as _j
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('channel_access_token'):
        return
    token = cfg['channel_access_token']
    body = {
        "to": user_id,
        "messages": [{
            "type": "text",
            "text": f"{title}\n{subtitle}",
            "quickReply": {
                "items": [{
                    "type": "action",
                    "action": {
                        "type": "location",
                        "label": "傳送我的位置"
                    }
                }]
            }
        }]
    }
    payload = _j.dumps(body).encode('utf-8')
    req = urllib.request.Request(
        'https://api.line.me/v2/bot/message/push',
        data=payload,
        headers={
            'Content-Type':  'application/json',
            'Authorization': f'Bearer {token}'
        }
    )
    try:
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        print(f"[LINE PUNCH] quick reply error: {e}")
        # Fallback to plain text
        _send_line_punch(user_id, f"{title}\n{subtitle}\n\n（點選 + → 位置資訊）")

# ── Webhook ────────────────────────────────────────────────────────

@app.route('/line-punch/webhook', methods=['POST'])
def line_punch_webhook():
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('enabled') or not cfg.get('channel_secret'):
        return 'disabled', 200

    signature = request.headers.get('X-Line-Signature', '')
    body      = request.get_data(as_text=True)

    # Verify signature
    import hmac, hashlib as _hl, base64 as _b64
    secret = cfg['channel_secret'].encode('utf-8')
    computed = _b64.b64encode(
        hmac.new(secret, body.encode('utf-8'), _hl.sha256).digest()
    ).decode('utf-8')
    if not hmac.compare_digest(computed, signature):
        return 'Invalid signature', 400

    import json as _j
    events = _j.loads(body).get('events', [])
    for event in events:
        try:
            _handle_line_punch_event(event, cfg)
        except Exception as e:
            print(f"[LINE PUNCH] event handler error: {e}\n{traceback.format_exc()}")
    return 'OK', 200


def _handle_line_punch_event(event, cfg):
    import json as _j
    source    = event.get('source', {})
    user_id   = source.get('userId')
    evt_type  = event.get('type')
    if not user_id:
        return

    msg = event.get('message', {})
    msg_type = msg.get('type', '')

    if evt_type == 'follow':
        # New follower — ask to bind
        _send_line_punch(user_id,
            '歡迎使用員工打卡系統！👋\n\n請輸入您的登入帳號完成綁定。\n\n✏️ 輸入範例：\n  綁定 mary123\n（請將 mary123 換成您自己的帳號）\n\n不知道帳號？請詢問管理員。')
        return

    if evt_type != 'message':
        return

    with get_db() as conn:
        staff = conn.execute(
            "SELECT * FROM punch_staff WHERE line_user_id=%s AND active=TRUE",
            (user_id,)
        ).fetchone()

    # ── Not bound yet ──────────────────────────────────────────
    if not staff:
        if msg_type == 'text':
            text = msg.get('text', '').strip()
            if text.startswith('綁定 ') or text.startswith('绑定 '):
                username = text.split(' ', 1)[1].strip()
                # Guard: reject placeholder text
                if username in ('帳號', '您的帳號', '[您的帳號]', 'username', '帳號名稱'):
                    _send_line_punch(user_id,
                        '請輸入您「實際的」登入帳號，而非說明文字。\n\n'
                        '範例：綁定 mary123\n'
                        '（請將 mary123 換成您自己的帳號）')
                    return
                with get_db() as conn:
                    candidate = conn.execute(
                        "SELECT * FROM punch_staff WHERE username=%s AND active=TRUE",
                        (username,)
                    ).fetchone()
                if not candidate:
                    _send_line_punch(user_id,
                        f'找不到帳號「{username}」\n\n'
                        f'請確認帳號是否正確，或詢問管理員您的登入帳號。')
                    return
                if candidate['line_user_id']:
                    _send_line_punch(user_id, '此帳號已綁定其他 LINE 帳號，請聯絡管理員。')
                    return
                with get_db() as conn:
                    conn.execute(
                        "UPDATE punch_staff SET line_user_id=%s WHERE id=%s",
                        (user_id, candidate['id'])
                    )
                _send_line_punch(user_id,
                    f'✅ 綁定成功！\n歡迎 {candidate["name"]}！\n\n打卡方式：\n📍 傳送位置訊息 → 自動打卡\n💬 或輸入：上班 / 下班 / 休息 / 回來\n\n輸入「狀態」可查看今日打卡記錄。')
            else:
                _send_line_punch(user_id,
                    '您尚未綁定打卡帳號。\n\n請輸入您的登入帳號：\n  綁定 [您的帳號]\n\n範例：綁定 mary123\n（請將 mary123 換成您自己的帳號）')
        return

    # ── Bound staff ────────────────────────────────────────────
    PUNCH_CMDS = {
        '上班': 'in', '上班打卡': 'in',
        '下班': 'out', '下班打卡': 'out',
        '休息': 'break_out', '休息開始': 'break_out',
        '回來': 'break_in', '休息結束': 'break_in',
    }
    PUNCH_LABEL = {
        'in': '上班打卡', 'out': '下班打卡',
        'break_out': '休息開始', 'break_in': '休息結束'
    }

    if msg_type == 'location':
        # Auto-punch with GPS verification
        lat = msg.get('latitude')
        lng = msg.get('longitude')
        _do_line_punch(staff, user_id, lat, lng, None, PUNCH_LABEL, cfg)

    elif msg_type == 'text':
        text = msg.get('text', '').strip()

        if text == '狀態' or text == '打卡記錄':
            _send_status(staff, user_id)
            return

        if text == '解除綁定':
            with get_db() as conn:
                conn.execute(
                    "UPDATE punch_staff SET line_user_id=NULL WHERE id=%s",
                    (staff['id'],)
                )
            _send_line_punch(user_id, '已解除 LINE 帳號綁定。')
            return

        punch_type = PUNCH_CMDS.get(text)
        if punch_type:
            # Text command — check if GPS required
            with get_db() as conn:
                pcfg = conn.execute("SELECT * FROM punch_config WHERE id=1").fetchone()
                locs = conn.execute(
                    "SELECT * FROM punch_locations WHERE active=TRUE"
                ).fetchall()

            gps_required = pcfg['gps_required'] if pcfg else False
            if gps_required and locs:
                _send_line_punch_with_location_reply(
                    user_id,
                    f'請傳送您的位置來完成{PUNCH_LABEL[punch_type]}',
                    '點下方「傳送位置」按鈕即可打卡'
                )
                _pending_line_punches[user_id] = punch_type
            else:
                # No GPS required — punch directly
                _do_line_punch(staff, user_id, None, None, punch_type, PUNCH_LABEL, cfg)
        else:
            _send_line_punch(user_id,
                f'哈囉 {staff["name"]}！\n\n打卡指令：\n📍 傳送位置 → 自動打卡\n💬 上班 / 下班 / 休息 / 回來\n📋 狀態 → 查看今日記錄')


# In-memory pending punch (for GPS-required text command flow)
_pending_line_punches = {}  # {line_user_id: punch_type}


def _do_line_punch(staff, user_id, lat, lng, forced_type, PUNCH_LABEL, cfg):
    """Execute punch with GPS check. forced_type overrides location-based auto."""
    from datetime import datetime as _dt2

    # Determine punch type
    if forced_type:
        punch_type = forced_type
    elif user_id in _pending_line_punches:
        punch_type = _pending_line_punches.pop(user_id)
    else:
        # Auto-detect: if no clock-in today → in, else out
        with get_db() as conn:
            today_records = conn.execute("""
                SELECT punch_type FROM punch_records
                WHERE staff_id=%s
                  AND (punched_at AT TIME ZONE 'Asia/Taipei')::date
                    = (NOW() AT TIME ZONE 'Asia/Taipei')::date
                ORDER BY punched_at DESC LIMIT 1
            """, (staff['id'],)).fetchone()
        if not today_records:
            punch_type = 'in'
        elif today_records['punch_type'] == 'in':
            punch_type = 'out'
        elif today_records['punch_type'] == 'break_out':
            punch_type = 'break_in'
        else:
            punch_type = 'in'

    label = PUNCH_LABEL.get(punch_type, punch_type)

    # GPS check
    gps_distance = None
    matched_name = ''
    if lat is not None and lng is not None:
        with get_db() as conn:
            pcfg = conn.execute("SELECT * FROM punch_config WHERE id=1").fetchone()
            locs = conn.execute(
                "SELECT * FROM punch_locations WHERE active=TRUE"
            ).fetchall()

        gps_required = pcfg['gps_required'] if pcfg else False
        if locs:
            min_dist, min_loc = None, None
            for loc in locs:
                d = _gps_distance(lat, lng, float(loc['lat']), float(loc['lng']))
                if min_dist is None or d < min_dist:
                    min_dist, min_loc = d, loc
            gps_distance = min_dist
            matched_name = min_loc['location_name'] if min_loc else ''
            if gps_required and min_dist > int(min_loc['radius_m']):
                _send_line_punch(user_id,
                    f'❌ {label}失敗\n'
                    f'您距離「{min_loc["location_name"]}」{min_dist} 公尺\n'
                    f'超出允許範圍 {min_loc["radius_m"]} 公尺\n\n'
                    f'請確認您在正確地點後重試。')
                return

    # Duplicate check
    with get_db() as conn:
        recent = conn.execute("""
            SELECT id FROM punch_records
            WHERE staff_id=%s AND punch_type=%s
              AND punched_at > NOW() - INTERVAL '1 minute'
        """, (staff['id'], punch_type)).fetchone()
        if recent:
            _send_line_punch(user_id, f'⚠️ 1 分鐘內已打過{label}，請勿重複打卡。')
            return

        row = conn.execute("""
            INSERT INTO punch_records
              (staff_id, punch_type, latitude, longitude, gps_distance, location_name)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING *
        """, (staff['id'], punch_type,
              lat, lng, gps_distance, matched_name)).fetchone()

    from datetime import timezone as _tz, timedelta as _td2
    TW = _tz(_td2(hours=8))
    now = _dt.now(TW)
    time_str = now.strftime('%H:%M')
    date_str = now.strftime('%Y/%m/%d')
    gps_info = f'\n📍 {matched_name} ({gps_distance}m)' if gps_distance is not None else ''

    _send_line_punch(user_id,
        f'✅ {label}成功\n'
        f'👤 {staff["name"]}\n'
        f'🕐 {date_str} {time_str}'
        f'{gps_info}')


def _send_status(staff, user_id):
    with get_db() as conn:
        rows = conn.execute("""
            SELECT punch_type, punched_at, gps_distance, location_name, is_manual
            FROM punch_records
            WHERE staff_id=%s
              AND (punched_at AT TIME ZONE 'Asia/Taipei')::date
                = (NOW() AT TIME ZONE 'Asia/Taipei')::date
            ORDER BY punched_at ASC
        """, (staff['id'],)).fetchall()

    LABEL = {'in':'上班','out':'下班','break_out':'休息開始','break_in':'休息結束'}
    if not rows:
        _send_line_punch(user_id, f'📋 {staff["name"]} 今日尚無打卡記錄。')
        return

    from datetime import timezone as _tz2, timedelta as _td3
    TW2 = _tz2(_td3(hours=8))
    lines = [f'📋 {staff["name"]} 今日打卡記錄']
    for r in rows:
        if r['punched_at']:
            pa = r['punched_at']
            if pa.tzinfo is None:
                pa = pa.replace(tzinfo=_tz2(_td3(0)))  # treat as UTC
            t = pa.astimezone(TW2).strftime('%H:%M')
        else:
            t = ''
        label = LABEL.get(r['punch_type'], r['punch_type'])
        dist  = f' ({r["gps_distance"]}m)' if r['gps_distance'] is not None else ''
        manual= ' [補打]' if r['is_manual'] else ''
        lines.append(f'• {label} {t}{dist}{manual}')

    _send_line_punch(user_id, '\n'.join(lines))


# ── Admin LINE Punch Config API ────────────────────────────────────

@app.route('/api/line-punch/config', methods=['GET'])
@login_required
def api_line_punch_config_get():
    with get_db() as conn:
        row = conn.execute("SELECT * FROM line_punch_config WHERE id=1").fetchone()
    if not row:
        return jsonify({'enabled': False, 'channel_access_token': '', 'channel_secret': ''})
    d = dict(row)
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    # Mask token for display
    tok = d.get('channel_access_token','')
    d['channel_access_token_masked'] = tok[:8]+'...' + tok[-4:] if len(tok)>12 else ('***' if tok else '')
    return jsonify(d)

@app.route('/api/line-punch/config', methods=['PUT'])
@login_required
def api_line_punch_config_put():
    b = request.get_json(force=True)
    token   = b.get('channel_access_token','').strip()
    secret  = b.get('channel_secret','').strip()
    enabled = bool(b.get('enabled', False))
    with get_db() as conn:
        conn.execute("""
            UPDATE line_punch_config
            SET channel_access_token=%s, channel_secret=%s, enabled=%s, updated_at=NOW()
            WHERE id=1
        """, (token, secret, enabled))
    return jsonify({'ok': True, 'enabled': enabled})

@app.route('/api/line-punch/staff', methods=['GET'])
@login_required
def api_line_punch_staff():
    """List staff with LINE binding status."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, name, username, role, active, line_user_id
            FROM punch_staff ORDER BY name
        """).fetchall()
    return jsonify([{
        'id': r['id'], 'name': r['name'], 'username': r['username'],
        'role': r['role'], 'active': r['active'],
        'line_bound': bool(r['line_user_id']),
        'line_user_id': r['line_user_id'] or ''
    } for r in rows])

@app.route('/api/line-punch/staff/<int:sid>/unbind', methods=['POST'])
@login_required
def api_line_punch_unbind(sid):
    with get_db() as conn:
        conn.execute("UPDATE punch_staff SET line_user_id=NULL WHERE id=%s", (sid,))
    return jsonify({'ok': True})



# ═══════════════════════════════════════════════════════════════════
# Salary Module API  (uses punch_staff as employee source)
# ═══════════════════════════════════════════════════════════════════
import ast as _ast

def _safe_eval(formula, ctx):
    allowed = {k: v for k, v in ctx.items()}
    allowed.update({'round': round, 'max': max, 'min': min,
                    'abs': abs, 'int': int, 'float': float})
    try:
        tree = _ast.parse(formula, mode='eval')
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Call):
                if not (isinstance(node.func, _ast.Name) and node.func.id in allowed):
                    return 0
            elif isinstance(node, _ast.Name):
                if node.id not in allowed:
                    return 0
        return round(float(eval(compile(tree, '<f>', 'eval'),
                               {"__builtins__": {}}, allowed)), 2)
    except Exception:
        return 0

def _calc_service_years(hire_date):
    if not hire_date: return 0
    from datetime import date as _date
    return (_date.today() - hire_date).days // 365

def _is_birth_month(birth_date, month_str):
    return bool(birth_date and birth_date.month == int(month_str[5:7]))

def _compute_item_amount(comp, staff, month_str):
    ctx = {
        'base_salary':    float(staff.get('base_salary') or 0),
        'insured_salary': float(staff.get('insured_salary') or 0),
        'service_years':  _calc_service_years(staff.get('hire_date')),
    }
    if comp['calc_type'] == 'formula' and comp['formula']:
        return _safe_eval(comp['formula'], ctx)
    return float(comp['default_amount'] or 0)

def sal_emp_row(row):
    if not row: return None
    d = dict(row)
    for f in ['base_salary', 'insured_salary']:
        try:
            if d.get(f) is not None: d[f] = float(d[f])
        except Exception: d[f] = 0
    for f in ['hire_date', 'birth_date']:
        try:
            if d.get(f): d[f] = d[f].isoformat()
        except Exception: d[f] = None
    for f in ['created_at', 'updated_at']:
        try:
            if d.get(f): d[f] = d[f].isoformat()
        except Exception: d[f] = None
    # Fill defaults for any missing salary columns
    d.setdefault('employee_code', '')
    d.setdefault('department', '')
    d.setdefault('position_title', '')
    d.setdefault('base_salary', 0)
    d.setdefault('insured_salary', 0)
    d.setdefault('salary_notes', '')
    d.setdefault('hire_date', None)
    d.setdefault('birth_date', None)
    return d

def sal_rec_row(row, items=None):
    if not row: return None
    d = dict(row)
    for f in ['gross_pay', 'total_deductions', 'net_pay']:
        if d.get(f) is not None: d[f] = float(d[f])
    if d.get('pay_date'):   d['pay_date']   = d['pay_date'].isoformat()
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    if items is not None:
        d['items'] = [{'id': i['id'], 'component_id': i['component_id'],
                       'component_name': i['component_name'],
                       'comp_type': i['comp_type'],
                       'amount': float(i['amount']),
                       'note': i['note']} for i in items]
    return d

# ── Staff salary profile (edit punch_staff salary fields) ──────────

@app.route('/api/salary/employees', methods=['GET'])
@login_required
def api_sal_emp_list():
    """List all punch_staff as salary employees. Auto-runs column migrations."""
    # Ensure salary columns exist on punch_staff (idempotent)
    salary_migrations = [
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS employee_code TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS department TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS position_title TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hire_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS birth_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS base_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS insured_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_notes TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS staff_id INT",
    ]
    for sql in salary_migrations:
        try:
            with get_db() as conn:
                conn.execute(sql)
        except Exception:
            pass

    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, name, username, role, active,
                   COALESCE(employee_code,'') as employee_code,
                   COALESCE(department,'')    as department,
                   COALESCE(position_title,'') as position_title,
                   hire_date, birth_date,
                   COALESCE(base_salary,0)    as base_salary,
                   COALESCE(insured_salary,0) as insured_salary,
                   COALESCE(salary_notes,'')  as salary_notes,
                   created_at,
                   updated_at
            FROM punch_staff
            ORDER BY name
        """).fetchall()
    return jsonify([sal_emp_row(r) for r in rows])

@app.route('/api/salary/employees/<int:eid>', methods=['PUT'])
@login_required
def api_sal_emp_update(eid):
    """Update salary fields on a punch_staff member."""
    b = request.get_json(force=True)
    # Ensure columns exist before updating
    for sql in [
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS employee_code TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS department TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS position_title TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hire_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS birth_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS base_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS insured_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_notes TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
    ]:
        try:
            with get_db() as conn: conn.execute(sql)
        except Exception: pass

    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_staff SET
              employee_code=%s, department=%s, position_title=%s,
              hire_date=%s, birth_date=%s,
              base_salary=%s, insured_salary=%s,
              daily_hours=%s, ot_rate1=%s, ot_rate2=%s,
              salary_type=%s, hourly_rate=%s,
              vacation_quota=%s,
              salary_notes=%s, updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (b.get('employee_code','').strip(),
              b.get('department','').strip(),
              b.get('position_title','').strip(),
              b.get('hire_date') or None,
              b.get('birth_date') or None,
              float(b.get('base_salary') or 0),
              float(b.get('insured_salary') or 0),
              float(b.get('daily_hours') or 8),
              float(b.get('ot_rate1') or 1.33),
              float(b.get('ot_rate2') or 1.67),
              b.get('salary_type','monthly'),
              float(b.get('hourly_rate') or 0),
              int(b['vacation_quota']) if b.get('vacation_quota') not in (None, '', 'null') else None,
              b.get('salary_notes','').strip(),
              eid)).fetchone()
    return jsonify(sal_emp_row(row)) if row else ('', 404)

# ── Salary Components ──────────────────────────────────────────────

@app.route('/api/salary/components', methods=['GET'])
@login_required
def api_sal_comp_list():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM salary_components ORDER BY sort_order, id"
        ).fetchall()
    return jsonify([dict(r) | {'default_amount': float(r['default_amount'])} for r in rows])

@app.route('/api/salary/components', methods=['POST'])
@login_required
def api_sal_comp_create():
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO salary_components
              (name,comp_type,calc_type,formula,default_amount,sort_order,is_birthday,description)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
        """, (b['name'], b['comp_type'], b.get('calc_type','fixed'),
              b.get('formula',''), float(b.get('default_amount') or 0),
              int(b.get('sort_order') or 0), bool(b.get('is_birthday', False)),
              b.get('description',''))).fetchone()
    return jsonify(dict(row)), 201

@app.route('/api/salary/components/<int:cid>', methods=['PUT'])
@login_required
def api_sal_comp_update(cid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE salary_components SET
              name=%s,comp_type=%s,calc_type=%s,formula=%s,
              default_amount=%s,sort_order=%s,is_birthday=%s,active=%s,description=%s
            WHERE id=%s RETURNING *
        """, (b['name'], b['comp_type'], b.get('calc_type','fixed'),
              b.get('formula',''), float(b.get('default_amount') or 0),
              int(b.get('sort_order') or 0), bool(b.get('is_birthday', False)),
              bool(b.get('active', True)), b.get('description',''), cid)).fetchone()
    return jsonify(dict(row)) if row else ('', 404)

@app.route('/api/salary/components/<int:cid>', methods=['DELETE'])
@login_required
def api_sal_comp_delete(cid):
    with get_db() as conn:
        conn.execute("DELETE FROM salary_components WHERE id=%s", (cid,))
    return jsonify({'deleted': cid})

# ── Salary Records (staff_id references punch_staff.id) ────────────

@app.route('/api/salary/records/<month>', methods=['GET'])
@login_required
def api_sal_records_month(month):
    with get_db() as conn:
        recs = conn.execute("""
            SELECT sr.*, ps.name as emp_name,
                   COALESCE(ps.employee_code,'') as employee_code,
                   COALESCE(ps.department,'')    as department,
                   COALESCE(ps.position_title,'') as position
            FROM salary_records sr
            JOIN punch_staff ps ON ps.id = sr.staff_id
            WHERE sr.month=%s
            ORDER BY ps.name
        """, (month,)).fetchall()
        result = []
        for r in recs:
            items = conn.execute(
                "SELECT * FROM salary_record_items WHERE record_id=%s ORDER BY comp_type, id",
                (r['id'],)
            ).fetchall()
            d = sal_rec_row(r, items)
            d['emp_name']      = r['emp_name']
            d['employee_code'] = r['employee_code']
            d['department']    = r['department']
            d['position']      = r['position']
            result.append(d)
    return jsonify(result)

def _calc_overtime(conn, staff_id, month, base_salary, daily_hours, ot_rate1, ot_rate2):
    """
    Calculate overtime pay for a staff member in a given month.
    Returns (ot_hours_1, ot_hours_2, ot_pay) where:
      ot_hours_1 = hours at 1.33x  (first 2h/day over daily_hours)
      ot_hours_2 = hours at 1.67x  (hours beyond first 2h overtime)
    """
    from datetime import timezone as _tz_ot, timedelta as _td_ot
    TW_OT = _tz_ot(_td_ot(hours=8))

    # Get all punch records for this month
    rows = conn.execute("""
        SELECT punch_type,
               punched_at AT TIME ZONE 'Asia/Taipei' as punched_tw
        FROM punch_records
        WHERE staff_id = %s
          AND to_char(punched_at AT TIME ZONE 'Asia/Taipei', 'YYYY-MM') = %s
        ORDER BY punched_at ASC
    """, (staff_id, month)).fetchall()

    # Group by date, find clock_in / clock_out per day
    from collections import defaultdict
    daily = defaultdict(dict)
    for r in rows:
        dt = r['punched_tw']
        ds = dt.strftime('%Y-%m-%d')
        pt = r['punch_type']
        t  = dt
        if pt == 'in' and 'in' not in daily[ds]:
            daily[ds]['in'] = t
        elif pt == 'out':
            daily[ds]['out'] = t
        elif pt == 'break_out' and 'break_out' not in daily[ds]:
            daily[ds]['break_out'] = t
        elif pt == 'break_in' and 'break_in' not in daily[ds]:
            daily[ds]['break_in'] = t

    hourly_rate = float(base_salary) / (30 * float(daily_hours)) if base_salary and daily_hours else 0
    total_ot1 = 0.0   # hours at rate1
    total_ot2 = 0.0   # hours at rate2

    for ds, times in daily.items():
        if 'in' not in times or 'out' not in times:
            continue
        # Total worked hours
        total_secs = (times['out'] - times['in']).total_seconds()
        # Subtract break time if both recorded
        if 'break_out' in times and 'break_in' in times:
            break_secs = (times['break_in'] - times['break_out']).total_seconds()
            if 0 < break_secs < 7200:   # sanity: break < 2h
                total_secs -= break_secs
        worked_hours = total_secs / 3600
        if worked_hours <= 0:
            continue
        ot = max(0, worked_hours - float(daily_hours))
        if ot <= 0:
            continue
        ot1 = min(ot, 2.0)       # first 2h → rate1
        ot2 = max(0, ot - 2.0)   # beyond 2h → rate2
        total_ot1 += ot1
        total_ot2 += ot2

    ot_pay = round(hourly_rate * (total_ot1 * float(ot_rate1) + total_ot2 * float(ot_rate2)), 0)
    return round(total_ot1, 2), round(total_ot2, 2), ot_pay


@app.route('/api/salary/records/<month>/generate', methods=['POST'])
@login_required
def api_sal_generate(month):
    with get_db() as conn:
        # Ensure overtime columns exist
        for _ot_sql in [
            "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS daily_hours NUMERIC(4,1) DEFAULT 8",
            "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate1 NUMERIC(4,2) DEFAULT 1.33",
            "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate2 NUMERIC(4,2) DEFAULT 1.67",
        ]:
            try: conn.execute(_ot_sql)
            except Exception: pass

        staff_list = conn.execute("""
            SELECT id, name,
                   COALESCE(employee_code,'') as employee_code,
                   COALESCE(department,'')    as department,
                   COALESCE(base_salary,0)    as base_salary,
                   COALESCE(insured_salary,0) as insured_salary,
                   COALESCE(daily_hours,8)    as daily_hours,
                   COALESCE(ot_rate1,1.33)    as ot_rate1,
                   COALESCE(ot_rate2,1.67)    as ot_rate2,
                   salary_type, hourly_rate,
                   vacation_quota,
                   hire_date, birth_date, active
            FROM punch_staff WHERE active=TRUE ORDER BY name
        """).fetchall()
        comps = conn.execute(
            "SELECT * FROM salary_components WHERE active=TRUE ORDER BY sort_order"
        ).fetchall()

        generated, skipped = [], []
        for staff in staff_list:
            existing = conn.execute(
                "SELECT id, status FROM salary_records WHERE staff_id=%s AND month=%s",
                (staff['id'], month)
            ).fetchone()
            if existing and existing['status'] == 'confirmed':
                skipped.append(staff['name']); continue

            birth_ok = _is_birth_month(staff.get('birth_date'), month)
            items_data = []
            for comp in comps:
                if comp['is_birthday'] and not birth_ok: continue
                amount = _compute_item_amount(comp, staff, month)
                items_data.append({
                    'component_id': comp['id'], 'component_name': comp['name'],
                    'comp_type': comp['comp_type'], 'amount': amount,
                    'note': '生日禮金' if comp['is_birthday'] else ''
                })

            # ── Overtime calculation from punch records ──────────
            try:
                ot1_h, ot2_h, ot_pay = _calc_overtime(
                    conn, staff['id'], month,
                    staff.get('base_salary', 0),
                    staff.get('daily_hours', 8),
                    staff.get('ot_rate1', 1.33),
                    staff.get('ot_rate2', 1.67)
                )
                if ot_pay > 0:
                    note_parts = []
                    if ot1_h > 0: note_parts.append(f'{ot1_h}h×{staff.get("ot_rate1",1.33)}')
                    if ot2_h > 0: note_parts.append(f'{ot2_h}h×{staff.get("ot_rate2",1.67)}')
                    items_data.append({
                        'component_id': None,
                        'component_name': '加班費',
                        'comp_type': 'allowance',
                        'amount': ot_pay,
                        'note': '、'.join(note_parts)
                    })
            except Exception as _ot_e:
                print(f"[OT CALC] {staff['name']}: {_ot_e}")
            # ── Overtime from approved OT requests ───────────────
            try:
                approved_ots = conn.execute(
                    """SELECT ot_hours, ot_pay, day_type,
                           start_time::text as st, end_time::text as et
                    FROM overtime_requests
                    WHERE staff_id=%s
                      AND to_char(request_date,'YYYY-MM')=%s
                      AND status='approved' AND ot_pay > 0""",
                    (staff['id'], month)
                ).fetchall()
                for ot in approved_ots:
                    dt_label = {'weekday':'平日','rest_day':'休息日',
                                'holiday':'國定假日','special':'例假日'}.get(
                                ot['day_type'] or 'weekday','')
                    items_data.append({
                        'component_id': None,
                        'component_name': '加班費（申請）',
                        'comp_type': 'allowance',
                        'amount': float(ot['ot_pay']),
                        'note': f"{dt_label} {str(ot['st'])[:5]}~{str(ot['et'])[:5]} {float(ot['ot_hours'])}h"
                    })
            except Exception as _ot_e2:
                print(f"[OT REQ] {staff['name']}: {_ot_e2}")

            gross  = sum(i['amount'] for i in items_data if i['comp_type'] == 'allowance')
            deduct = sum(i['amount'] for i in items_data if i['comp_type'] == 'deduction')
            net    = gross - deduct

            if existing:
                rid = existing['id']
                conn.execute(
                    "UPDATE salary_records SET gross_pay=%s,total_deductions=%s,net_pay=%s,updated_at=NOW() WHERE id=%s",
                    (gross, deduct, net, rid))
                conn.execute("DELETE FROM salary_record_items WHERE record_id=%s", (rid,))
            else:
                row = conn.execute("""
                    INSERT INTO salary_records (staff_id,month,gross_pay,total_deductions,net_pay)
                    VALUES (%s,%s,%s,%s,%s) RETURNING id
                """, (staff['id'], month, gross, deduct, net)).fetchone()
                rid = row['id']

            for it in items_data:
                conn.execute("""
                    INSERT INTO salary_record_items
                      (record_id,component_id,component_name,comp_type,amount,note)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (rid, it['component_id'], it['component_name'],
                      it['comp_type'], it['amount'], it['note']))
            generated.append(staff['name'])

    return jsonify({'generated': generated, 'skipped': skipped})

@app.route('/api/salary/records/<int:rid>', methods=['GET'])
@login_required
def api_sal_record_get(rid):
    with get_db() as conn:
        r = conn.execute("""
            SELECT sr.*, ps.name as emp_name,
                   COALESCE(ps.employee_code,'') as employee_code,
                   COALESCE(ps.department,'')    as department,
                   COALESCE(ps.position_title,'') as position,
                   COALESCE(ps.base_salary,0)    as base_salary,
                   ps.hire_date, ps.birth_date
            FROM salary_records sr
            JOIN punch_staff ps ON ps.id=sr.staff_id
            WHERE sr.id=%s
        """, (rid,)).fetchone()
        if not r: return ('', 404)
        items = conn.execute(
            "SELECT * FROM salary_record_items WHERE record_id=%s ORDER BY comp_type, id",
            (rid,)
        ).fetchall()
    d = sal_rec_row(r, items)
    d['emp_name']      = r['emp_name']
    d['employee_code'] = r['employee_code']
    d['department']    = r['department']
    d['position']      = r['position']
    d['base_salary']   = float(r['base_salary']) if r['base_salary'] else 0
    d['hire_date']     = r['hire_date'].isoformat()  if r['hire_date']  else None
    d['service_years'] = _calc_service_years(r.get('hire_date'))
    return jsonify(d)

@app.route('/api/salary/records/<int:rid>', methods=['PUT'])
@login_required
def api_sal_record_update(rid):
    b      = request.get_json(force=True)
    items  = b.get('items', [])
    gross  = sum(float(i['amount']) for i in items if i['comp_type'] == 'allowance')
    deduct = sum(float(i['amount']) for i in items if i['comp_type'] == 'deduction')
    net    = gross - deduct
    with get_db() as conn:
        conn.execute("""
            UPDATE salary_records SET
              gross_pay=%s, total_deductions=%s, net_pay=%s,
              pay_date=%s, status=%s, notes=%s, updated_at=NOW()
            WHERE id=%s
        """, (gross, deduct, net,
              b.get('pay_date') or None, b.get('status','draft'),
              b.get('notes','').strip(), rid))
        conn.execute("DELETE FROM salary_record_items WHERE record_id=%s", (rid,))
        for it in items:
            conn.execute("""
                INSERT INTO salary_record_items
                  (record_id,component_id,component_name,comp_type,amount,note)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (rid, it.get('component_id'), it['component_name'],
                  it['comp_type'], float(it['amount']), it.get('note','')))
    return jsonify({'ok': True, 'net_pay': net})

@app.route('/api/salary/records/<int:rid>/confirm', methods=['POST'])
@login_required
def api_sal_record_confirm(rid):
    with get_db() as conn:
        conn.execute(
            "UPDATE salary_records SET status='confirmed', updated_at=NOW() WHERE id=%s", (rid,))
    return jsonify({'ok': True})

@app.route('/api/salary/records/<int:rid>', methods=['DELETE'])
@login_required
def api_sal_record_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM salary_records WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})

# ── Employee self-service ──────────────────────────────────────────

@app.route('/api/salary/my-records', methods=['GET'])
def api_sal_my_records():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    month = request.args.get('month', '')
    with get_db() as conn:
        emp = conn.execute(
            "SELECT * FROM punch_staff WHERE id=%s AND active=TRUE", (sid,)
        ).fetchone()
        if not emp: return jsonify({'records': [], 'employee': None})

        conds  = ["sr.staff_id=%s", "sr.status='confirmed'"]
        params = [sid]
        if month:
            conds.append("sr.month=%s"); params.append(month)

        recs = conn.execute(f"""
            SELECT sr.* FROM salary_records sr
            WHERE {' AND '.join(conds)}
            ORDER BY sr.month DESC LIMIT 24
        """, params).fetchall()

        records = []
        for r in recs:
            items = conn.execute(
                "SELECT * FROM salary_record_items WHERE record_id=%s ORDER BY comp_type, id",
                (r['id'],)
            ).fetchall()
            records.append(sal_rec_row(r, items))

    emp_dict = sal_emp_row(emp)
    return jsonify({'records': records, 'employee': emp_dict})



# ── Rich Menu ──────────────────────────────────────────────────────

def _call_line_api(cfg, method, path, body=None):
    """Call LINE Messaging API, return (status_code, response_dict)."""
    import json as _j
    token = cfg.get('channel_access_token','')
    url   = 'https://api.line.me/v2/bot' + path
    data  = _j.dumps(body).encode('utf-8') if body else None
    req   = urllib.request.Request(url, data=data, method=method,
                headers={
                    'Content-Type':  'application/json',
                    'Authorization': f'Bearer {token}'
                })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, _j.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, {'error': e.read().decode('utf-8', errors='replace')}
    except Exception as e:
        return 0, {'error': str(e)}


def _build_richmenu_body(gps_required, staff_url=''):
    """Build the Rich Menu JSON for punch buttons.
    Layout with custom image:
      Top-Left:     上班打卡 (Clock In)
      Top-Right:    下班打卡 (Clock Out)
      Center:       員工系統連結 (opens staff URL)
      Bottom-Left:  休息開始 (Break Out)
      Bottom-Right: 休息結束 (Break In)
    """
    # Center area: 700x400 pixels centered in 2500x843
    cx, cy = 900, 221
    cw, ch = 700, 400

    # Four corner areas avoid the center
    areas = [
        # Top-Left: Clock In
        {"bounds": {"x": 0,    "y": 0,   "width": 900,  "height": 421},
         "action": {"type": "message", "text": "上班"}},
        # Top-Right: Clock Out
        {"bounds": {"x": 1600, "y": 0,   "width": 900,  "height": 421},
         "action": {"type": "message", "text": "下班"}},
        # Bottom-Left: Break Out
        {"bounds": {"x": 0,    "y": 422, "width": 900,  "height": 421},
         "action": {"type": "message", "text": "休息"}},
        # Bottom-Right: Break In
        {"bounds": {"x": 1600, "y": 422, "width": 900,  "height": 421},
         "action": {"type": "message", "text": "回來"}},
        # Center: Staff system link
        {"bounds": {"x": cx, "y": cy, "width": cw, "height": ch},
         "action": {"type": "uri", "uri": staff_url or "https://example.com/staff"}},
    ]

    return {
        "size":       {"width": 2500, "height": 843},
        "selected":   True,
        "name":       "舒室圈打卡選單",
        "chatBarText": "打卡",
        "areas": areas
    }


def _find_cjk_font():
    """Find a CJK font on this system, return path or None."""
    import os, subprocess
    candidates = [
        # Noto CJK variants
        '/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc',
        '/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc',
        '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
        '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
        '/usr/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf',
        '/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc',
        # wqy
        '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
        '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
        # arphic
        '/usr/share/fonts/truetype/arphic/ukai.ttc',
        '/usr/share/fonts/truetype/arphic/uming.ttc',
        # Render might have these
        '/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf',
        '/usr/share/fonts/truetype/fonts-japanese-gothic.ttf',
    ]
    for p in candidates:
        if os.path.exists(p):
            print(f"[RICHMENU] Found font: {p}")
            return p
    # Try fc-list as last resort
    try:
        out = subprocess.check_output(
            ['fc-list', ':lang=zh', '--format=%{file}\n'], timeout=5
        ).decode('utf-8', errors='ignore')
        for line in out.strip().split('\n'):
            line = line.strip()
            if line and os.path.exists(line):
                print(f"[RICHMENU] fc-list font: {line}")
                return line
    except Exception:
        pass
    print("[RICHMENU] No CJK font found, will use embedded fallback")
    return None


def _make_richmenu_png():
    """Generate the 2500x843 rich menu PNG bytes."""
    import io, os
    from PIL import Image, ImageDraw, ImageFont

    W, H = 2500, 843
    img  = Image.new('RGB', (W, H), '#0f1c3a')
    draw = ImageDraw.Draw(img)

    panels = [
        (0,    0,   1250, 421, '#2e9e6b', 'Clock In'),
        (1250, 0,   2500, 421, '#d64242', 'Clock Out'),
        (0,    422, 1250, 843, '#e07b2a', 'Break Out'),
        (1250, 422, 2500, 843, '#4a7bda', 'Break In'),
    ]

    import os

    def _get_font(size):
        # 1. Check common system paths
        SYS_PATHS = [
            '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
            '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',
            '/usr/share/fonts/truetype/freefont/FreeSansBold.ttf',
            '/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf',
            '/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf',
        ]
        for fp in SYS_PATHS:
            if os.path.exists(fp):
                try:
                    f = ImageFont.truetype(fp, size)
                    print(f"[RICHMENU] System font: {fp}")
                    return f
                except Exception:
                    pass

        # 2. Download DejaVu Bold from GitHub (open-source, ~300KB)
        cached = '/tmp/DejaVuSans-Bold.ttf'
        if not os.path.exists(cached):
            try:
                url = 'https://github.com/dejavu-fonts/dejavu-fonts/raw/main/ttf/DejaVuSans-Bold.ttf'
                print(f"[RICHMENU] Downloading font from {url}")
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = resp.read()
                with open(cached, 'wb') as fh:
                    fh.write(data)
                print(f"[RICHMENU] Font downloaded: {len(data)} bytes")
            except Exception as e:
                print(f"[RICHMENU] Font download failed: {e}")

        if os.path.exists(cached):
            try:
                f = ImageFont.truetype(cached, size)
                print(f"[RICHMENU] Downloaded font loaded")
                return f
            except Exception as e:
                print(f"[RICHMENU] Downloaded font load failed: {e}")

        # 3. Last resort: Pillow default (will look like boxes for large sizes)
        print("[RICHMENU] WARNING: Using Pillow default bitmap font")
        try:
            return ImageFont.load_default(size=size)
        except Exception:
            return ImageFont.load_default()

    font = _get_font(200)

    DIVIDER = 6
    for x1, y1, x2, y2, color, label in panels:
        draw.rectangle([x1 + DIVIDER//2, y1 + DIVIDER//2,
                        x2 - DIVIDER//2 - 1, y2 - DIVIDER//2 - 1], fill=color)
        cx = (x1 + x2) // 2
        cy = (y1 + y2) // 2
        bb = draw.textbbox((0, 0), label, font=font)
        tw, th = bb[2] - bb[0], bb[3] - bb[1]
        draw.text((cx - tw // 2, cy - th // 2), label, fill='#ffffff', font=font)

    draw.rectangle([1248, 0, 1252, H], fill='#0f1c3a')
    draw.rectangle([0, 419, W, 423],   fill='#0f1c3a')

    buf = io.BytesIO()
    img.save(buf, 'PNG', optimize=True)
    return buf.getvalue()


CUSTOM_RICHMENU_IMAGE_PATH = '/tmp/custom_richmenu.png'

def _create_richmenu_image(rich_menu_id, cfg, gps_required):
    """Upload rich menu image — uses custom image if uploaded, else auto-generates."""
    import io, os

    token = cfg.get('channel_access_token', '')
    png_bytes = None

    # 1. Try custom uploaded image first
    if os.path.exists(CUSTOM_RICHMENU_IMAGE_PATH):
        try:
            with open(CUSTOM_RICHMENU_IMAGE_PATH, 'rb') as f:
                png_bytes = f.read()
            print(f"[RICHMENU] Using custom image: {len(png_bytes)} bytes")
        except Exception as e:
            print(f"[RICHMENU] Failed to read custom image: {e}")
            png_bytes = None

    # 2. Try auto-generate with Pillow
    if not png_bytes:
        try:
            png_bytes = _make_richmenu_png()
            print(f"[RICHMENU] Generated PNG: {len(png_bytes)} bytes")
        except Exception as e:
            print(f"[RICHMENU] Pillow failed ({e}), using plain PNG fallback")
            png_bytes = None

    # 3. Final fallback: plain colored blocks (no text, pure Python)
    if not png_bytes:
        import struct, zlib

        def _png_chunk(name, data):
            c = struct.pack('>I', len(data)) + name + data
            return c + struct.pack('>I', zlib.crc32(c[4:]) & 0xffffffff)

        W, H = 2500, 843
        colors = [(0x2e,0x9e,0x6b),(0xd6,0x42,0x42),(0xe0,0x7b,0x2a),(0x4a,0x7b,0xda)]
        rows = []
        for y in range(H):
            row = bytearray()
            for x in range(W):
                p = (0 if y < 422 else 1) * 2 + (0 if x < 1250 else 1)
                r, g, b = colors[p]
                if x in (1249, 1250) or y in (421, 422):
                    r, g, b = 0x0f, 0x1c, 0x3a
                row += bytes([r, g, b])
            rows.append(bytes([0]) + bytes(row))
        compressed = zlib.compress(b''.join(rows), 1)
        png_bytes = (b'\x89PNG\r\n\x1a\n'
                     + _png_chunk(b'IHDR', struct.pack('>IIBBBBB', W, H, 8, 2, 0, 0, 0))
                     + _png_chunk(b'IDAT', compressed)
                     + _png_chunk(b'IEND', b''))
        print(f"[RICHMENU] Fallback PNG generated: {len(png_bytes)} bytes")

    # LINE requires image <= 1MB; compress if needed
    if len(png_bytes) > 1_000_000:
        try:
            from PIL import Image as _PIM
            import io as _io2
            img_obj = _PIM.open(_io2.BytesIO(png_bytes)).convert('RGB')
            buf2 = _io2.BytesIO()
            # Try JPEG which compresses much better
            img_obj.save(buf2, 'JPEG', quality=85, optimize=True)
            if buf2.tell() <= 1_000_000:
                png_bytes = buf2.getvalue()
                content_type = 'image/jpeg'
                print(f"[RICHMENU] Converted to JPEG: {len(png_bytes)} bytes")
            else:
                # Try lower quality
                buf3 = _io2.BytesIO()
                img_obj.save(buf3, 'JPEG', quality=60, optimize=True)
                png_bytes = buf3.getvalue()
                content_type = 'image/jpeg'
                print(f"[RICHMENU] Compressed JPEG q60: {len(png_bytes)} bytes")
        except Exception as ce:
            print(f"[RICHMENU] Compress failed: {ce}")
            content_type = 'image/png'
    else:
        content_type = 'image/png'

    print(f"[RICHMENU] Uploading {len(png_bytes)} bytes as {content_type} to richmenu {rich_menu_id}")
    upload_url = f'https://api-data.line.me/v2/bot/richmenu/{rich_menu_id}/content'
    req = urllib.request.Request(
        upload_url, data=png_bytes, method='POST',
        headers={'Content-Type': content_type, 'Authorization': f'Bearer {token}'}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
            print(f"[RICHMENU] Upload success: {resp.status} {body}")
            return resp.status, {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8', errors='replace')
        print(f"[RICHMENU] Upload HTTPError {e.code}: {err_body}")
        return e.code, {'error': err_body}
    except Exception as e:
        print(f"[RICHMENU] Upload exception: {e}")
        return 0, {'error': str(e)}


@app.route('/api/line-punch/richmenu/create', methods=['POST'])
@login_required
def api_richmenu_create():
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('channel_access_token'):
        return jsonify({'error': '請先設定 Channel Access Token'}), 400

    gps_required = False
    try:
        with get_db() as conn:
            pc = conn.execute("SELECT gps_required FROM punch_config WHERE id=1").fetchone()
            if pc: gps_required = bool(pc['gps_required'])
    except Exception:
        pass

    # Get staff system URL
    staff_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if staff_url:
        staff_url = staff_url.rstrip('/') + '/staff'
    else:
        staff_url = request.host_url.rstrip('/') + '/staff'

    # 1. Create rich menu
    body   = _build_richmenu_body(gps_required, staff_url)
    status, data = _call_line_api(cfg, 'POST', '/richmenu', body)
    if status != 200:
        return jsonify({'error': f'建立失敗 ({status}): {data.get("error","")}'}), 500

    rich_menu_id = data.get('richMenuId','')

    # 2. Upload image
    img_status, img_data = _create_richmenu_image(rich_menu_id, cfg, gps_required)
    if img_status not in (200, 204):
        print(f"[RICHMENU] Image upload failed: {img_status} {img_data}")
        # Continue even if image upload fails — menu still works

    # 3. Set as default
    status2, _ = _call_line_api(cfg, 'POST', f'/user/all/richmenu/{rich_menu_id}')

    # Save rich menu id to config
    with get_db() as conn:
        conn.execute(
            "UPDATE line_punch_config SET updated_at=NOW() WHERE id=1"
        )

    img_ok = img_status in (200, 204)
    return jsonify({
        'ok': True,
        'rich_menu_id': rich_menu_id,
        'image_uploaded': img_ok,
        'image_error': '' if img_ok else f'HTTP {img_status}: {img_data}',
        'set_default': status2 in (200, 204)
    })




@app.route('/api/line-punch/richmenu/upload-image', methods=['POST'])
@login_required
def api_richmenu_upload_image():
    """Upload a custom rich menu image (PNG/JPG, max 1MB)."""
    import os
    from PIL import Image as _PIL_Image
    import io as _io

    if 'image' not in request.files:
        return jsonify({'error': '請選擇圖片檔案'}), 400

    f = request.files['image']
    if not f.filename:
        return jsonify({'error': '請選擇圖片檔案'}), 400

    data = f.read()
    if len(data) > 3 * 1024 * 1024:
        return jsonify({'error': '圖片不可超過 3MB'}), 400

    try:
        img = _PIL_Image.open(_io.BytesIO(data))
        # Convert and resize to exact 2500x843
        img = img.convert('RGB')
        if img.size != (2500, 843):
            img = img.resize((2500, 843), _PIL_Image.LANCZOS)
            print(f"[RICHMENU] Resized from {img.size} to 2500x843")
        buf = _io.BytesIO()
        img.save(buf, 'PNG', optimize=True)
        png_bytes = buf.getvalue()
    except Exception as e:
        return jsonify({'error': f'圖片處理失敗：{str(e)}'}), 400

    with open(CUSTOM_RICHMENU_IMAGE_PATH, 'wb') as out:
        out.write(png_bytes)

    return jsonify({
        'ok': True,
        'size': len(png_bytes),
        'message': f'自訂圖片已儲存（{len(png_bytes)//1024} KB），下次建立圖文選單時將使用此圖片'
    })

@app.route('/api/line-punch/richmenu/delete-custom-image', methods=['POST'])
@login_required
def api_richmenu_delete_custom():
    """Remove custom image, revert to auto-generated."""
    import os
    if os.path.exists(CUSTOM_RICHMENU_IMAGE_PATH):
        os.remove(CUSTOM_RICHMENU_IMAGE_PATH)
    return jsonify({'ok': True, 'message': '已刪除自訂圖片，將使用自動產生的圖片'})

@app.route('/api/line-punch/richmenu/has-custom-image', methods=['GET'])
@login_required
def api_richmenu_has_custom():
    import os
    exists = os.path.exists(CUSTOM_RICHMENU_IMAGE_PATH)
    size   = os.path.getsize(CUSTOM_RICHMENU_IMAGE_PATH) if exists else 0
    return jsonify({'has_custom': exists, 'size_kb': size // 1024})

@app.route('/api/line-punch/richmenu/preview-image')
@login_required
def api_richmenu_preview_image():
    """Return the rich menu PNG directly for browser preview/download."""
    try:
        from flask import Response
        png_bytes = _make_richmenu_png()
        return Response(png_bytes, mimetype='image/png',
                        headers={'Content-Disposition': 'inline; filename="richmenu.png"'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/line-punch/richmenu/list', methods=['GET'])
@login_required
def api_richmenu_list():
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('channel_access_token'):
        return jsonify({'menus': []})
    status, data = _call_line_api(cfg, 'GET', '/richmenu/list')
    if status != 200:
        return jsonify({'menus': [], 'error': data.get('error','')})
    return jsonify({'menus': data.get('richmenus', [])})


@app.route('/api/line-punch/richmenu/<rich_menu_id>', methods=['DELETE'])
@login_required
def api_richmenu_delete(rich_menu_id):
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('channel_access_token'):
        return jsonify({'error': '未設定 Token'}), 400
    # Cancel default first
    _call_line_api(cfg, 'DELETE', '/user/all/richmenu')
    # Delete menu
    status, data = _call_line_api(cfg, 'DELETE', f'/richmenu/{rich_menu_id}')
    return jsonify({'ok': status in (200,204), 'status': status})


@app.route('/api/line-punch/richmenu/default', methods=['DELETE'])
@login_required
def api_richmenu_unset_default():
    cfg = get_line_punch_config()
    if not cfg or not cfg.get('channel_access_token'):
        return jsonify({'error': '未設定 Token'}), 400
    status, _ = _call_line_api(cfg, 'DELETE', '/user/all/richmenu')
    return jsonify({'ok': status in (200,204)})


# ═══════════════════════════════════════════════════════════════════
# Punch Request (補打卡申請) API
# ═══════════════════════════════════════════════════════════════════

def punch_req_row(row):
    if not row: return None
    d = dict(row)
    if d.get('requested_at'): d['requested_at'] = d['requested_at'].isoformat()
    if d.get('reviewed_at'):  d['reviewed_at']  = d['reviewed_at'].isoformat()
    if d.get('created_at'):   d['created_at']   = d['created_at'].isoformat()
    return d

# ── Employee: submit request ───────────────────────────────────────

@app.route('/api/punch/request', methods=['POST'])
def api_punch_req_submit():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b = request.get_json(force=True)
    punch_type   = b.get('punch_type')
    requested_at = b.get('requested_at')   # ISO datetime string (Taiwan time)
    reason       = b.get('reason','').strip()

    if punch_type not in ('in','out','break_out','break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400
    if not requested_at:
        return jsonify({'error': '請選擇補打時間'}), 400

    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO punch_requests (staff_id, punch_type, requested_at, reason)
            VALUES (%s, %s, %s, %s) RETURNING *
        """, (sid, punch_type, requested_at, reason)).fetchone()
    return jsonify(punch_req_row(row)), 201

@app.route('/api/punch/request/my', methods=['GET'])
def api_punch_req_my():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM punch_requests
            WHERE staff_id=%s
            ORDER BY requested_at DESC LIMIT 20
        """, (sid,)).fetchall()
    return jsonify([punch_req_row(r) for r in rows])

# ── Admin: list / approve / reject ────────────────────────────────

@app.route('/api/punch/requests', methods=['GET'])
@login_required
def api_punch_reqs_list():
    status = request.args.get('status','')
    conds, params = ['TRUE'], []
    if status: conds.append('pr.status=%s'); params.append(status)
    where = ' AND '.join(conds)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT pr.*, ps.name as staff_name, ps.role as staff_role
            FROM punch_requests pr
            JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE {where}
            ORDER BY pr.created_at DESC LIMIT 200
        """, params).fetchall()
    return jsonify([punch_req_row(r) for r in rows])

@app.route('/api/punch/requests/<int:rid>', methods=['PUT'])
@login_required
def api_punch_req_review(rid):
    b           = request.get_json(force=True)
    action      = b.get('action')       # 'approve' | 'reject'
    reviewed_by = b.get('reviewed_by','').strip()
    review_note = b.get('review_note','').strip()

    if action not in ('approve','reject'):
        return jsonify({'error': 'invalid action'}), 400

    new_status = 'approved' if action == 'approve' else 'rejected'

    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_requests
            SET status=%s, reviewed_by=%s, review_note=%s, reviewed_at=NOW()
            WHERE id=%s RETURNING *, (SELECT name FROM punch_staff WHERE id=staff_id) as staff_name
        """, (new_status, reviewed_by, review_note, rid)).fetchone()

        if not row: return ('', 404)

        # If approved → insert actual punch record
        if action == 'approve':
            conn.execute("""
                INSERT INTO punch_records
                  (staff_id, punch_type, punched_at, note, is_manual, manual_by)
                VALUES (%s, %s, %s, %s, TRUE, %s)
            """, (row['staff_id'], row['punch_type'], row['requested_at'],
                  f'補打卡申請 #{rid}：{row["reason"]}', reviewed_by))

    return jsonify(punch_req_row(row))

@app.route('/api/punch/requests/<int:rid>', methods=['DELETE'])
@login_required
def api_punch_req_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_requests WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})


@app.route('/api/punch/my-records', methods=['GET'])
def api_punch_my_records():
    """Employee self-service: get own punch records for a month."""
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': 'not logged in'}), 401
    month = request.args.get('month', '')   # e.g. 2026-03
    if not month:
        from datetime import datetime as _dt2
        from datetime import timezone as _tz3, timedelta as _td4
        TW3 = _tz3(_td4(hours=8))
        month = _dt2.now(TW3).strftime('%Y-%m')

    with get_db() as conn:
        rows = conn.execute("""
            SELECT punch_type, punched_at, gps_distance, location_name, is_manual
            FROM punch_records
            WHERE staff_id=%s
              AND to_char(punched_at AT TIME ZONE 'Asia/Taipei', 'YYYY-MM') = %s
            ORDER BY punched_at ASC
        """, (sid, month)).fetchall()

    from datetime import timezone as _tz4, timedelta as _td5
    TW4 = _tz4(_td5(hours=8))
    result = {}
    LABEL = {'in':'上班','out':'下班','break_out':'休息開始','break_in':'休息結束'}
    for r in rows:
        pa = r['punched_at']
        if pa.tzinfo is None:
            from datetime import timezone as _utz
            pa = pa.replace(tzinfo=_utz.utc)
        pa_tw = pa.astimezone(TW4)
        date_str = pa_tw.strftime('%Y-%m-%d')
        time_str = pa_tw.strftime('%H:%M')
        if date_str not in result:
            result[date_str] = []
        result[date_str].append({
            'type':          r['punch_type'],
            'label':         LABEL.get(r['punch_type'], r['punch_type']),
            'time':          time_str,
            'gps_distance':  r['gps_distance'],
            'location_name': r['location_name'] or '',
            'is_manual':     bool(r['is_manual']),
        })

    return jsonify({'month': month, 'records': result})


# ═══════════════════════════════════════════════════════════════════
# Shift Schedule API
# ═══════════════════════════════════════════════════════════════════

def shift_type_row(row):
    if not row: return None
    d = dict(row)
    if d.get('start_time'): d['start_time'] = str(d['start_time'])[:5]
    if d.get('end_time'):   d['end_time']   = str(d['end_time'])[:5]
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d

def shift_assign_row(row):
    if not row: return None
    d = dict(row)
    if d.get('shift_date'): d['shift_date'] = d['shift_date'].isoformat()
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d

# ── Shift Types CRUD ───────────────────────────────────────────────

@app.route('/api/shifts/types', methods=['GET'])
@login_required
def api_shift_types_list():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM shift_types ORDER BY sort_order, id"
        ).fetchall()
    return jsonify([shift_type_row(r) for r in rows])

@app.route('/api/shifts/types/public', methods=['GET'])
def api_shift_types_public():
    """Public endpoint for employee page."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM shift_types WHERE active=TRUE ORDER BY sort_order, id"
        ).fetchall()
    return jsonify([shift_type_row(r) for r in rows])

@app.route('/api/shifts/types', methods=['POST'])
@login_required
def api_shift_type_create():
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO shift_types (name, start_time, end_time, color, departments, sort_order)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING *
        """, (b['name'], b['start_time'], b['end_time'],
              b.get('color','#4a7bda'), b.get('departments',''),
              int(b.get('sort_order',0)))).fetchone()
    return jsonify(shift_type_row(row)), 201

@app.route('/api/shifts/types/<int:sid>', methods=['PUT'])
@login_required
def api_shift_type_update(sid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE shift_types SET
              name=%s, start_time=%s, end_time=%s, color=%s,
              departments=%s, sort_order=%s, active=%s
            WHERE id=%s RETURNING *
        """, (b['name'], b['start_time'], b['end_time'],
              b.get('color','#4a7bda'), b.get('departments',''),
              int(b.get('sort_order',0)), bool(b.get('active',True)),
              sid)).fetchone()
    return jsonify(shift_type_row(row)) if row else ('', 404)

@app.route('/api/shifts/types/<int:sid>', methods=['DELETE'])
@login_required
def api_shift_type_delete(sid):
    with get_db() as conn:
        conn.execute("DELETE FROM shift_types WHERE id=%s", (sid,))
    return jsonify({'deleted': sid})

# ── Shift Assignments ──────────────────────────────────────────────

@app.route('/api/shifts/assignments', methods=['GET'])
@login_required
def api_shift_assignments_list():
    month = request.args.get('month','')
    conds, params = ['TRUE'], []
    if month:
        conds.append("to_char(sa.shift_date,'YYYY-MM')=%s")
        params.append(month)
    where = ' AND '.join(conds)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT sa.*, ps.name as staff_name, ps.role as staff_role,
                   st.name as shift_name, st.start_time, st.end_time, st.color,
                   st.departments
            FROM shift_assignments sa
            JOIN punch_staff ps ON ps.id=sa.staff_id
            JOIN shift_types  st ON st.id=sa.shift_type_id
            WHERE {where}
            ORDER BY sa.shift_date, ps.name
        """, params).fetchall()
    result = []
    for r in rows:
        d = shift_assign_row(r)
        d['staff_name']  = r['staff_name']
        d['staff_role']  = r['staff_role']
        d['shift_name']  = r['shift_name']
        d['start_time']  = str(r['start_time'])[:5]
        d['end_time']    = str(r['end_time'])[:5]
        d['color']       = r['color']
        d['departments'] = r['departments']
        result.append(d)
    return jsonify(result)

@app.route('/api/shifts/assignments', methods=['POST'])
@login_required
def api_shift_assignment_create():
    """Upsert — one shift per staff per day. Blocked if employee has approved leave."""
    b = request.get_json(force=True)
    staff_ids     = b.get('staff_ids', [])
    shift_type_id = b.get('shift_type_id')
    dates         = b.get('dates', [])
    note          = b.get('note','').strip()
    force         = bool(b.get('force', False))   # admin override flag

    if not staff_ids or not shift_type_id or not dates:
        return jsonify({'error': '請選擇員工、班別及日期'}), 400

    created  = 0
    blocked  = []   # [{staff_name, date}]

    with get_db() as conn:
        # Build leave lookup for all involved staff × dates
        # Format: {staff_id: set(date_str)}
        leave_lookup = {}
        if not force:
            for sid in staff_ids:
                # Get all approved leave dates for this staff in relevant months
                months = list({d[:7] for d in dates})
                for month in months:
                    row = conn.execute("""
                        SELECT dates FROM schedule_requests
                        WHERE staff_id=%s AND month=%s AND status='approved'
                    """, (sid, month)).fetchone()
                    if row:
                        approved_dates = row['dates'] or []
                        if isinstance(approved_dates, str):
                            import json as _j2
                            try: approved_dates = _j2.loads(approved_dates)
                            except: approved_dates = []
                        if sid not in leave_lookup:
                            leave_lookup[sid] = set()
                        leave_lookup[sid].update(approved_dates)

        # Get staff names for error messages
        staff_names = {}
        rows = conn.execute(
            "SELECT id, name FROM punch_staff WHERE id = ANY(%s::int[])",
            (staff_ids,)
        ).fetchall()
        for r in rows:
            staff_names[r['id']] = r['name']

        for sid in staff_ids:
            leave_dates = leave_lookup.get(sid, set())
            for date_str in dates:
                if date_str in leave_dates and not force:
                    blocked.append({
                        'staff_name': staff_names.get(sid, str(sid)),
                        'date': date_str
                    })
                    continue
                conn.execute("""
                    INSERT INTO shift_assignments (staff_id, shift_type_id, shift_date, note)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (staff_id, shift_date) DO UPDATE
                      SET shift_type_id=%s, note=%s, created_at=NOW()
                """, (sid, shift_type_id, date_str, note, shift_type_id, note))
                created += 1

    if blocked and created == 0:
        msgs = [f"{x['staff_name']} {x['date']}" for x in blocked]
        err_msg = '以下日期員工已有核准的排休，無法指派班別：' + '、'.join(msgs)
        return jsonify({'error': err_msg, 'blocked': blocked}), 422

    result = {'created': created}
    if blocked:
        result['warning'] = f'已指派 {created} 筆，跳過 {len(blocked)} 筆（員工當日有核准排休）'
        result['blocked'] = blocked
    return jsonify(result), 201

@app.route('/api/shifts/assignments/<int:aid>', methods=['DELETE'])
@login_required
def api_shift_assignment_delete(aid):
    with get_db() as conn:
        conn.execute("DELETE FROM shift_assignments WHERE id=%s", (aid,))
    return jsonify({'deleted': aid})

@app.route('/api/shifts/assignments/batch-delete', methods=['POST'])
@login_required
def api_shift_assignment_batch_delete():
    b = request.get_json(force=True)
    staff_ids = b.get('staff_ids', [])
    dates     = b.get('dates', [])
    if not staff_ids or not dates:
        return jsonify({'error': '請選擇員工及日期'}), 400
    deleted = 0
    with get_db() as conn:
        for sid in staff_ids:
            for date_str in dates:
                r = conn.execute(
                    "DELETE FROM shift_assignments WHERE staff_id=%s AND shift_date=%s RETURNING id",
                    (sid, date_str)
                ).fetchone()
                if r: deleted += 1
    return jsonify({'deleted': deleted})

# ── Employee self-service ─────────────────────────────────────────

@app.route('/api/shifts/my-schedule', methods=['GET'])
def api_my_shift_schedule():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    month = request.args.get('month','')
    conds = ["sa.staff_id=%s"]
    params = [sid]
    if month:
        conds.append("to_char(sa.shift_date,'YYYY-MM')=%s")
        params.append(month)
    with get_db() as conn:
        rows = conn.execute("""
            SELECT sa.shift_date, sa.note,
                   st.name as shift_name, st.start_time, st.end_time, st.color
            FROM shift_assignments sa
            JOIN shift_types st ON st.id=sa.shift_type_id
            WHERE sa.staff_id=%s
              AND to_char(sa.shift_date,'YYYY-MM')=%s
            ORDER BY sa.shift_date
        """, (sid, month) if month else (sid,)).fetchall()
    result = {}
    for r in rows:
        ds = r['shift_date'].isoformat()
        result[ds] = {
            'shift_name': r['shift_name'],
            'start_time': str(r['start_time'])[:5],
            'end_time':   str(r['end_time'])[:5],
            'color':      r['color'],
            'note':       r['note'],
        }
    return jsonify({'month': month, 'shifts': result})


# ═══════════════════════════════════════════════════════════════════
# Overtime Request API
# ═══════════════════════════════════════════════════════════════════

def ot_req_row(row):
    if not row: return None
    d = dict(row)
    if d.get('request_date'): d['request_date'] = d['request_date'].isoformat()
    if d.get('start_time'):   d['start_time']   = str(d['start_time'])[:5]
    if d.get('end_time'):     d['end_time']      = str(d['end_time'])[:5]
    if d.get('ot_pay'):       d['ot_pay']        = float(d['ot_pay'])
    if d.get('ot_hours'):     d['ot_hours']      = float(d['ot_hours'])
    if d.get('reviewed_at'):  d['reviewed_at']   = d['reviewed_at'].isoformat()
    if d.get('created_at'):   d['created_at']    = d['created_at'].isoformat()
    return d

# Day type labels for display
DAY_TYPE_LABEL = {
    'weekday':  '平日',
    'rest_day': '休息日',
    'holiday':  '國定假日',
    'special':  '例假日',
}

def _calc_ot_pay(staff_row, ot_hours, day_type='weekday'):
    """Calculate overtime pay based on salary type and day type.
    day_type: weekday | rest_day | holiday | special
    Taiwan Labor Standards Act:
      - weekday:  first 2h × 1.33, beyond × 1.67
      - rest_day: first 2h × 1.33, beyond × 1.67, minimum billed = 4h
      - holiday:  full hours × 2.0 (double pay)
      - special:  full hours × 2.0 (double pay, rarely allowed)
    """
    salary_type = staff_row.get('salary_type', 'monthly') or 'monthly'
    base_salary = float(staff_row.get('base_salary')  or 0)
    hourly_rate = float(staff_row.get('hourly_rate')  or 0)
    daily_hours = float(staff_row.get('daily_hours')  or 8)
    ot_rate1    = float(staff_row.get('ot_rate1')     or 1.33)
    ot_rate2    = float(staff_row.get('ot_rate2')     or 1.67)

    if salary_type == 'hourly':
        base_hourly = hourly_rate
    else:
        base_hourly = base_salary / 30 / daily_hours if (base_salary and daily_hours) else 0

    if base_hourly <= 0:
        return 0.0, base_hourly

    h = float(ot_hours)

    if day_type in ('holiday', 'special'):
        # 國定假日 / 例假日：全部 2 倍（已含原薪，加給 1 倍）
        pay = round(base_hourly * h * 2.0, 0)

    elif day_type == 'rest_day':
        # 休息日：最少計 4 小時，前 2h × 1.33，後續 × 1.67
        billed = max(h, 4.0)
        h1  = min(billed, 2.0)
        h2  = max(0.0, billed - 2.0)
        pay = round(base_hourly * (h1 * ot_rate1 + h2 * ot_rate2), 0)

    else:
        # 平日加班：前 2h × 1.33，後續 × 1.67
        h1  = min(h, 2.0)
        h2  = max(0.0, h - 2.0)
        pay = round(base_hourly * (h1 * ot_rate1 + h2 * ot_rate2), 0)

    return pay, base_hourly

def _ensure_ot_cols(conn):
    for sql in [
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_type TEXT DEFAULT 'monthly'",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hourly_rate NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS daily_hours NUMERIC(4,1) DEFAULT 8",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate1 NUMERIC(4,2) DEFAULT 1.33",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate2 NUMERIC(4,2) DEFAULT 1.67",
    ]:
        try: conn.execute(sql)
        except Exception: pass

# ── Employee: submit OT request ───────────────────────────────────

@app.route('/api/overtime/my-requests', methods=['GET'])
def api_ot_my_list():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM overtime_requests
            WHERE staff_id=%s ORDER BY request_date DESC LIMIT 30
        """, (sid,)).fetchall()
    return jsonify([ot_req_row(r) for r in rows])

@app.route('/api/overtime/my-requests', methods=['POST'])
def api_ot_submit():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b = request.get_json(force=True)
    request_date = b.get('request_date','').strip()
    start_time   = b.get('start_time','').strip()
    end_time     = b.get('end_time','').strip()
    reason       = b.get('reason','').strip()
    day_type     = b.get('day_type','weekday').strip()
    if day_type not in ('weekday','rest_day','holiday','special'):
        day_type = 'weekday'

    if not request_date or not start_time or not end_time:
        return jsonify({'error': '請填寫加班日期及時間'}), 400
    if not reason:
        return jsonify({'error': '請填寫加班原因'}), 400

    # Calculate OT hours
    from datetime import datetime as _dt_ot, timedelta as _td_ot
    try:
        s = _dt_ot.strptime(start_time, '%H:%M')
        e = _dt_ot.strptime(end_time,   '%H:%M')
        if e <= s: e += _td_ot(days=1)   # overnight
        ot_hours = round((e - s).total_seconds() / 3600, 2)
    except ValueError:
        return jsonify({'error': '時間格式錯誤'}), 400

    if ot_hours <= 0 or ot_hours > 12:
        return jsonify({'error': '加班時數不合理（0~12小時）'}), 400

    with get_db() as conn:
        _ensure_ot_cols(conn)
        try: conn.execute("ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS day_type TEXT DEFAULT 'weekday'")
        except Exception: pass
        row = conn.execute("""
            INSERT INTO overtime_requests
              (staff_id, request_date, start_time, end_time, ot_hours, reason, day_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *
        """, (sid, request_date, start_time, end_time, ot_hours, reason, day_type)).fetchone()
    return jsonify(ot_req_row(row)), 201

# ── Admin: list / review ─────────────────────────────────────────

@app.route('/api/overtime/requests', methods=['GET'])
@login_required
def api_ot_admin_list():
    status = request.args.get('status','')
    month  = request.args.get('month','')
    conds, params = ['TRUE'], []
    if status: conds.append('r.status=%s');                         params.append(status)
    if month:  conds.append("to_char(r.request_date,'YYYY-MM')=%s"); params.append(month)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT r.*, ps.name as staff_name, ps.role as staff_role
            FROM overtime_requests r
            JOIN punch_staff ps ON ps.id=r.staff_id
            WHERE {' AND '.join(conds)}
            ORDER BY r.request_date DESC, r.created_at DESC
        """, params).fetchall()
    return jsonify([ot_req_row(r) | {'staff_name': r['staff_name'], 'staff_role': r['staff_role']} for r in rows])

@app.route('/api/overtime/requests/<int:rid>', methods=['PUT'])
@login_required
def api_ot_review(rid):
    b           = request.get_json(force=True)
    action      = b.get('action')    # approve | reject
    reviewed_by = b.get('reviewed_by','').strip()
    review_note = b.get('review_note','').strip()

    if action not in ('approve','reject'):
        return jsonify({'error': 'invalid action'}), 400

    new_status = 'approved' if action == 'approve' else 'rejected'
    ot_pay_final = 0.0

    with get_db() as conn:
        _ensure_ot_cols(conn)
        req = conn.execute("SELECT * FROM overtime_requests WHERE id=%s", (rid,)).fetchone()
        if not req: return ('', 404)

        if action == 'approve':
            staff = conn.execute("""
                SELECT base_salary, hourly_rate, daily_hours, ot_rate1, ot_rate2, salary_type
                FROM punch_staff WHERE id=%s
            """, (req['staff_id'],)).fetchone()

            if staff:
                dtype = req.get('day_type','weekday') or 'weekday'
                ot_pay_final, _ = _calc_ot_pay(staff, req['ot_hours'] or 0, dtype)

        row = conn.execute("""
            UPDATE overtime_requests SET
              status=%s, reviewed_by=%s, review_note=%s,
              ot_pay=%s, reviewed_at=NOW()
            WHERE id=%s RETURNING *
        """, (new_status, reviewed_by, review_note, ot_pay_final, rid)).fetchone()

        result = ot_req_row(row)
        result['staff_name'] = req['staff_id']  # will be filled below

        # Get staff name
        sn = conn.execute("SELECT name FROM punch_staff WHERE id=%s", (req['staff_id'],)).fetchone()
        result['staff_name'] = sn['name'] if sn else ''

    return jsonify(result)

@app.route('/api/overtime/requests/<int:rid>', methods=['DELETE'])
@login_required
def api_ot_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM overtime_requests WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})

# ── Preview pay calculation ───────────────────────────────────────

@app.route('/api/overtime/calc-preview', methods=['POST'])
@login_required
def api_ot_calc_preview():
    """Preview overtime pay for given staff + hours."""
    b        = request.get_json(force=True)
    staff_id = b.get('staff_id')
    ot_hours = float(b.get('ot_hours') or 0)
    if not staff_id: return jsonify({'error': 'staff_id required'}), 400
    with get_db() as conn:
        _ensure_ot_cols(conn)
        staff = conn.execute("""
            SELECT name, base_salary, hourly_rate, daily_hours, ot_rate1, ot_rate2, salary_type
            FROM punch_staff WHERE id=%s
        """, (staff_id,)).fetchone()
    if not staff: return ('', 404)
    day_type = b.get('day_type','weekday') or 'weekday'
    ot_pay, base_hourly = _calc_ot_pay(staff, ot_hours, day_type)
    if day_type == 'rest_day':
        billed = max(ot_hours, 4.0)
        h1 = min(billed, 2.0); h2 = max(0.0, billed - 2.0)
    elif day_type in ('holiday','special'):
        h1 = ot_hours; h2 = 0.0
    else:
        h1 = min(ot_hours, 2.0); h2 = max(0.0, ot_hours - 2.0)
    return jsonify({
        'staff_name':    staff['name'],
        'salary_type':   staff.get('salary_type','monthly'),
        'base_salary':   float(staff.get('base_salary') or 0),
        'hourly_rate':   float(staff.get('hourly_rate') or 0),
        'base_hourly':   round(base_hourly, 2),
        'ot_hours':      ot_hours,
        'day_type':      day_type,
        'h1':            h1,
        'h2':            h2,
        'ot_rate1':      float(staff.get('ot_rate1') or 1.33),
        'ot_rate2':      float(staff.get('ot_rate2') or 1.67),
        'ot_pay':        ot_pay,
    })

@app.route('/')
def index():
    return redirect(url_for('admin_login'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)