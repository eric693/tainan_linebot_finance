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
        print("[OK] Database initialised")
    except Exception as e:
        print(f"[ERROR] init_db failed: {e}")
        raise


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


@app.route('/')
def index():
    return redirect(url_for('admin_login'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)