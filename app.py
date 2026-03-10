import os
import json
import sqlite3
import hashlib
import secrets
from datetime import datetime, date
from functools import wraps

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
LINE_CHANNEL_SECRET = os.environ.get('LINE_CHANNEL_SECRET', '')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── Field Definitions ───────────────────────────────────────────────────────

FIELDS = [
    {'key': 'breakfast_total',    'label': '早餐點收',      'subtract': False},
    {'key': 'breakfast_cash',     'label': '早餐餐-現金',   'subtract': False},
    {'key': 'breakfast_card',     'label': '早餐餐-刷卡合庫', 'subtract': False},
    {'key': 'breakfast_linepay',  'label': '早餐餐-LINE Pay', 'subtract': False},
    {'key': 'breakfast_transfer', 'label': '早餐餐-轉帳',   'subtract': False},
    {'key': 'counter_expense',    'label': '櫃檯支出',      'subtract': True},
    {'key': 'panda',              'label': '熊貓',          'subtract': False},
    {'key': 'ubereats',           'label': 'Uber Eats',     'subtract': False},
    {'key': 'tips',               'label': '小費',          'subtract': False},
    {'key': 'surplus',            'label': '溢收',          'subtract': False},
    {'key': 'pos_total',          'label': 'POS機總額',     'subtract': False},
]

FIELD_KEYS = [f['key'] for f in FIELDS]
FIELD_MAP  = {f['key']: f for f in FIELDS}

# ─── Database ─────────────────────────────────────────────────────────────────

DB_PATH = os.environ.get('DB_PATH', 'finance.db')

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS records (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            record_date      TEXT NOT NULL UNIQUE,
            breakfast_total  REAL DEFAULT 0,
            breakfast_cash   REAL DEFAULT 0,
            breakfast_card   REAL DEFAULT 0,
            breakfast_linepay REAL DEFAULT 0,
            breakfast_transfer REAL DEFAULT 0,
            counter_expense  REAL DEFAULT 0,
            panda            REAL DEFAULT 0,
            ubereats         REAL DEFAULT 0,
            tips             REAL DEFAULT 0,
            surplus          REAL DEFAULT 0,
            pos_total        REAL DEFAULT 0,
            total_income     REAL DEFAULT 0,
            created_at       TEXT,
            updated_at       TEXT
        )
    ''')
    conn.commit()
    conn.close()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def row_to_dict(row):
    return dict(row) if row else None

def calculate_total(data: dict) -> float:
    total = 0.0
    for f in FIELDS:
        val = float(data.get(f['key']) or 0)
        total -= val if f['subtract'] else -val
        total += val if not f['subtract'] else -val
    # Simpler:
    total = 0.0
    for f in FIELDS:
        val = float(data.get(f['key']) or 0)
        if f['subtract']:
            total -= val
        else:
            total += val
    return total

init_db()

# ─── User State (LINE Bot conversation) ──────────────────────────────────────

user_states = {}   # { user_id: { 'step': ..., 'date': ..., 'field': ... } }

def get_state(uid):
    return user_states.get(uid, {})

def set_state(uid, data):
    user_states[uid] = data

def clear_state(uid):
    user_states.pop(uid, None)

# ─── LINE Bot Flex Builders ───────────────────────────────────────────────────

def make_start_flex():
    """Initial greeting bubble with date picker."""
    return {
        "type": "bubble",
        "size": "kilo",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1a2744",
            "paddingAll": "20px",
            "contents": [
                {"type": "text", "text": "財務記帳系統", "color": "#ffffff",
                 "size": "xl", "weight": "bold"},
                {"type": "text", "text": "請選擇要記帳的日期", "color": "#9eb3d8",
                 "size": "sm", "margin": "sm"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "datetimepicker",
                        "label": "選擇日期",
                        "data": "action=select_date",
                        "mode": "date"
                    },
                    "style": "primary",
                    "color": "#1a2744",
                    "height": "sm"
                }
            ]
        }
    }


def make_field_flex(record_date, record=None):
    """Field selection flex message (2-column grid)."""
    record = record or {}

    def val_text(key):
        v = record.get(key, 0) or 0
        return f"${int(v):,}" if v else "未填"

    rows = []
    # Group fields in pairs
    for i in range(0, len(FIELDS), 2):
        pair = FIELDS[i:i+2]
        cols = []
        for f in pair:
            cols.append({
                "type": "box",
                "layout": "vertical",
                "flex": 1,
                "spacing": "xs",
                "contents": [
                    {"type": "text", "text": f['label'],
                     "size": "xs", "color": "#666666", "wrap": True},
                    {"type": "text", "text": val_text(f['key']),
                     "size": "sm", "weight": "bold",
                     "color": "#c0392b" if f['subtract'] else "#1a2744"},
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": "輸入",
                            "data": f"action=input_field&date={record_date}&field={f['key']}"
                        },
                        "style": "secondary",
                        "height": "sm",
                        "color": "#f0f4ff"
                    }
                ]
            })
        if len(pair) == 1:
            cols.append({"type": "box", "layout": "vertical", "flex": 1, "contents": []})
        rows.append({
            "type": "box",
            "layout": "horizontal",
            "spacing": "md",
            "margin": "md",
            "contents": cols
        })

    # Total box
    total = calculate_total(record)
    rows.append({
        "type": "separator",
        "margin": "lg"
    })
    rows.append({
        "type": "box",
        "layout": "horizontal",
        "margin": "lg",
        "contents": [
            {"type": "text", "text": "當日總收入",
             "size": "md", "weight": "bold", "color": "#333333", "flex": 1},
            {"type": "text", "text": f"${int(total):,}",
             "size": "xl", "weight": "bold", "color": "#1a2744",
             "align": "end", "flex": 1}
        ]
    })
    rows.append({
        "type": "button",
        "margin": "lg",
        "action": {
            "type": "postback",
            "label": "完成記帳",
            "data": f"action=done&date={record_date}"
        },
        "style": "primary",
        "color": "#1a2744",
        "height": "sm"
    })

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1a2744",
            "paddingAll": "18px",
            "contents": [
                {"type": "text", "text": record_date,
                 "color": "#ffffff", "size": "lg", "weight": "bold"},
                {"type": "text", "text": "點選欄位輸入金額",
                 "color": "#9eb3d8", "size": "xs", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "16px",
            "contents": rows
        }
    }


def make_confirm_flex(record_date, field_label, amount, record=None):
    """Confirmation after entering an amount."""
    total = calculate_total(record or {})
    return {
        "type": "bubble",
        "size": "kilo",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "contents": [
                {"type": "text", "text": "記帳成功", "weight": "bold",
                 "size": "lg", "color": "#1a2744"},
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {"type": "text", "text": field_label,
                         "size": "sm", "color": "#666666", "flex": 1},
                        {"type": "text", "text": f"${int(amount):,}",
                         "size": "sm", "weight": "bold", "color": "#1a2744",
                         "align": "end", "flex": 1}
                    ]
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "sm",
                    "contents": [
                        {"type": "text", "text": "當日總收入",
                         "size": "sm", "color": "#666666", "flex": 1},
                        {"type": "text", "text": f"${int(total):,}",
                         "size": "sm", "weight": "bold", "color": "#1a2744",
                         "align": "end", "flex": 1}
                    ]
                },
                {"type": "separator", "margin": "md"},
                {
                    "type": "button",
                    "margin": "md",
                    "action": {
                        "type": "postback",
                        "label": "繼續記帳",
                        "data": f"action=continue&date={record_date}"
                    },
                    "style": "secondary",
                    "height": "sm"
                },
                {
                    "type": "button",
                    "margin": "sm",
                    "action": {
                        "type": "postback",
                        "label": "完成",
                        "data": f"action=done&date={record_date}"
                    },
                    "style": "primary",
                    "color": "#1a2744",
                    "height": "sm"
                }
            ]
        }
    }


def make_summary_flex(record_date, record):
    """Day summary bubble."""
    total = calculate_total(record)
    rows = []
    for f in FIELDS:
        val = float(record.get(f['key']) or 0)
        if val == 0:
            continue
        color = "#c0392b" if f['subtract'] else "#2c3e50"
        prefix = "-" if f['subtract'] else ""
        rows.append({
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {"type": "text", "text": f['label'],
                 "size": "sm", "color": "#666666", "flex": 2},
                {"type": "text", "text": f"{prefix}${int(val):,}",
                 "size": "sm", "weight": "bold", "color": color,
                 "align": "end", "flex": 1}
            ]
        })

    if not rows:
        rows.append({"type": "text", "text": "尚無資料",
                     "size": "sm", "color": "#aaaaaa", "align": "center"})

    rows += [
        {"type": "separator", "margin": "lg"},
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "lg",
            "contents": [
                {"type": "text", "text": "總收入",
                 "size": "lg", "weight": "bold", "flex": 1},
                {"type": "text", "text": f"${int(total):,}",
                 "size": "lg", "weight": "bold", "color": "#1a2744",
                 "align": "end", "flex": 1}
            ]
        }
    ]

    return {
        "type": "bubble",
        "size": "kilo",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1a2744",
            "paddingAll": "18px",
            "contents": [
                {"type": "text", "text": f"{record_date} 總覽",
                 "color": "#ffffff", "size": "lg", "weight": "bold"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "16px",
            "contents": rows
        }
    }

# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_or_create_record(record_date):
    conn = get_db()
    row = conn.execute(
        'SELECT * FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    if not row:
        now = datetime.now().isoformat()
        conn.execute(
            'INSERT INTO records (record_date, created_at, updated_at) VALUES (?,?,?)',
            (record_date, now, now)
        )
        conn.commit()
        row = conn.execute(
            'SELECT * FROM records WHERE record_date=?', (record_date,)
        ).fetchone()
    conn.close()
    return row_to_dict(row)

def update_record_field(record_date, field_key, amount):
    now = datetime.now().isoformat()
    conn = get_db()
    record = get_or_create_record(record_date)
    record[field_key] = amount
    total = calculate_total(record)
    conn.execute(
        f'UPDATE records SET {field_key}=?, total_income=?, updated_at=? WHERE record_date=?',
        (amount, total, now, record_date)
    )
    conn.commit()
    row = conn.execute(
        'SELECT * FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    conn.close()
    return row_to_dict(row)

# ─── LINE Bot Routes ──────────────────────────────────────────────────────────

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
    uid = event.source.user_id
    text = event.message.text.strip()
    state = get_state(uid)

    # Waiting for amount input
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
        record = update_record_field(record_date, field_key, amount)
        clear_state(uid)

        flex = make_confirm_flex(record_date, field_label, amount, record)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text="記帳成功", contents=flex)
        )
        return

    # Default: show start menu
    flex = make_start_flex()
    line_bot_api.reply_message(
        event.reply_token,
        FlexSendMessage(alt_text="財務記帳系統", contents=flex)
    )


@handler.add(PostbackEvent)
def handle_postback(event):
    uid  = event.source.user_id
    data = dict(p.split('=', 1) for p in event.postback.data.split('&') if '=' in p)
    action = data.get('action')

    if action == 'select_date':
        record_date = event.postback.params.get('date', str(date.today()))
        record = get_or_create_record(record_date)
        flex = make_field_flex(record_date, record)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text=f"{record_date} 記帳", contents=flex)
        )

    elif action == 'input_field':
        record_date = data.get('date')
        field_key   = data.get('field')
        field_label = FIELD_MAP.get(field_key, {}).get('label', field_key)
        set_state(uid, {'step': 'input_amount', 'date': record_date, 'field': field_key})
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"請輸入【{field_label}】的金額：")
        )

    elif action == 'continue':
        record_date = data.get('date')
        record = get_or_create_record(record_date)
        flex = make_field_flex(record_date, record)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text=f"{record_date} 記帳", contents=flex)
        )

    elif action == 'done':
        record_date = data.get('date')
        record = get_or_create_record(record_date)
        flex = make_summary_flex(record_date, record)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text=f"{record_date} 記帳完成", contents=flex)
        )

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
        pw = request.form.get('password', '')
        if pw == ADMIN_PASSWORD:
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
    conn = get_db()
    rows = conn.execute(
        'SELECT * FROM records ORDER BY record_date DESC'
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/records/<record_date>', methods=['GET'])
@login_required
def api_get_record(record_date):
    conn = get_db()
    row = conn.execute(
        'SELECT * FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(dict(row))


@app.route('/api/records', methods=['POST'])
@login_required
def api_create_record():
    body = request.get_json(force=True)
    record_date = body.get('record_date')
    if not record_date:
        return jsonify({'error': 'record_date required'}), 400

    conn = get_db()
    existing = conn.execute(
        'SELECT id FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    if existing:
        conn.close()
        return jsonify({'error': '該日期已存在'}), 409

    now = datetime.now().isoformat()
    vals = {k: float(body.get(k) or 0) for k in FIELD_KEYS}
    total = calculate_total(vals)

    placeholders = ', '.join(FIELD_KEYS)
    q_marks = ', '.join(['?'] * len(FIELD_KEYS))
    conn.execute(
        f'INSERT INTO records (record_date, {placeholders}, total_income, created_at, updated_at) '
        f'VALUES (?, {q_marks}, ?, ?, ?)',
        [record_date] + [vals[k] for k in FIELD_KEYS] + [total, now, now]
    )
    conn.commit()
    row = conn.execute(
        'SELECT * FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    conn.close()
    return jsonify(dict(row)), 201


@app.route('/api/records/<record_date>', methods=['PUT'])
@login_required
def api_update_record(record_date):
    body = request.get_json(force=True)
    conn = get_db()
    row = conn.execute(
        'SELECT * FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'not found'}), 404

    now = datetime.now().isoformat()
    current = dict(row)
    vals = {k: float(body.get(k, current.get(k)) or 0) for k in FIELD_KEYS}
    total = calculate_total(vals)

    set_clause = ', '.join([f'{k}=?' for k in FIELD_KEYS])
    conn.execute(
        f'UPDATE records SET {set_clause}, total_income=?, updated_at=? WHERE record_date=?',
        [vals[k] for k in FIELD_KEYS] + [total, now, record_date]
    )
    conn.commit()
    row = conn.execute(
        'SELECT * FROM records WHERE record_date=?', (record_date,)
    ).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.route('/api/records/<record_date>', methods=['DELETE'])
@login_required
def api_delete_record(record_date):
    conn = get_db()
    conn.execute('DELETE FROM records WHERE record_date=?', (record_date,))
    conn.commit()
    conn.close()
    return jsonify({'deleted': record_date})


@app.route('/api/summary', methods=['GET'])
@login_required
def api_summary():
    conn = get_db()
    rows = conn.execute('''
        SELECT
            COUNT(*) as days,
            SUM(total_income) as total,
            AVG(total_income) as avg_daily
        FROM records
    ''').fetchone()
    conn.close()
    return jsonify(dict(rows))


@app.route('/')
def index():
    return redirect(url_for('admin_login'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
