import os
import secrets
import threading
import time
import urllib.request
from datetime import datetime, date
from functools import wraps

import psycopg2
import psycopg2.extras
import psycopg2.errorcodes
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
DATABASE_URL              = os.environ.get('DATABASE_URL', '')
# Set this to your Render URL, e.g. https://linebot-finance.onrender.com
SELF_URL                  = os.environ.get('SELF_URL', '')

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler      = WebhookHandler(LINE_CHANNEL_SECRET)

# ─── Field Definitions ────────────────────────────────────────────────────────

FIELDS = [
    {'key': 'breakfast_total',    'label': '早餐點收',        'subtract': False},
    {'key': 'breakfast_cash',     'label': '早餐餐-現金',     'subtract': False},
    {'key': 'breakfast_card',     'label': '早餐餐-刷卡合庫', 'subtract': False},
    {'key': 'breakfast_linepay',  'label': '早餐餐-LINE Pay', 'subtract': False},
    {'key': 'breakfast_transfer', 'label': '早餐餐-轉帳',     'subtract': False},
    {'key': 'counter_expense',    'label': '櫃檯支出',        'subtract': True},
    {'key': 'panda',              'label': '熊貓',            'subtract': False},
    {'key': 'ubereats',           'label': 'Uber Eats',       'subtract': False},
    {'key': 'tips',               'label': '小費',            'subtract': False},
    {'key': 'surplus',            'label': '溢收',            'subtract': False},
    {'key': 'pos_total',          'label': 'POS機總額',       'subtract': False},
]

FIELD_KEYS = [f['key'] for f in FIELDS]
FIELD_MAP  = {f['key']: f for f in FIELDS}

# ─── Database ─────────────────────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


def init_db():
    col_defs = '\n'.join(f'    {k} NUMERIC(14,2) DEFAULT 0,' for k in FIELD_KEYS)
    sql = f"""
        CREATE TABLE IF NOT EXISTS records (
            id           SERIAL PRIMARY KEY,
            record_date  DATE    NOT NULL UNIQUE,
            {col_defs}
            total_income NUMERIC(14,2) DEFAULT 0,
            created_at   TIMESTAMPTZ   DEFAULT NOW(),
            updated_at   TIMESTAMPTZ   DEFAULT NOW()
        );
    """
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    conn.close()
    print('[db] init_db OK')


def calculate_total(data: dict) -> float:
    total = 0.0
    for f in FIELDS:
        val = float(data.get(f['key']) or 0)
        total += -val if f['subtract'] else val
    return total


def row_to_dict(row):
    if not row:
        return None
    d = dict(row)
    if 'record_date' in d:
        d['record_date'] = str(d['record_date'])
    for ts_col in ('created_at', 'updated_at'):
        if d.get(ts_col):
            d[ts_col] = str(d[ts_col])
    return d


try:
    init_db()
except Exception as e:
    print(f'[db] Warning during init_db: {e}')

# ─── Keep-Alive Thread ────────────────────────────────────────────────────────

def keep_alive_loop():
    """Ping /ping every 10 minutes to prevent Render free tier from sleeping."""
    while True:
        time.sleep(600)
        url = SELF_URL.rstrip('/') + '/ping' if SELF_URL else None
        if url:
            try:
                urllib.request.urlopen(url, timeout=15)
                print(f'[keep-alive] pinged {url}')
            except Exception as e:
                print(f'[keep-alive] error: {e}')


threading.Thread(target=keep_alive_loop, daemon=True).start()


@app.route('/ping')
def ping():
    return 'pong', 200

# ─── In-memory LINE user state ────────────────────────────────────────────────

user_states: dict = {}
def get_state(uid):    return user_states.get(uid, {})
def set_state(uid, d): user_states[uid] = d
def clear_state(uid):  user_states.pop(uid, None)

# ─── Flex Message Builders ────────────────────────────────────────────────────

def make_start_flex():
    return {
        "type": "bubble", "size": "kilo",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1a2744", "paddingAll": "20px",
            "contents": [
                {"type": "text", "text": "財務記帳系統",
                 "color": "#ffffff", "size": "xl", "weight": "bold"},
                {"type": "text", "text": "請選擇要記帳的日期",
                 "color": "#9eb3d8", "size": "sm", "margin": "sm"}
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
        v = record.get(key) or 0
        return f"${int(float(v)):,}" if float(v) else "未填"

    rows = []
    for i in range(0, len(FIELDS), 2):
        pair = FIELDS[i:i+2]
        cols = []
        for f in pair:
            cols.append({
                "type": "box", "layout": "vertical", "flex": 1, "spacing": "xs",
                "contents": [
                    {"type": "text", "text": f['label'],
                     "size": "xs", "color": "#666666", "wrap": True},
                    {"type": "text", "text": val_text(f['key']), "size": "sm",
                     "weight": "bold",
                     "color": "#c0392b" if f['subtract'] else "#1a2744"},
                    {"type": "button",
                     "action": {"type": "postback", "label": "輸入",
                                "data": f"action=input_field&date={record_date}&field={f['key']}"},
                     "style": "secondary", "height": "sm", "color": "#f0f4ff"}
                ]
            })
        if len(pair) == 1:
            cols.append({"type": "box", "layout": "vertical", "flex": 1, "contents": []})
        rows.append({"type": "box", "layout": "horizontal",
                     "spacing": "md", "margin": "md", "contents": cols})

    total = calculate_total(record)
    rows += [
        {"type": "separator", "margin": "lg"},
        {"type": "box", "layout": "horizontal", "margin": "lg", "contents": [
            {"type": "text", "text": "當日總收入", "size": "md",
             "weight": "bold", "color": "#333333", "flex": 1},
            {"type": "text", "text": f"${int(total):,}", "size": "xl",
             "weight": "bold", "color": "#1a2744", "align": "end", "flex": 1}
        ]},
        {"type": "button", "margin": "lg",
         "action": {"type": "postback", "label": "完成記帳",
                    "data": f"action=done&date={record_date}"},
         "style": "primary", "color": "#1a2744", "height": "sm"}
    ]

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1a2744", "paddingAll": "18px",
            "contents": [
                {"type": "text", "text": record_date,
                 "color": "#ffffff", "size": "lg", "weight": "bold"},
                {"type": "text", "text": "點選欄位輸入金額",
                 "color": "#9eb3d8", "size": "xs", "margin": "xs"}
            ]
        },
        "body": {"type": "box", "layout": "vertical",
                 "paddingAll": "16px", "contents": rows}
    }


def make_confirm_flex(record_date, field_label, amount, record=None):
    total = calculate_total(record or {})
    return {
        "type": "bubble", "size": "kilo",
        "body": {
            "type": "box", "layout": "vertical", "paddingAll": "20px",
            "contents": [
                {"type": "text", "text": "記帳成功",
                 "weight": "bold", "size": "lg", "color": "#1a2744"},
                {"type": "separator", "margin": "md"},
                {"type": "box", "layout": "horizontal", "margin": "md", "contents": [
                    {"type": "text", "text": field_label,
                     "size": "sm", "color": "#666666", "flex": 1},
                    {"type": "text", "text": f"${int(amount):,}",
                     "size": "sm", "weight": "bold", "color": "#1a2744",
                     "align": "end", "flex": 1}
                ]},
                {"type": "box", "layout": "horizontal", "margin": "sm", "contents": [
                    {"type": "text", "text": "當日總收入",
                     "size": "sm", "color": "#666666", "flex": 1},
                    {"type": "text", "text": f"${int(total):,}",
                     "size": "sm", "weight": "bold", "color": "#1a2744",
                     "align": "end", "flex": 1}
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
    rows  = []
    for f in FIELDS:
        val = float(record.get(f['key']) or 0)
        if val == 0:
            continue
        rows.append({
            "type": "box", "layout": "horizontal", "margin": "sm",
            "contents": [
                {"type": "text", "text": f['label'],
                 "size": "sm", "color": "#666666", "flex": 2},
                {"type": "text",
                 "text": f"{'-' if f['subtract'] else ''}${int(val):,}",
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
            {"type": "text", "text": "總收入",
             "size": "lg", "weight": "bold", "flex": 1},
            {"type": "text", "text": f"${int(total):,}",
             "size": "lg", "weight": "bold", "color": "#1a2744",
             "align": "end", "flex": 1}
        ]}
    ]
    return {
        "type": "bubble", "size": "kilo",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1a2744", "paddingAll": "18px",
            "contents": [
                {"type": "text", "text": f"{record_date} 總覽",
                 "color": "#ffffff", "size": "lg", "weight": "bold"}
            ]
        },
        "body": {"type": "box", "layout": "vertical",
                 "paddingAll": "16px", "contents": rows}
    }

# ─── DB Helpers ───────────────────────────────────────────────────────────────

def get_or_create_record(record_date):
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM records WHERE record_date=%s', (record_date,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    'INSERT INTO records (record_date) VALUES (%s) '
                    'ON CONFLICT (record_date) DO NOTHING RETURNING *',
                    (record_date,)
                )
                row = cur.fetchone()
                if not row:
                    cur.execute('SELECT * FROM records WHERE record_date=%s', (record_date,))
                    row = cur.fetchone()
    conn.close()
    return row_to_dict(row)


def update_record_field(record_date, field_key, amount):
    record = get_or_create_record(record_date)
    record[field_key] = amount
    total = calculate_total(record)
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f'UPDATE records SET {field_key}=%s, total_income=%s, '
                f'updated_at=NOW() WHERE record_date=%s RETURNING *',
                (amount, total, record_date)
            )
            row = cur.fetchone()
    conn.close()
    return row_to_dict(row)

# ─── LINE Bot Webhook ─────────────────────────────────────────────────────────

@app.route('/webhook', methods=['POST'])
def webhook():
    sig  = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    uid   = event.source.user_id
    text  = event.message.text.strip()
    state = get_state(uid)

    if state.get('step') == 'input_amount':
        try:
            amount = float(text.replace(',', '').replace('$', ''))
            if amount < 0:
                raise ValueError
        except ValueError:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text='請輸入有效的數字金額，例如：1500')
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
            FlexSendMessage(alt_text='記帳成功', contents=flex)
        )
        return

    line_bot_api.reply_message(
        event.reply_token,
        FlexSendMessage(alt_text='財務記帳系統', contents=make_start_flex())
    )


@handler.add(PostbackEvent)
def handle_postback(event):
    uid    = event.source.user_id
    data   = dict(p.split('=', 1) for p in event.postback.data.split('&') if '=' in p)
    action = data.get('action')

    if action == 'select_date':
        record_date = event.postback.params.get('date', str(date.today()))
        record = get_or_create_record(record_date)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text=f'{record_date} 記帳',
                            contents=make_field_flex(record_date, record))
        )

    elif action == 'input_field':
        field_key   = data.get('field')
        field_label = FIELD_MAP.get(field_key, {}).get('label', field_key)
        set_state(uid, {'step': 'input_amount',
                        'date': data.get('date'), 'field': field_key})
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f'請輸入【{field_label}】的金額：')
        )

    elif action == 'continue':
        record_date = data.get('date')
        record = get_or_create_record(record_date)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text=f'{record_date} 記帳',
                            contents=make_field_flex(record_date, record))
        )

    elif action == 'done':
        record_date = data.get('date')
        record = get_or_create_record(record_date)
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(alt_text=f'{record_date} 記帳完成',
                            contents=make_summary_flex(record_date, record))
        )

# ─── Admin ────────────────────────────────────────────────────────────────────

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


# ── Admin API ──────────────────────────────────────────────────

@app.route('/api/records', methods=['GET'])
@login_required
def api_list_records():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM records ORDER BY record_date DESC')
        rows = cur.fetchall()
    conn.close()
    return jsonify([row_to_dict(r) for r in rows])


@app.route('/api/records/<record_date>', methods=['GET'])
@login_required
def api_get_record(record_date):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM records WHERE record_date=%s', (record_date,))
        row = cur.fetchone()
    conn.close()
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

    conn = get_db()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'INSERT INTO records (record_date, {cols}, total_income) '
                    f'VALUES (%s, {phs}, %s) RETURNING *',
                    [record_date] + [vals[k] for k in FIELD_KEYS] + [total]
                )
                row = cur.fetchone()
        conn.close()
        return jsonify(row_to_dict(row)), 201
    except psycopg2.errors.UniqueViolation:
        conn.close()
        return jsonify({'error': '該日期已存在'}), 409
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500


@app.route('/api/records/<record_date>', methods=['PUT'])
@login_required
def api_update_record(record_date):
    body = request.get_json(force=True)
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM records WHERE record_date=%s', (record_date,))
        row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'not found'}), 404

    current    = dict(row)
    vals       = {k: float(body.get(k, current.get(k)) or 0) for k in FIELD_KEYS}
    total      = calculate_total(vals)
    set_clause = ', '.join([f'{k}=%s' for k in FIELD_KEYS])

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f'UPDATE records SET {set_clause}, total_income=%s, '
                f'updated_at=NOW() WHERE record_date=%s RETURNING *',
                [vals[k] for k in FIELD_KEYS] + [total, record_date]
            )
            row = cur.fetchone()
    conn.close()
    return jsonify(row_to_dict(row))


@app.route('/api/records/<record_date>', methods=['DELETE'])
@login_required
def api_delete_record(record_date):
    conn = get_db()
    with conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM records WHERE record_date=%s', (record_date,))
    conn.close()
    return jsonify({'deleted': record_date})


@app.route('/api/summary', methods=['GET'])
@login_required
def api_summary():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute('''
            SELECT COUNT(*)                         AS days,
                   COALESCE(SUM(total_income), 0)   AS total,
                   COALESCE(AVG(total_income), 0)   AS avg_daily
            FROM records
        ''')
        row = cur.fetchone()
    conn.close()
    d = dict(row)
    d['total']     = float(d['total'])
    d['avg_daily'] = float(d['avg_daily'])
    return jsonify(d)


@app.route('/')
def index():
    return redirect(url_for('admin_login'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)