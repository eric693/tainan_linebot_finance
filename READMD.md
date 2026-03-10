# LINE Bot 財務管理系統

以 LINE Bot 為介面的每日財務記帳系統，搭配後台管理儀表板，支援新增、編輯、刪除記帳資料，並即時計算每日總收入。

---

## 功能特色

- LINE Bot Flex Message 操作介面，支援日期選擇與欄位輸入
- 自動計算每日總收入（櫃檯支出自動扣除，其餘欄位加總）
- 後台管理儀表板，支援新增、編輯、刪除
- 資料即時同步，LINE Bot 輸入後台立即反映
- 可部署至 Render 免費方案

---

## 記帳欄位

| 欄位 | 計算方式 |
|---|---|
| 早餐點收 | 加總 |
| 早餐餐-現金 | 加總 |
| 早餐餐-刷卡合庫 | 加總 |
| 早餐餐-LINE Pay | 加總 |
| 早餐餐-轉帳 | 加總 |
| 櫃檯支出 | **扣除** |
| 熊貓 | 加總 |
| Uber Eats | 加總 |
| 小費 | 加總 |
| 溢收 | 加總 |
| POS機總額 | 加總 |

---

## 專案結構

```
linebot-finance/
├── app.py                  # Flask 主程式、LINE Bot Webhook、後台 API
├── templates/
│   ├── login.html          # 後台登入頁
│   └── admin.html          # 後台管理儀表板
├── requirements.txt
├── render.yaml             # Render 部署設定
├── Procfile
└── README.md
```

---

## 環境需求

- Python 3.10 以上
- LINE Messaging API Channel

---

## 本地開發

**1. 安裝套件**

```bash
pip install -r requirements.txt
```

**2. 建立 `.env` 並填入環境變數**

```env
LINE_CHANNEL_ACCESS_TOKEN=你的_channel_access_token
LINE_CHANNEL_SECRET=你的_channel_secret
ADMIN_PASSWORD=你的後台密碼
SECRET_KEY=任意亂數字串
DB_PATH=finance.db
```

**3. 啟動伺服器**

```bash
python app.py
```

本地預設啟動於 `http://localhost:5000`

> 本地測試 LINE Bot Webhook 需搭配 [ngrok](https://ngrok.com) 對外暴露端口：
> ```bash
> ngrok http 5000
> ```
> 將 ngrok 產生的網址填入 LINE Developers Webhook URL，例如：
> `https://xxxx.ngrok-free.app/webhook`

---

## 部署至 Render

### 第一步：推上 GitHub

```bash
git init
git add .
git commit -m "first commit"
git remote add origin https://github.com/你的帳號/你的repo.git
git push -u origin main
```

### 第二步：建立 Render Web Service

1. 前往 [render.com](https://render.com) 登入
2. 點選 **New → Web Service**
3. 連結你的 GitHub repo
4. 填入以下設定：

| 欄位 | 值 |
|---|---|
| Environment | Python 3 |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1` |

### 第三步：設定環境變數

在 Render 的 **Environment Variables** 新增：

| Key | 說明 |
|---|---|
| `LINE_CHANNEL_ACCESS_TOKEN` | LINE Bot Channel Access Token |
| `LINE_CHANNEL_SECRET` | LINE Bot Channel Secret |
| `ADMIN_PASSWORD` | 後台登入密碼 |
| `SECRET_KEY` | 任意亂數字串（用於 session 加密） |
| `DB_PATH` | `/opt/render/project/src/finance.db` |

### 第四步：取得 Webhook URL

部署完成後，Render 會給你一個網址，例如：

```
https://linebot-finance.onrender.com
```

各頁面網址如下：

| 用途 | 網址 |
|---|---|
| LINE Webhook | `https://linebot-finance.onrender.com/webhook` |
| 後台管理 | `https://linebot-finance.onrender.com/admin` |

### 第五步：設定 LINE Developers

1. 前往 [developers.line.biz](https://developers.line.biz)
2. 進入你的 Messaging API Channel
3. 點選 **Messaging API** 分頁
4. 在 **Webhook URL** 填入 `https://linebot-finance.onrender.com/webhook`
5. 點 **Verify**，出現綠色勾勾代表連線成功
6. 確認 **Use webhook** 已開啟

---

## 後台管理

| 功能 | 說明 |
|---|---|
| 登入 | 使用 `ADMIN_PASSWORD` 環境變數設定的密碼 |
| 每日記帳 | 查看所有日期記錄，可新增、編輯、刪除 |
| 總覽統計 | 顯示各欄位加總、累計總收入、日均收入 |

---

## LINE Bot 使用流程

```
使用者傳送任意訊息
  └─> Bot 顯示「選擇日期」按鈕
        └─> 使用者選擇日期
              └─> Bot 顯示所有欄位清單
                    └─> 使用者點選欄位 → 輸入金額
                          └─> Bot 確認並顯示當日累計總收入
                                └─> 繼續記帳 或 完成
```

---

## API 端點

後台 API 需登入 session 才能存取。

| Method | 路徑 | 說明 |
|---|---|---|
| GET | `/api/records` | 取得所有記錄 |
| GET | `/api/records/<date>` | 取得單日記錄 |
| POST | `/api/records` | 新增記錄 |
| PUT | `/api/records/<date>` | 更新記錄 |
| DELETE | `/api/records/<date>` | 刪除記錄 |
| GET | `/api/summary` | 取得統計摘要 |

---

## 注意事項

- Render 免費方案的服務在閒置 15 分鐘後會進入休眠，第一次收到 LINE 訊息時需等待約 30 秒喚醒
- 免費方案使用本地 SQLite，重新部署後資料庫會重置；如需持久化資料，建議升級 Render 付費方案或改用外部資料庫（PostgreSQL）
- `DB_PATH` 設為 `/opt/render/project/src/finance.db` 可在同一次部署期間保留資料

---

## 技術堆疊

- **Backend**：Flask 3.0
- **LINE SDK**：line-bot-sdk 3.x
- **資料庫**：SQLite
- **部署**：Render / Gunicorn