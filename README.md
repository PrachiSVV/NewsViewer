# Results Viewer (Streamlit + MongoDB)

A simple Streamlit app to **fetch the latest company announcement** (actuals) and **append predicted results** from the `company_result_previews` collection **at the end** of the page.

## ⚙️ Features
- Login (env-based credentials)
- Fetch by **NSE symbol / BSE code / ISIN / company name**
- Show latest actual doc (by `dt_tm`) with sentiment/impact, summaries, and **PDF links as hypertext**
- Append **Predicted Results**: consensus KPIs + broker estimates table (with optional PDF link column) and CSV download
- Minimal styling, wide layout

## 🧰 Tech
- Streamlit
- MongoDB via `pymongo`
- `python-dotenv` for local env loading

## 📁 Repo Structure
```
.
├── app.py
├── requirements.txt
├── .env.example
├── .gitignore
└── .streamlit/
    └── config.toml
```

## 🔐 Configuration

Create a `.env` from the example and fill values (for local/dev). On Streamlit Cloud use **Secrets** instead.

```bash
cp .env.example .env
```

**Environment variables**:
- `MONGO_URI` — Mongo connection string
- `DB_NAME` — DB containing actual/announcement docs (default: `RAG_CHATBOT`)
- `NEWS_COLLECTION` — Collection name for actual docs (default: `selected_ann`)
- `PREV_DB` — DB containing previews (default: `CAG_CHATBOT`)
- `PREV_COLLECTION` — Collection name for predicted results (default: `company_result_previews`)
- `APP_USER`, `APP_PASS` — Login credentials (default: `admin` / `admin123`)

## 🧪 Local Run

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

Open the URL printed in your terminal.

## 🚀 Deploy via GitHub + Streamlit Cloud

1. Push this repo to GitHub.
2. In Streamlit Cloud → **New app** → connect your repo.
3. Set **Python version** to 3.11+ (works on 3.12 too).
4. Add **secrets** under *App settings → Secrets*:
   ```toml
   MONGO_URI = "mongodb+srv://..."
   DB_NAME = "RAG_CHATBOT"
   NEWS_COLLECTION = "selected_ann"
   PREV_DB = "CAG_CHATBOT"
   PREV_COLLECTION = "company_result_previews"
   APP_USER = "your_user"
   APP_PASS = "your_pass"
   ```
5. Deploy.

## 🔎 Usage

- Use the sidebar search to query by **NSE symbol** (e.g., `COROMANDEL`), **BSE code**, **ISIN**, or **Company Name**.
- The page shows **actuals** first.
- **Predicted Results** (from `company_result_previews`) are shown **at the very end**, including consensus KPIs and the broker table.
- If your `broker_estimates` include a URL field (e.g., `source_url`), it will be rendered as a clickable **PDF** link in the table.

## 📝 Notes
- Latest actual is chosen by sorting by `dt_tm` (descending). Ensure `dt_tm` is `"YYYY-MM-DD HH:MM:SS"`.
- Latest preview is chosen by `updated_at` or `created_at` (ISO format).

---

Made with ❤️ for quick financial result viewing.
