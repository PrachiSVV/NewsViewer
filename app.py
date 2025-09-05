# app.py
import os, json, re
from datetime import datetime
from typing import Any, Dict, Optional, List

import streamlit as st
from pymongo import MongoClient
import pandas as pd
from dotenv import load_dotenv

# -------------------- LOAD ENV (.env if present) --------------------
load_dotenv()

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Results Viewer", page_icon="📊", layout="wide")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("DB_NAME", "RAG_CHATBOT")
NEWS_COLL = os.getenv("NEWS_COLLECTION", "selected_ann")                # <- actual/announcement docs
PREV_DB   = os.getenv("PREV_DB", "CAG_CHATBOT")                         # <- previews live here if separate DB
PREV_COLL = os.getenv("PREV_COLLECTION", "company_result_previews")     # <- predicted results

APP_USER  = os.getenv("APP_USER", "admin")
APP_PASS  = os.getenv("APP_PASS", "admin123")

# -------------------- STYLES --------------------
st.markdown("""
<style>
.badge{display:inline-block;padding:4px 10px;margin-right:8px;border-radius:999px;background:#eef;color:#222;font-size:12px}
.badge.neg{background:#ffe6e6;color:#a30000}
.badge.pos{background:#e7f7ec;color:#0b6b2f}
.card{border:1px solid #eee;border-radius:12px;padding:18px;margin-bottom:14px;background:#fff}
.kpi{border:1px solid #eee;border-radius:14px;padding:16px;margin-right:12px;background:#fff}
.tbl th, .tbl td{padding:10px 8px;border-bottom:1px solid #f0f0f0;font-size:14px}
.tbl th{font-weight:600;background:#fafafa}
.small{color:#666;font-size:12px}

/* New layout helpers */
.row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.header-card{border:1px solid #eee;border-radius:12px;padding:14px 16px;margin:10px 0;background:#fff}
.header-grid{display:grid;grid-template-columns:1fr auto;gap:8px;align-items:center}
.meta{color:#666;font-size:12px}
.news-card{border:1px solid #eee;border-radius:12px;padding:14px 16px;margin:16px 0;background:#fff}
.section-title{font-weight:700;margin:6px 0 10px 0}
</style>
""", unsafe_allow_html=True)
st.markdown("""
<style>
/* --- Horizontal header + compact pills --- */
.header-line{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.company-name{font-size:20px;font-weight:800;letter-spacing:.2px}
.meta{color:#666;font-size:12px}

.pills{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-top:6px}
.pill{
  display:inline-flex;align-items:center;
  padding:6px 12px;border-radius:999px;
  background:#eef;color:#222;font-weight:700;font-size:13px
}
.pill.primary{background:#edf3ff;color:#1b3a8a}
.pill.positive{background:#e8f8ef;color:#0b6b2f}
.pill.negative{background:#ffe8e8;color:#a00}
.section-title{font-weight:800;margin:10px 0 8px 0}
</style>
""", unsafe_allow_html=True)


# -------------------- AUTH --------------------
# Initialize your own session keys (avoid "auth" name clash with form keys)
if "is_authed" not in st.session_state:
    st.session_state.is_authed = False
if "remember_me" not in st.session_state:
    st.session_state.remember_me = False

def login_view():
    st.title("🔒 Login")
    # Use a different form key and widget keys to avoid collisions
    with st.form("login_form"):
        u = st.text_input("Username", key="login_user")
        p = st.text_input("Password", type="password", key="login_pass")
        remember = st.checkbox("Remember me", key="login_remember")
        ok = st.form_submit_button("Sign in")

    if ok:
        if u == APP_USER and p == APP_PASS:
            st.session_state.is_authed = True
            st.session_state.remember_me = remember
            st.rerun()  # or st.experimental_rerun() on older Streamlit
        else:
            st.error("Invalid credentials")

# Gate
if not st.session_state.get("is_authed"):
    login_view()
    st.stop()

# -------------------- DB --------------------
client = MongoClient(MONGO_URI)
db_news = client[DB_NAME]
col_news = db_news[NEWS_COLL]
db_prev  = client[PREV_DB] if PREV_DB else db_news
col_prev = db_prev[PREV_COLL]

# -------------------- HELPERS --------------------
def _try_int(x):
    try: return int(str(x).strip())
    except: return None

def _parse_dt(s):
    try: return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except: return None
def _to_float_or_none(x):
    try:
        return float(x)
    except Exception:
        return None
def fmt_money_cr(x):               # value already in crores
    v = _to_float_or_none(x)
    return "-" if v is None else f"₹ {v:,.1f} cr"

def fmt1(x):  # 1-decimal with thousands
    v = _to_float_or_none(x)
    return "-" if v is None else f"{v:,.1f}"

# def fmt_money_mn(x):  # ₹ … mn, 1-decimal
#     v = _to_float_or_none(x)
#     return "-" if v is None else f"₹ {v:,.1f} mn"

def fmt_pct(x):  # 1-decimal % (no thousands)
    v = _to_float_or_none(x)
    return "-" if v is None else f"{v:.1f} %"


def fetch_preview_doc(company_query: str) -> Optional[Dict[str, Any]]:
    q = (company_query or "").strip()
    if not q:
        return None
    or_filters = [
        {"company_id": q.upper()},
        {"symbolmap.NSE": q.upper()},
        {"company_display": {"$regex": q, "$options":"i"}},
        {"company_key": {"$regex": q, "$options":"i"}},
        {"symbolmap.Company_Name": {"$regex": q, "$options":"i"}},
    ]
    docs = list(col_prev.find({"$or": or_filters}))
    if not docs:
        return None
    def keyer(d):
        for k in ("updated_at","created_at"):
            v = d.get(k)
            if v:
                try: return datetime.fromisoformat(v.replace("Z","+00:00"))
                except: pass
        return datetime.min
    docs.sort(key=keyer, reverse=True)
    return docs[0]

def chip(text, kind=None):
    kind = kind or ""
    st.markdown(f'<span class="badge {kind}">{text}</span>', unsafe_allow_html=True)

def build_broker_df(preview: Dict[str, Any]) -> pd.DataFrame:
    def r1(x):
        v = _to_float_or_none(x)
        return round(v, 1) if v is not None else None

    rows = []
    for b in (preview.get("broker_estimates") or []):
        pdf_name  = b.get("source_file") or b.get("report_id") or ""
        source_url = b.get("source_url") or ""  # optional if you store URLs
        rows.append({
            "Broker": b.get("broker_name"),
            "Published": (b.get("published_date","") or "")[:10],
            "Expected Sales (₹ cr)":  r1(b.get("expected_sales")),
            "Expected EBITDA (₹ cr)": r1(b.get("expected_ebitda")),
            "Expected PAT (₹ cr)":    r1(b.get("expected_pat")),
            "EBITDA Margin %":        r1(b.get("ebitda_margin_percent")),
            "PAT Margin %":           r1(b.get("pat_margin_percent")),
            "Commentary": b.get("commentary",""),
            "PDF": source_url or pdf_name,
        })

    df = pd.DataFrame(rows)
    for c in ["Expected Sales (₹ cr)","Expected EBITDA (₹ cr)","Expected PAT (₹ cr)",
              "EBITDA Margin %","PAT Margin %"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round(1)
    return df


# ---------- NEW: Dropdown options (only companies that have news) ----------
@st.cache_data(ttl=600)
def get_company_options() -> List[Dict[str, Any]]:
    """
    Build a dropdown list of companies that have news in NEWS_COLL.
    Label: Company_Name — NSE X | BSE Y | ISIN Z  (count)
    """
    pipeline = [
        {"$group": {"_id": {
            "nse": "$symbolmap.NSE",
            "bse": "$symbolmap.BSE",
            "name": "$symbolmap.Company_Name",
            "isin": "$company"
        }, "count": {"$sum": 1}}},
        {"$sort": {"_id.name": 1}}
    ]
    items = list(col_news.aggregate(pipeline))
    out = []
    for it in items:
        _id = it["_id"] or {}
        nse = _id.get("nse")
        bse = _id.get("bse")
        name = _id.get("name")
        isin = _id.get("isin")
        label = f"{name or nse or isin or bse} — NSE {nse or '-'} | BSE {bse or '-'} | ISIN {isin or '-'}  ({it['count']})"
        out.append({"label": label, "nse": nse, "bse": bse, "name": name, "isin": isin, "count": it["count"]})
    return out

# ---------- NEW: Fetch ALL news docs for selected company ----------
def fetch_actual_docs(opt: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
    """Fetch news docs for the selected company, newest first."""
    ors = []
    if opt.get("nse"):  ors.append({"symbolmap.NSE": opt["nse"]})
    if opt.get("bse"):  ors.append({"symbolmap.BSE": opt["bse"]})
    if opt.get("isin"): ors.append({"company": opt["isin"]})
    if opt.get("name"): ors.append({"symbolmap.Company_Name": opt["name"]})
    if not ors: return []
    # dt_tm is "YYYY-MM-DD HH:MM:SS" so string sort works for chronological order
    return list(col_news.find({"$or": ors}).sort("dt_tm", -1).limit(limit))

# ---------- NEW: Cleaner horizontal header + content ----------
def render_actual_card(doc: Dict[str, Any]):
    sym = doc.get("symbolmap", {}) or {}
    company = sym.get("Company_Name") or sym.get("NSE") or "Company"
    filed = doc.get("dt_tm", "")

    # Header (horizontal)
    st.markdown('<div class="header-card">', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="header-line">
          <div class="company-name">{company}</div>
          <div class="meta">Filed: {filed}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Company identifiers — one HTML block (horizontal pills)
    chips = []
    if sym.get("NSE"): chips.append(f'<span class="pill primary">NSE {sym.get("NSE")}</span>')
    if sym.get("BSE"): chips.append(f'<span class="pill primary">BSE {sym.get("BSE")}</span>')
    if doc.get("company"): chips.append(f'<span class="pill primary">ISIN {doc.get("company")}</span>')
    if doc.get("category"): chips.append(f'<span class="pill">{doc.get("category")}</span>')
    if doc.get("subcategory"): chips.append(f'<span class="pill">{doc.get("subcategory")}</span>')
    st.markdown(f'<div class="pills">{"".join(chips)}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)  # close header-card

    # Sentiment / impact — bold & eye-catching pills in ONE block
    sentiment = doc.get("sentiment", "-")
    scls = "negative" if "neg" in sentiment.lower() else ("positive" if "pos" in sentiment.lower() else "")
    # Pills block (keep your existing sentiment/sensitivity/timeline lines)
    pills = [
        f'<span class="pill {scls}"><b>Sentiment:</b>&nbsp;{sentiment}</span>',
        f'<span class="pill"><b>Sensitivity:</b>&nbsp;{doc.get("sensitivity","-")}</span>',
        f'<span class="pill"><b>Timeline:</b>&nbsp;{doc.get("timelineflag")}</span>',
        f'<span class="pill"><b>Impact Score:</b>&nbsp;{fmt1(doc.get("impactscore"))}/10</span>',
    ]
    st.markdown(f'<div class="pills">{"".join(pills)}</div>', unsafe_allow_html=True)
    
    # Progress bar (numeric value, not formatted string)
    score_f = _to_float_or_none(doc.get("impactscore")) or 0.0
    st.progress(max(0.0, min(1.0, score_f/10.0)))

    # st.markdown(f'<div class="pills">{"".join(pills)}</div>', unsafe_allow_html=True)

    # # Optional progress bar (kept)
    # try:
    #     st.progress(float(doc.get("impactscore", 0)) / 10.0)
    # except:
    #     st.progress(0.0)

    # Impact (NEW) — shown before Short Summary
    impact_txt = (doc.get("impact") or "").strip()
    if impact_txt:
        st.markdown('<div class="section-title">Impact</div>', unsafe_allow_html=True)
        st.write(impact_txt)


    # Short & Detailed summaries
    if doc.get("shortsummary"):
        st.markdown('<div class="section-title">Short Summary</div>', unsafe_allow_html=True)
        st.write(doc["shortsummary"])

    if doc.get("summary"):
        st.markdown('<div class="section-title">Detailed Summary</div>', unsafe_allow_html=True)
        st.write(doc["summary"])

    # PDF links
    live = doc.get("pdf_link_live"); hist = doc.get("pdf_link")
    if live or hist:
        st.markdown('<div class="section-title">PDF Links</div>', unsafe_allow_html=True)
        if live: st.markdown(f"- [Open Live PDF]({live})")
        if hist: st.markdown(f"- [Open Historical PDF]({hist})")

    # Raw JSON (collapsed)
    with st.expander("Raw JSON"):
        st.json(doc)


# -------------------- UI --------------------
with st.sidebar:
    st.markdown("### 🔍 Company (only those with news)")
    options = get_company_options()
    if not options:
        st.error("No companies found in news collection.")
        st.stop()

    # Number of news items to append (newest first)
    # Default: min(20, count of first option)
    default_max = min(20, max(1, options[0]["count"]))
    max_items = st.slider("Max news to show", 1, 50, default_max, help="Show up to N latest news items")
    selected = st.selectbox(
        "Search & select",
        options,
        index=0,
        format_func=lambda o: o["label"],
        key="company_select",
    )

    st.caption(f"Showing up to {max_items} latest news items.")
    st.divider()
    if st.button("Logout"):
        st.session_state.is_authed = False
        st.session_state.remember_me = False
        st.rerun()

st.title("Results Viewer")

# ========== ALL ACTUAL NEWS FOR SELECTED COMPANY ==========
docs = fetch_actual_docs(selected, limit=max_items)
if not docs:
    st.info("No news for this company.")
else:
    for i, doc in enumerate(docs, start=1):
        st.markdown(f"#### News {i}")
        render_actual_card(doc)
        st.divider()

# ========== PREDICTED RESULTS APPENDED AT END ==========
preview_query = selected.get("nse") or selected.get("isin") or selected.get("name")
preview = fetch_preview_doc(preview_query)

if preview:
    st.markdown("### Predicted Results (from `company_result_previews`)")

    # Consensus KPIs
    cons = preview.get("consensus") or {}

    kpis = [
        ("Expected Sales",  fmt_money_cr((cons.get("expected_sales") or {}).get("mean"))),
        ("Expected EBITDA", fmt_money_cr((cons.get("expected_ebitda") or {}).get("mean"))),
        ("Expected PAT",    fmt_money_cr((cons.get("expected_pat") or {}).get("mean"))),
        ("EBITDA Margin %", fmt_pct((cons.get("ebitda_margin_percent") or {}).get("mean"))),
        ("PAT Margin %",    fmt_pct((cons.get("pat_margin_percent") or {}).get("mean"))),
    ]
    
    kpi_cols = st.columns(len(kpis))
    for col, (label, val_str) in zip(kpi_cols, kpis):
        with col:
            st.markdown(
                f'<div class="kpi"><div class="small">{label}</div>'
                f'<div style="font-size:20px;font-weight:700">{val_str}</div></div>',
                unsafe_allow_html=True
            )



    # Broker table
    df = build_broker_df(preview)
    if not df.empty:
        if "http" in "".join(df["PDF"].astype(str).tolist()):
            st.dataframe(
                df,
                hide_index=True,
                column_config={
                    "PDF": st.column_config.LinkColumn("PDF", help="Open source document")
                },
                use_container_width=True,
            )
        else:
            st.dataframe(df, hide_index=True, use_container_width=True)

        st.download_button(
            "Download CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"{preview.get('company_id','predicted')}_broker_estimates.csv",
            mime="text/csv"
        )
    else:
        st.info("No broker estimates found in preview doc.")
else:
    st.info("No predicted results found for this company.")
