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
</style>
""", unsafe_allow_html=True)

# -------------------- AUTH --------------------
def login_view():
    st.title("🔒 Login")
    with st.form("auth"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        remember = st.checkbox("Remember me")
        ok = st.form_submit_button("Sign in")
    if ok:
        if u == APP_USER and p == APP_PASS:
            st.session_state["auth"] = True
            st.session_state["remember"] = remember
            st.rerun()
        else:
            st.error("Invalid credentials")

if not st.session_state.get("auth"):
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

def fetch_actual_doc(company_query: str) -> Optional[Dict[str, Any]]:
    q = (company_query or "").strip()
    if not q:
        return None
    scrip = _try_int(q)
    or_filters = [
        {"symbolmap.NSE": q.upper()},
        {"symbolmap.SELECTED": q.upper()},
        {"company": q},  # ISIN
        {"symbolmap.Company_Name": {"$regex": q, "$options":"i"}},
    ]
    if scrip is not None:
        or_filters.append({"scrip_cd": scrip})
        or_filters.append({"symbolmap.BSE": scrip})
    docs = list(col_news.find({"$or": or_filters}))
    if not docs: 
        return None
    def keyer(d):
        dt = _parse_dt(d.get("dt_tm","") or "")
        return dt or datetime.min
    docs.sort(key=keyer, reverse=True)
    return docs[0]

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
    rows = []
    for b in (preview.get("broker_estimates") or []):
        pdf_name = b.get("source_file") or b.get("report_id") or ""
        source_url = b.get("source_url") or ""  # optional if you store it
        rows.append({
            "Broker": b.get("broker_name"),
            "Published": (b.get("published_date","") or "")[:10],
            "Expected Sales (₹ mn)": b.get("expected_sales"),
            "Expected EBITDA (₹ mn)": b.get("expected_ebitda"),
            "Expected PAT (₹ mn)": b.get("expected_pat"),
            "EBITDA Margin %": b.get("ebitda_margin_percent"),
            "PAT Margin %": b.get("pat_margin_percent"),
            "Commentary": b.get("commentary",""),
            "PDF": source_url or pdf_name,
        })
    return pd.DataFrame(rows)

# -------------------- UI --------------------
with st.sidebar:
    st.markdown("### 🔍 Find Company")
    q_default = st.session_state.get("company_query", "COROMANDEL")
    company_query = st.text_input("NSE / BSE / ISIN / Name", value=q_default)
    go = st.button("Fetch")
    if go:
        st.session_state["company_query"] = company_query
        st.rerun()
    st.divider()
    if st.button("Logout"):
        st.session_state.clear(); st.rerun()

company_query = st.session_state.get("company_query", "COROMANDEL")
st.title("Results Viewer")

actual = fetch_actual_doc(company_query)
preview = fetch_preview_doc(company_query)

if not actual and not preview:
    st.warning("No documents found for this company.")
    st.stop()

# ========== ACTUAL DOC SECTION ==========
if actual:
    sym = actual.get("symbolmap", {})
    st.subheader(sym.get("Company_Name") or sym.get("NSE") or "Company")
    chips = []
    chips.append(f'NSE {sym.get("NSE","-")}')
    if sym.get("BSE"): chips.append(f'BSE {sym.get("BSE")}')
    if actual.get("company"): chips.append(f'ISIN {actual.get("company")}')
    chips.append(actual.get("category","-"))
    if actual.get("subcategory"): chips.append(actual.get("subcategory","-"))
    for c in chips: chip(c)

    filed = actual.get("dt_tm","")
    st.caption(f"Filed: {filed}")

    # Sentiment/impact row
    snt = (actual.get("sentiment") or "").lower()
    snt_kind = "neg" if "neg" in snt else ("pos" if "pos" in snt else "")
    st.markdown('<div class="card">', unsafe_allow_html=True)
    chip(f"Sentiment: {actual.get('sentiment','-')}", snt_kind)
    chip(f"Sensitivity: {actual.get('sensitivity','-')}")
    chip(f"Timeline: {actual.get('timelineflag')}")
    score = actual.get("impactscore")
    chip(f"Impact Score: {score if score is not None else '-'} / 10")
    try:
        st.progress(float(score)/10)
    except:
        st.progress(0.0)
    st.markdown('</div>', unsafe_allow_html=True)

    # Short & detailed summaries
    if actual.get("shortsummary"):
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("#### Short Summary")
        st.write(actual["shortsummary"])
        st.markdown('</div>', unsafe_allow_html=True)

    if actual.get("summary"):
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("#### Detailed Summary")
        st.write(actual["summary"])
        st.markdown('</div>', unsafe_allow_html=True)

    # PDF links (hypertext)
    live = actual.get("pdf_link_live"); hist = actual.get("pdf_link")
    if live or hist:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("**PDF Links**")
        if live: st.markdown(f"- [Open Live PDF]({live})")
        if hist: st.markdown(f"- [Open Historical PDF]({hist})")
        st.markdown('</div>', unsafe_allow_html=True)

    # Raw JSON expander
    with st.expander("Raw JSON"):
        st.json(actual)

# ========== PREDICTED RESULTS APPENDED AT END ==========
if preview:
    st.markdown("### Predicted Results (from `company_result_previews`)")

    # Consensus KPIs
    cons = preview.get("consensus") or {}
    kpi_cols = st.columns(5)
    kpi_map = [
        ("Expected Sales", cons.get("expected_sales",{}).get("mean")),
        ("Expected EBITDA", cons.get("expected_ebitda",{}).get("mean")),
        ("Expected PAT", cons.get("expected_pat",{}).get("mean")),
        ("EBITDA Margin %", cons.get("ebitda_margin_percent",{}).get("mean")),
        ("PAT Margin %", cons.get("pat_margin_percent",{}).get("mean")),
    ]
    for col, (label, val) in zip(kpi_cols, kpi_map):
        with col:
            st.markdown(f'<div class="kpi"><div class="small">{label}</div><div style="font-size:20px;font-weight:700">{val if val is not None else "-"}</div></div>', unsafe_allow_html=True)

    # Broker table
    df = build_broker_df(preview)
    if not df.empty:
        # If you have URLs in the 'PDF' column, render as link column
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
