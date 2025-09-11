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

# Actuals source -> LatestCmotData
ACTUAL_DB   = os.getenv("ACTUAL_DB", DB_NAME)                           # set ACTUAL_DB in .env if different
ACTUAL_COLL = os.getenv("ACTUAL_COLLECTION", "LatestCmotData")

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
if "is_authed" not in st.session_state:
    st.session_state.is_authed = False
if "remember_me" not in st.session_state:
    st.session_state.remember_me = False

def login_view():
    st.title("🔒 Login")
    with st.form("login_form"):
        u = st.text_input("Username", key="login_user")
        p = st.text_input("Password", type="password", key="login_pass")
        remember = st.checkbox("Remember me", key="login_remember")
        ok = st.form_submit_button("Sign in")

    if ok:
        if u == APP_USER and p == APP_PASS:
            st.session_state.is_authed = True
            st.session_state.remember_me = remember
            st.rerun()
        else:
            st.error("Invalid credentials")

# Gate
if not st.session_state.get("is_authed"):
    login_view()
    st.stop()

# -------------------- DB --------------------
client   = MongoClient(MONGO_URI)
db_news  = client[DB_NAME]
col_news = db_news[NEWS_COLL]
db_prev  = client[PREV_DB] if PREV_DB else db_news
col_prev = db_prev[PREV_COLL]
db_actual = client[ACTUAL_DB]
col_fin   = db_actual[ACTUAL_COLL]

# -------------------- HELPERS --------------------
def _try_int(x):
    try: return int(str(x).strip())
    except: return None

def _to_float_or_none(x):
    try: return float(x)
    except Exception: return None

def fmt_money_cr(x):               # value in crores
    v = _to_float_or_none(x)
    return "-" if v is None else f"₹ {v:,.1f} cr"

def fmt1(x):
    v = _to_float_or_none(x)
    return "-" if v is None else f"{v:,.1f}"

def fmt_pct(x):
    v = _to_float_or_none(x)
    return "-" if v is None else f"{v:.1f} %"

# Month mapping for period parsing
_MONTHS = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}

def _period_to_dt(label: Optional[str]) -> datetime:
    """
    Parse 'Jun2025' or 'Jun-2025' -> datetime(2025,6,1)
    """
    if not label:
        return datetime.min
    m = re.match(r"([A-Za-z]{3})-?(\d{4})", str(label).strip())
    if not m:
        return datetime.min
    mon = _MONTHS.get(m.group(1)[:3].title(), 1)
    yr  = int(m.group(2))
    return datetime(yr, mon, 1)

def _parse_results_period_label(label: Optional[str]) -> datetime:
    """Parse 'Quarter ended 30-Jun-2025' -> datetime(2025,6,30)."""
    if not label:
        return datetime.min
    m = re.search(r'(\d{1,2})-([A-Za-z]{3})-(\d{4})', str(label))
    if not m:
        return datetime.min
    day = int(m.group(1)); mon = _MONTHS.get(m.group(2)[:3].title(), 1); yr = int(m.group(3))
    return datetime(yr, mon, day)

def _to_crores(val, unit: Optional[str]) -> Optional[float]:
    """
    Normalize numeric to ₹ crores.
    For LatestCmotData you already store 'unit': 'cr' -> factor 1.0 (safe no-op).
    """
    v = _to_float_or_none(val)
    if v is None: return None
    u = (unit or "").strip().lower()
    if u in ("cr","crore","crores","₹ cr","inr cr"): factor = 1.0
    elif u in ("mn","million","millions"): factor = 0.1
    elif u in ("bn","billion","billions"): factor = 100.0
    else: factor = 1.0
    return v * factor

# -------- Preview (predictions) --------
def fetch_preview_doc(company_query: str) -> Optional[Dict[str, Any]]:
    q = (company_query or "").strip()
    if not q: return None
    or_filters = [
        {"company_id": q.upper()},
        {"symbolmap.NSE": q.upper()},
        {"company_display": {"$regex": q, "$options":"i"}},
        {"company_key": {"$regex": q, "$options":"i"}},
        {"symbolmap.Company_Name": {"$regex": q, "$options":"i"}},
        {"company": q.upper()},  # ISIN exact
    ]
    if q.isdigit():
        try: or_filters.append({"symbolmap.BSE": int(q)})
        except: pass

    docs = list(col_prev.find({"$or": or_filters}))
    if not docs: return None

    def keyer(d):
        for k in ("updated_at","created_at"):
            v = d.get(k)
            if v:
                try: return datetime.fromisoformat(str(v).replace("Z","+00:00"))
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
        pdf_name   = b.get("source_file") or b.get("report_id") or ""
        source_url = b.get("source_url") or ""
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

# -------- Actuals (LatestCmotData) extractors --------
def _extract_from_latest_cmot(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    LatestCmotData structure:
      - doc["Consolidated"] or doc["Standalone"] is a dict of { "Jun2025": {...}, "actual": {"Jun2025": {... unit: 'cr'}} }
    Prefer Consolidated; else Standalone.
    Use 'actual' if present, else latest quarter key.
    """
    for basis in ("Consolidated", "Standalone"):
        block = doc.get(basis)
        if not isinstance(block, dict):
            continue

        # 1) Prefer 'actual' sub-block
        actual_block = block.get("actual")
        if isinstance(actual_block, dict) and actual_block:
            # pick latest period from keys like "Jun2025"
            period_keys = [k for k in actual_block.keys() if k and isinstance(k, str)]
            period_keys.sort(key=lambda x: _period_to_dt(x), reverse=True)
            sel_key = period_keys[0]
            m = actual_block.get(sel_key) or {}
            unit = m.get("unit")  # already 'cr'
            sales  = _to_crores(m.get("net_sales"),  unit)
            ebitda = _to_crores(m.get("ebitda"),     unit)
            pat    = _to_crores(m.get("net_profit"), unit)
            emarg  = _to_float_or_none(m.get("ebitda_margin"))
            pmarg  = _to_float_or_none(m.get("pat_margin"))
            return {
                "basis": basis,
                "period_label": sel_key,
                "sales": sales,
                "ebitda": ebitda,
                "pat": pat,
                "ebitda_margin_percent": emarg,
                "pat_margin_percent": pmarg,
            }

        # 2) Fallback to latest quarter entry in the block (exclude the 'actual' key)
        quarter_keys = [k for k in block.keys() if k != "actual" and isinstance(block.get(k), dict)]
        if quarter_keys:
            quarter_keys.sort(key=lambda x: _period_to_dt(x), reverse=True)
            sel_key = quarter_keys[0]
            m = block.get(sel_key) or {}
            # assume values in crores already
            sales  = _to_float_or_none(m.get("net_sales"))
            ebitda = _to_float_or_none(m.get("ebitda") or m.get("operating_profit"))
            pat    = _to_float_or_none(m.get("net_profit"))
            emarg  = _to_float_or_none(m.get("ebitda_margin"))
            pmarg  = _to_float_or_none(m.get("pat_margin"))
            return {
                "basis": basis,
                "period_label": sel_key,
                "sales": sales,
                "ebitda": ebitda,
                "pat": pat,
                "ebitda_margin_percent": emarg,
                "pat_margin_percent": pmarg,
            }
    return None

# Backward-compat extractor (older 'results' schema) — kept for safety
def _extract_from_results(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    res = (doc.get("results") or {})
    cons = res.get("Consolidated") or []
    stand = res.get("Standalone") or []
    basis = arr = None
    if cons: basis, arr = "Consolidated", cons
    elif stand: basis, arr = "Standalone", stand
    else: return None

    def _key(it):
        lbl = ((it.get("period") or {}).get("label")) or ""
        return _parse_results_period_label(lbl)
    arr_sorted = sorted(arr, key=_key, reverse=True)
    item = arr_sorted[0]
    metrics = (item.get("metrics") or {}); unit = metrics.get("unit")

    sales_cr  = _to_crores(metrics.get("Sales"), unit)
    ebitda_cr = _to_crores(metrics.get("EBITDA") or metrics.get("Ebitda") or metrics.get("EBITDA_Profit"), unit)
    pat_cr    = _to_crores(metrics.get("PAT") or metrics.get("Net_Profit") or metrics.get("Profit_After_Tax"), unit)
    emargin   = _to_float_or_none(metrics.get("EBITDA_Margin") or metrics.get("Ebitda_Margin") or metrics.get("EBITDA_Margin_%"))
    pmargin   = _to_float_or_none(metrics.get("PAT_Margin") or metrics.get("PAT_Margin_%"))

    if emargin is None and ebitda_cr and sales_cr:
        emargin = (ebitda_cr / sales_cr) * 100.0
    if pmargin is None and pat_cr and sales_cr:
        pmargin = (pat_cr / sales_cr) * 100.0

    period_label = ((item.get("period") or {}).get("label")) or (doc.get("period") or None)
    return {
        "basis": basis,
        "period_label": period_label,
        "sales": sales_cr,
        "ebitda": ebitda_cr,
        "pat": pat_cr,
        "ebitda_margin_percent": emargin,
        "pat_margin_percent": pmargin,
    }

def fetch_preview_doc_query(selected: Dict[str, Any]) -> Optional[str]:
    return (
        selected.get("nse")
        or selected.get("isin")
        or selected.get("name")
        or (str(selected.get("bse")) if selected.get("bse") is not None else None)
    )

def fetch_actual_results(company_query: str,
                         col_fin_handle,
                         basis: Optional[str] = None,
                         report_period: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch actuals from LatestCmotData (preferred) with fallback to older 'results' schema.
    """
    q = (company_query or "").strip()
    if not q:
        return None

    or_filters = [
        {"company_id": q.upper()},
        {"symbolmap.NSE": q.upper()},
        {"company": q.upper()},  # ISIN
        {"company_display": {"$regex": q, "$options":"i"}},
        {"company_key": {"$regex": q, "$options":"i"}},
        {"symbolmap.Company_Name": {"$regex": q, "$options":"i"}},
    ]
    if q.isdigit():
        try: or_filters.append({"symbolmap.BSE": int(q)})
        except: pass

    docs = list(col_fin_handle.find({"$or": or_filters}))
    if not docs:
        return None

    def sort_key(d):
        # Prefer updated_at if present
        v = d.get("updated_at")
        if v:
            try: return (2, datetime.fromisoformat(str(v).replace("Z","+00:00")))
            except: pass
        return (0, datetime.min)

    docs.sort(key=sort_key, reverse=True)
    doc = docs[0]

    # 1) LatestCmotData path
    extracted = _extract_from_latest_cmot(doc)
    if extracted:
        return extracted

    # 2) Fallback: older 'results' array style
    extracted2 = _extract_from_results(doc)
    if extracted2:
        return extracted2

    # 3) Final fallback: flat keys (rare)
    def _deep_get(d: Any, key: str) -> Optional[Any]:
        if not isinstance(d, dict): return None
        if key in d: return d[key]
        for v in d.values():
            if isinstance(v, dict):
                got = _deep_get(v, key)
                if got is not None: return got
        return None

    def _pick_any(doc_: Dict[str, Any], candidates) -> Optional[Any]:
        for k in candidates:
            v = _deep_get(doc_, k)
            if v is not None: return v
        return None

    sales   = _pick_any(doc, ["actual_sales","sales","net_sales","revenue","total_income"])
    ebitda  = _pick_any(doc, ["actual_ebitda","ebitda","operating_profit"])
    pat     = _pick_any(doc, ["actual_pat","pat","net_profit","profit_after_tax","net_profit"])
    e_marg  = _pick_any(doc, ["ebitda_margin_percent","ebitda_margin"])
    p_marg  = _pick_any(doc, ["pat_margin_percent","pat_margin"])
    return {
        "basis": doc.get("basis"),
        "period_label": doc.get("period"),
        "sales": _to_float_or_none(sales),
        "ebitda": _to_float_or_none(ebitda),
        "pat": _to_float_or_none(pat),
        "ebitda_margin_percent": _to_float_or_none(e_marg),
        "pat_margin_percent": _to_float_or_none(p_marg),
    }

# ---------- Dropdown options (only companies that have news) ----------
@st.cache_data(ttl=600)
def get_company_options() -> List[Dict[str, Any]]:
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
        nse = _id.get("nse"); bse = _id.get("bse"); name = _id.get("name"); isin = _id.get("isin")
        label = f"{name or nse or isin or bse} — NSE {nse or '-'} | BSE {bse or '-'} | ISIN {isin or '-'}  ({it['count']})"
        out.append({"label": label, "nse": nse, "bse": bse, "name": name, "isin": isin, "count": it["count"]})
    return out

# ---------- Fetch ALL news docs for selected company ----------
def fetch_actual_docs(opt: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
    ors = []
    if opt.get("nse"):  ors.append({"symbolmap.NSE": opt["nse"]})
    if opt.get("bse"):  ors.append({"symbolmap.BSE": opt["bse"]})
    if opt.get("isin"): ors.append({"company": opt["isin"]})
    if opt.get("name"): ors.append({"symbolmap.Company_Name": opt["name"]})
    if not ors: return []
    return list(col_news.find({"$or": ors}).sort("dt_tm", -1).limit(limit))

# ---------- Render a news card ----------
def render_actual_card(doc: Dict[str, Any]):
    sym = doc.get("symbolmap", {}) or {}
    company = sym.get("Company_Name") or sym.get("NSE") or "Company"
    filed = doc.get("dt_tm", "")

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

    chips = []
    if sym.get("NSE"): chips.append(f'<span class="pill primary">NSE {sym.get("NSE")}</span>')
    if sym.get("BSE"): chips.append(f'<span class="pill primary">BSE {sym.get("BSE")}</span>')
    if doc.get("company"): chips.append(f'<span class="pill primary">ISIN {doc.get("company")}</span>')
    if doc.get("category"): chips.append(f'<span class="pill">{doc.get("category")}</span>')
    if doc.get("subcategory"): chips.append(f'<span class="pill">{doc.get("subcategory")}</span>')
    st.markdown(f'<div class="pills">{"".join(chips)}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    sentiment = doc.get("sentiment", "-")
    scls = "negative" if isinstance(sentiment, str) and ("neg" in sentiment.lower()) else ("positive" if isinstance(sentiment, str) and ("pos" in sentiment.lower()) else "")
    pills = [
        f'<span class="pill {scls}"><b>Sentiment:</b>&nbsp;{sentiment}</span>',
        f'<span class="pill"><b>Sensitivity:</b>&nbsp;{doc.get("sensitivity","-")}</span>',
        f'<span class="pill"><b>Timeline:</b>&nbsp;{doc.get("timelineflag")}</span>',
        f'<span class="pill"><b>Impact Score:</b>&nbsp;{int(_to_float_or_none(doc.get("impactscore")) or 0)}/10</span>',
    ]
    st.markdown(f'<div class="pills">{"".join(pills)}</div>', unsafe_allow_html=True)

    score_f = _to_float_or_none(doc.get("impactscore")) or 0.0  # 0..10
    st.progress(min(100, max(0, int(round(score_f * 10)))))     # 0..100

    impact_txt = (doc.get("impact") or "").strip()
    if impact_txt:
        st.markdown('<div class="section-title">Impact</div>', unsafe_allow_html=True)
        st.write(impact_txt)
    impact_deduct = (doc.get("impactscore_deduction") or "").strip()
    if impact_deduct:
        st.markdown('<div class="section-title">Impactscore Deduction</div>', unsafe_allow_html=True)
        st.write(impact_deduct)

    if doc.get("shortsummary"):
        st.markdown('<div class="section-title">Short Summary</div>', unsafe_allow_html=True)
        st.write(doc["shortsummary"])

    if doc.get("summary"):
        st.markdown('<div class="section-title">Detailed Summary</div>', unsafe_allow_html=True)
        st.write(doc["summary"])

    live = doc.get("pdf_link_live"); hist = doc.get("pdf_link")
    if live or hist:
        st.markdown('<div class="section-title">PDF Links</div>', unsafe_allow_html=True)
        if live: st.markdown(f"- [Open Live PDF]({live})")
        if hist: st.markdown(f"- [Open Historical PDF]({hist})")

    with st.expander("Raw JSON"):
        st.json(doc)

# -------------------- UI --------------------
with st.sidebar:
    st.markdown("### 🔍 Company (only those with news)")
    options = get_company_options()
    if not options:
        st.error("No companies found in news collection.")
        st.stop()

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

# ========== PREDICTED RESULTS + ACTUALS (VERTICAL TABLE) ==========
preview_query = fetch_preview_doc_query(selected)
preview = fetch_preview_doc(preview_query) if preview_query else None

if preview:
    st.markdown("### Results vs Predictions")

    # Consensus KPIs (predicted)
    cons = preview.get("consensus") or {}
    pred_sales  = (cons.get("expected_sales") or {}).get("mean")
    pred_ebitda = (cons.get("expected_ebitda") or {}).get("mean")
    pred_pat    = (cons.get("expected_pat") or {}).get("mean")
    pred_emarg  = (cons.get("ebitda_margin_percent") or {}).get("mean")
    pred_pmarg  = (cons.get("pat_margin_percent") or {}).get("mean")

    # Actuals
    actual = fetch_actual_results(preview_query, col_fin_handle=col_fin) or {}

    def _surprise_pct(pred, act):
        try:
            p = float(pred) if pred is not None else None
            a = float(act)  if act  is not None else None
            if p is None or a is None or p == 0.0: return None
            return (a - p) / p * 100.0
        except: return None

    rows = [
        ("Sales (₹ cr)",      pred_sales,  actual.get("sales")),
        ("EBITDA (₹ cr)",     pred_ebitda, actual.get("ebitda")),
        ("PAT (₹ cr)",        pred_pat,    actual.get("pat")),
        ("EBITDA Margin (%)", pred_emarg,  actual.get("ebitda_margin_percent")),
        ("PAT Margin (%)",    pred_pmarg,  actual.get("pat_margin_percent")),
    ]

    table = []
    for metric, pred, act in rows:
        is_pct = "Margin" in metric
        pred_disp = fmt_pct(pred) if is_pct else fmt_money_cr(pred)
        act_disp  = fmt_pct(act)  if is_pct else fmt_money_cr(act)
        surprise  = None if is_pct else _surprise_pct(pred, act)
        table.append({
            "Metric": metric,
            "Predicted": pred_disp,
            "Actual": act_disp,
            "Surprise %": (f"{surprise:.1f} %" if surprise is not None else "-")
        })

    df_kpi = pd.DataFrame(table, columns=["Metric","Predicted","Actual","Surprise %"])
    st.dataframe(df_kpi, hide_index=True, use_container_width=True)

    _basis = actual.get("basis") or "—"
    _rper  = actual.get("period_label") or (preview.get("report_period") or "—")
    st.caption(f"Basis: {_basis} · Period: {_rper}")

    # -------- Broker table (unchanged) --------
    df = build_broker_df(preview)
    if not df.empty:
        if "http" in "".join(df["PDF"].astype(str).tolist()):
            st.dataframe(
                df,
                hide_index=True,
                column_config={"PDF": st.column_config.LinkColumn("PDF", help="Open source document")},
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
