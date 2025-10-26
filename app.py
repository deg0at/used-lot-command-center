# ==============================================================
# Used Lot Command Center — AI Edition (v5)
# Stateful: persists processed data across reruns via session_state
# Adds: "Use last inventory" + "Use existing Carfax cache" checkboxes
# ==============================================================

import io, re, zipfile, json, os, traceback
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime
from pypdf import PdfReader

# ---------- Local modules ----------
from modules.carfax_cache import load_cache, get_cached, upsert_cache
from modules.ai_query import interpret_query

# ---------- Optional OpenAI ----------
try:
    from openai import OpenAI
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", None)
    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    OPENAI_API_KEY = None
    openai_client = None

# ---------- Config ----------
st.set_page_config(page_title="Used Lot Command Center — AI", layout="wide")
st.title("🚗 Used Lot Command Center — AI Edition")

# ---------- Helpers ----------
VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")

def to_num(x):
    try:
        return float(str(x).replace("$","").replace(",","").strip())
    except:
        return None

def to_excel_bytes(df: pd.DataFrame) -> io.BytesIO:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    bio.seek(0)
    return bio

def extract_pdf_lines(file_like):
    reader = PdfReader(file_like)
    lines = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        lines.extend([ln for ln in txt.splitlines() if ln.strip()])
    return lines

def parse_carfax(lines, fname=""):
    vin = None
    for ln in lines:
        m = VIN_RE.search(ln)
        if m:
            vin = m.group(1)
            break
    if not vin:
        m = VIN_RE.search((fname or "").upper())
        if m:
            vin = m.group(1)
    if not vin:
        return None

    joined = "\n".join(lines).lower()
    sev = "none"
    if "accident" in joined or "damage" in joined:
        if "severe" in joined: sev = "severe"
        elif "moderate" in joined: sev = "moderate"
        elif "minor" in joined:   sev = "minor"

    owners = 1
    m = re.search(r"(\d+)\s+owner", joined)
    if m:
        try: owners = int(m.group(1))
        except: pass

    services = 0
    m = re.search(r"service\s+history\s+records?:?\s*(\d+)", joined)
    if m:
        try: services = int(m.group(1))
        except: pass

    usage = ""
    m = re.search(r"(personal|fleet|rental|commercial|taxi|lease)\s+use", joined)
    if m:
        usage = m.group(1)

    odo_issue = "Yes" if ("odometer" in joined and ("mismatch" in joined or "tamper" in joined or "inconsistent" in joined)) else "No"

    return {
        "VIN": vin,
        "AccidentSeverity": sev,
        "OwnerCount": owners,
        "ServiceEvents": services,
        "UsageType": usage,
        "OdometerIssue": odo_issue
    }

def parse_carfax_zip_with_cache(zip_file, cache):
    """Binary-safe parse; only parse PDFs not already cached by VIN."""
    results = {}
    with zipfile.ZipFile(zip_file) as z:
        pdfs = [n for n in z.namelist() if n.lower().endswith(".pdf")]

        for name in pdfs:
            m = VIN_RE.search(name.upper())
            vin = m.group(1) if m else None

            if vin:
                cached = get_cached(vin, cache)
                if cached:
                    results[vin] = cached
                    continue

            with z.open(name) as f:
                pdf_bytes = io.BytesIO(f.read())  # ensure binary mode
                lines = extract_pdf_lines(pdf_bytes)
            rec = parse_carfax(lines, name)
            if rec:
                results[rec["VIN"]] = rec
                upsert_cache(rec["VIN"], rec, cache)

    if not results:
        return pd.DataFrame(columns=[
            "VIN","AccidentSeverity","OwnerCount","ServiceEvents",
            "UsageType","OdometerIssue","last_updated"
        ])
    return pd.DataFrame(results.values())

# ---------- Session state bootstrapping ----------
ss = st.session_state
ss.setdefault("app_ready", False)       # True after first successful Process
ss.setdefault("inv_df", None)           # last inventory DF
ss.setdefault("inv_name", None)         # last inventory file name
ss.setdefault("cf_df", None)            # last parsed Carfax DF (optional)
ss.setdefault("cache_loaded", False)    # whether cache was loaded
ss.setdefault("data_df", None)          # last merged data DF

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Inventory & Carfax")
    use_last_inv = st.checkbox("Use last uploaded inventory", value=bool(ss.get("inv_df") is not None))
    inv_file = None if use_last_inv else st.file_uploader("Inventory (.csv or .xlsx)", type=["csv","xlsx"])

    use_cache_only = st.checkbox("Use existing Carfax cache (skip ZIP parsing)", value=False)
    carfax_zip = None if use_cache_only else st.file_uploader("Carfax ZIP (PDFs)", type=["zip"])
    st.caption("Tip: PDF filenames should include the 17-character VIN.")

    st.divider()
    st.header("View & Actions")
    view_mode = st.radio("View Mode", ["🧱 Card View", "📊 Table View"], horizontal=True)
    run_btn   = st.button("Process / Refresh")
    reset_btn = st.button("Reset session")

# Reset clears state but keeps cache file on disk
if reset_btn:
    for key in ["app_ready","inv_df","inv_name","cf_df","data_df","cache_loaded"]:
        ss[key] = None if key.endswith("_df") else False
    st.experimental_rerun()

# ---------- Early exit guard ----------
if not ss.get("app_ready") and not run_btn:
    st.info("Upload or reuse inventory and Carfax options in the sidebar, then click **Process / Refresh**.")
    st.stop()

# ---------- Main Logic (guarded) ----------
try:
    # -- Load or reuse inventory
    if not use_last_inv:
        if not inv_file:
            if not ss.get("inv_df") is not None:
                st.error("Please upload an inventory file or enable 'Use last uploaded inventory'.")
                st.stop()
        else:
            # read fresh upload
            if inv_file.name.lower().endswith(".csv"):
                raw = pd.read_csv(inv_file)
            else:
                raw = pd.read_excel(inv_file)
            raw.columns = [c.strip() for c in raw.columns]
            vin_col = next((c for c in raw.columns if "vin" in c.lower()), None)
            if not vin_col:
                st.error("No VIN column found in inventory.")
                st.stop()

            inv = pd.DataFrame({
                "VIN": raw[vin_col].astype(str).str.upper().str.strip(),
                "Year": raw.get("Year"),
                "Make": raw.get("Make"),
                "Model": raw.get("Model"),
                "Trim": raw.get("Trim"),
                "Body": raw.get("Body"),
                "Price": raw.get("Website Basis") if "Website Basis" in raw.columns else raw.get("Price"),
                "KBBValue": raw.get("MSRP / KBB") if "MSRP / KBB" in raw.columns else raw.get("KBB"),
                "Mileage": raw.get("Mileage"),
                "Days": raw.get("Days In Inv") if "Days In Inv" in raw.columns else raw.get("DaysInInventory"),
                "CPO": raw.get("CPO"),
                "Warranty": raw.get("Warr.") if "Warr." in raw.columns else raw.get("Warranty"),
                "Status": raw.get("Status")
            })

            ss["inv_df"] = inv
            ss["inv_name"] = inv_file.name

    # If user chose to reuse last inventory but we don't have it yet
    if use_last_inv and ss.get("inv_df") is None:
        st.error("No previous inventory found in session. Please upload once, then you can reuse it.")
        st.stop()

    inv = ss.get("inv_df")

    # -- Load cache & Carfax
    cache = load_cache()  # on-disk JSON cache (persists across sessions on same machine)
    ss["cache_loaded"] = True

    if use_cache_only:
        # Don't parse ZIP; just mark Carfax presence by cache keys
        cf = ss.get("cf_df")
        if cf is None or cf.empty or "VIN" not in (cf.columns if isinstance(cf, pd.DataFrame) else []):
            # Build a minimal cf from cache keys (presence only)
            cached_vins = list(cache.keys())
            cf = pd.DataFrame({"VIN": cached_vins})
        ss["cf_df"] = cf
    else:
        # Parse ZIP (if provided), otherwise reuse any previous cf_df
        if carfax_zip:
            with st.spinner("Parsing Carfax ZIP (skips cached VINs)…"):
                cf = parse_carfax_zip_with_cache(carfax_zip, cache)
            ss["cf_df"] = cf
        else:
            cf = ss.get("cf_df")
            if cf is None:
                # No ZIP and no prior Carfax DF; build minimal from cache
                cached_vins = list(cache.keys())
                cf = pd.DataFrame({"VIN": cached_vins})
                ss["cf_df"] = cf

    # Defensive: ensure cf has VIN column
    if cf is None or cf.empty or "VIN" not in cf.columns:
        st.warning("⚠️ No valid VINs extracted from Carfax source.")
        cf = pd.DataFrame(columns=["VIN"])

    # -- Merge
    data = inv.merge(cf, on="VIN", how="left")
    data["CarfaxUploaded"] = data["VIN"].isin(cf["VIN"]) if not cf.empty else False

    # -- Derived placeholders (replace with your real scoring when ready)
    rng = np.random.default_rng(42)
    data["Score"] = rng.integers(70, 96, len(data))
    data["SafetyScore"] = rng.integers(70, 96, len(data))
    data["ValueCategory"] = rng.choice(["Under Market","At Market","Over Market"], len(data))
    data["SalesMood"] = np.where(data["Score"]>=85,"🟢 Confident","🟡 Balanced")

    # Persist processed data for subsequent reruns
    ss["data_df"] = data
    ss["app_ready"] = True

except Exception as e:
    st.error("⚠️ An error stopped the app during processing:")
    st.exception(e)
    st.code(traceback.format_exc())
    st.stop()

# ---------- UI below uses persisted data (no reset on rerun) ----------
data = ss["data_df"].copy()

# ---------- Filters & AI Query ----------
st.divider()
st.subheader("🔍 Search & Filters")

query_text = st.text_input("Ask naturally (e.g., 'SUV under 25k with AWD and low miles'):")
use_ai = st.checkbox("Use AI Query", value=True)

fc1, fc2, fc3, fc4 = st.columns(4)
sc_min, sc_max = fc1.slider("Smart Score", 0, 100, (70, 100))
safety_cut = fc2.slider("Min Safety", 0, 100, 70)
make_sel = fc3.multiselect("Make", sorted(list(data["Make"].dropna().unique())))
val_sel = fc4.multiselect("Value Category", ["Under Market","At Market","Over Market"])

mask = pd.Series(True, index=data.index)
if "Score" in data.columns:
    mask &= data["Score"].between(sc_min, sc_max)
if "SafetyScore" in data.columns:
    mask &= data["SafetyScore"] >= safety_cut
if make_sel:
    mask &= data["Make"].isin(make_sel)
if val_sel:
    mask &= data["ValueCategory"].isin(val_sel)

# AI natural query → structured filters (doesn't reset processed state)
if use_ai and query_text.strip():
    with st.spinner("Analyzing your query..."):
        filters = interpret_query(query_text)
    st.caption(f"🧠 Interpreted filters: {filters}")

    if "Body" in filters and "Body" in data.columns:
        mask &= data["Body"].str.contains(filters["Body"], case=False, na=False)
    if "DriveTrain" in filters and "Drive Train" in data.columns:
        mask &= data["Drive Train"].str.contains(filters["DriveTrain"], case=False, na=False)
    if "Make" in filters and "Make" in data.columns:
        mask &= data["Make"].str.contains(filters["Make"], case=False, na=False)
    if "Model" in filters and "Model" in data.columns:
        mask &= data["Model"].str.contains(filters["Model"], case=False, na=False)
    if "PriceMax" in filters and "Price" in data.columns:
        mask &= data["Price"].apply(to_num) <= filters["PriceMax"]
    if "PriceMin" in filters and "Price" in data.columns:
        mask &= data["Price"].apply(to_num) >= filters["PriceMin"]
    if "MileageMax" in filters and "Mileage" in data.columns:
        mask &= data["Mileage"].apply(to_num) <= filters["MileageMax"]
    if "MileageMin" in filters and "Mileage" in data.columns:
        mask &= data["Mileage"].apply(to_num) >= filters["MileageMin"]

mask = mask.reindex(data.index, fill_value=False)
filtered = data.loc[mask].copy()

# ---------- KPIs ----------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Vehicles", f"{len(data)}")
col2.metric("Matches", f"{len(filtered)}")
col3.metric("Avg Smart", f"{np.mean(filtered['Score']) if not filtered.empty else 0:.1f}")
col4.metric("Carfax Attached", f"{(filtered['CarfaxUploaded'].mean()*100 if not filtered.empty else 0):.0f}%")

st.divider()
st.subheader(f"📋 Showing {len(filtered)} vehicles")

if filtered.empty:
    st.warning("No vehicles match your filters.")
else:
    if "Card" in view_mode:
        for _, r in filtered.head(50).iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([2,1,2])
                with c1:
                    yr = int(r['Year']) if pd.notna(r['Year']) else ''
                    st.markdown(f"**{yr} {r.get('Make','')} {r.get('Model','')} {r.get('Trim') or ''}**")
                    st.write(f"{r.get('Body','') or ''} • {r.get('Mileage')} mi • ${r.get('Price')}")
                    if r.get("CarfaxUploaded"): st.success("Carfax ✅")
                    else: st.warning("No Carfax")
                with c2:
                    st.metric("Smart", f"{r['Score']:.0f}")
                    st.metric("Safety", f"{r['SafetyScore']:.0f}")
                with c3:
                    st.caption(r.get("TalkTrack") or "")
                st.divider()
    else:
        cols = [
            "CarfaxUploaded","VIN","Year","Make","Model","Trim","Body",
            "Mileage","Price","KBBValue","ValueCategory",
            "AccidentSeverity","OwnerCount","ServiceEvents",
            "SafetyScore","Score","Status"
        ]
        cols = [c for c in cols if c in filtered.columns]
        st.dataframe(filtered[cols], use_container_width=True, hide_index=True)

# ---------- Downloads ----------
st.divider()
colA, colB = st.columns(2)
colA.download_button(
    "Download CSV",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name="filtered_results.csv",
    mime="text/csv"
)
colB.download_button(
    "Download Excel",
    data=to_excel_bytes(filtered),
    file_name="filtered_results.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
