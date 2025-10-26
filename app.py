# ==============================================================
# Used Lot Command Center — AI Edition (v4.2)
# Includes: Carfax Cache, AI Query, Card/Table Toggle, VIN-safe merge
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

# ---------- Carfax Parsing ----------
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
        elif "minor" in joined: sev = "minor"

    owners = 1
    m = re.search(r"(\d+)\s+owner", joined)
    if m:
        try:
            owners = int(m.group(1))
        except:
            pass

    services = 0
    m = re.search(r"service\s+history\s+records?:?\s*(\d+)", joined)
    if m:
        try:
            services = int(m.group(1))
        except:
            pass

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

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Upload Files")
    inv_file = st.file_uploader("Inventory (.csv or .xlsx)", type=["csv","xlsx"])
    carfax_zip = st.file_uploader("Carfax ZIP (PDFs)", type=["zip"])
    st.caption("Tip: PDF filenames should include the 17-character VIN.")
    st.divider()
    st.header("Preferences")
    view_mode = st.radio("View Mode", ["🧱 Card View","📊 Table View"], horizontal=True)
    run_btn = st.button("Process")

if not run_btn:
    st.info("Upload inventory and (optional) Carfax ZIP, then click **Process**.")
    st.stop()
if not inv_file:
    st.error("Inventory file required.")
    st.stop()

# ---------- Main Logic ----------
try:
    # -- Load inventory
    raw = pd.read_csv(inv_file) if inv_file.name.lower().endswith(".csv") else pd.read_excel(inv_file)
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

    # -- Parse Carfax
    cache = load_cache()
    cf = pd.DataFrame()
    if carfax_zip:
        with st.spinner("Parsing Carfax ZIP (skips cached VINs)…"):
            cf = parse_carfax_zip_with_cache(carfax_zip, cache)

    # --- Defensive merge (prevents KeyError)
    if cf is None or cf.empty or "VIN" not in cf.columns:
        st.warning("⚠️ No valid VINs extracted from Carfax ZIP.")
        cf = pd.DataFrame(columns=["VIN"])  # ensures merge works

    data = inv.merge(cf, on="VIN", how="left")
    data["CarfaxUploaded"] = data["VIN"].isin(cf["VIN"]) if not cf.empty else False

    # -- Derived placeholders
    rng = np.random.default_rng(42)
    data["Score"] = rng.integers(70, 96, len(data))
    data["SafetyScore"] = rng.integers(70, 96, len(data))
    data["ValueCategory"] = rng.choice(["Under Market","At Market","Over Market"], len(data))
    data["SalesMood"] = np.where(data["Score"]>=85,"🟢 Confident","🟡 Balanced")

    # ---------- Filters ----------
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

    # -- AI Query
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

    # ---------- Display ----------
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
                "SalesMood","CarfaxUploaded","VIN","Year","Make","Model","Trim","Body",
                "Mileage","Price","KBBValue","ValueCategory",
                "AccidentSeverity","OwnerCount","ServiceEvents","SafetyScore","Score"
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

except Exception as e:
    st.error("⚠️ An error stopped the app:")
    st.exception(e)
    st.code(traceback.format_exc())
