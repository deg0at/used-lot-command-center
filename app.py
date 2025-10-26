# ==============================================================
# Used Lot Command Center — AI Edition (v7.1)
# Persistent folders, Excel engine fix, AI query, bold pricing, cache-first parsing
# ==============================================================

import io, os, re, json, zipfile, traceback
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
from pypdf import PdfReader

# Local modules
from modules.carfax_cache import load_cache, get_cached, upsert_cache
from modules.ai_query import interpret_query

# ------------------ App Config ------------------
st.set_page_config(page_title="Used Lot Command Center — AI", layout="wide")
st.title("🚗 Used Lot Command Center — AI Edition")

DATA_DIR = "data"
CARFAX_DIR = os.path.join(DATA_DIR, "carfaxes")
LISTINGS_DIR = os.path.join(DATA_DIR, "listings")
os.makedirs(CARFAX_DIR, exist_ok=True)
os.makedirs(LISTINGS_DIR, exist_ok=True)

VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")

# ------------------ Helpers ------------------
def to_num(x):
    try:
        return float(str(x).replace("$", "").replace(",", "").strip())
    except Exception:
        return None

def to_excel_bytes(df: pd.DataFrame) -> io.BytesIO:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        df.to_excel(xw, index=False)
    bio.seek(0)
    return bio

def latest_file_in(dirpath: str):
    files = [os.path.join(dirpath, f) for f in os.listdir(dirpath)
             if os.path.isfile(os.path.join(dirpath, f))]
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

def extract_pdf_lines(file_like):
    reader = PdfReader(file_like)
    lines = []
    for pg in reader.pages:
        txt = pg.extract_text() or ""
        lines.extend([ln for ln in txt.splitlines() if ln.strip()])
    return lines

def parse_carfax(lines, fname=""):
    # VIN from content or filename
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
        if "severe" in joined:
            sev = "severe"
        elif "moderate" in joined:
            sev = "moderate"
        elif "minor" in joined:
            sev = "minor"

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

def parse_all_carfaxes_in_folder(cache: dict, skip_new_parse: bool) -> pd.DataFrame:
    """
    Reads PDFs in CARFAX_DIR. Uses cache to avoid re-parsing.
    If skip_new_parse is True, just returns VIN presence from cache.
    """
    if skip_new_parse:
        return pd.DataFrame({"VIN": list(cache.keys())}) if cache else pd.DataFrame(columns=["VIN"])

    results = {}
    pdfs = [f for f in os.listdir(CARFAX_DIR) if f.lower().endswith(".pdf")]
    for name in pdfs:
        full = os.path.join(CARFAX_DIR, name)

        # Quick cache-hit via filename VIN
        m = VIN_RE.search(name.upper())
        vin = m.group(1) if m else None
        if vin:
            cached = get_cached(vin, cache)
            if cached:
                results[vin] = cached
                continue

        # Parse PDF if not cached
        try:
            with open(full, "rb") as f:
                pdf_bytes = io.BytesIO(f.read())
            lines = extract_pdf_lines(pdf_bytes)
            rec = parse_carfax(lines, name)
            if rec:
                results[rec["VIN"]] = rec
                upsert_cache(rec["VIN"], rec, cache)
        except Exception:
            # Skip bad PDFs but keep going
            continue

    if not results:
        # Keep schema stable for merge
        return pd.DataFrame(columns=[
            "VIN", "AccidentSeverity", "OwnerCount", "ServiceEvents",
            "UsageType", "OdometerIssue", "last_updated"
        ])
    return pd.DataFrame(results.values())

def save_uploaded_inventory(file) -> str:
    os.makedirs(LISTINGS_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = file.name
    out = os.path.join(LISTINGS_DIR, f"{stamp}__{base}")
    with open(out, "wb") as f:
        f.write(file.getbuffer())
    return out

def save_uploaded_carfax_zip(file) -> int:
    os.makedirs(CARFAX_DIR, exist_ok=True)
    added = 0
    with zipfile.ZipFile(file) as z:
        for name in z.namelist():
            if name.lower().endswith(".pdf"):
                raw = z.read(name)
                base = os.path.basename(name)
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                out = os.path.join(CARFAX_DIR, f"{stamp}__{base}")
                with open(out, "wb") as f:
                    f.write(raw)
                added += 1
    return added

def save_uploaded_carfax_pdfs(files) -> int:
    os.makedirs(CARFAX_DIR, exist_ok=True)
    added = 0
    for file in files:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = os.path.join(CARFAX_DIR, f"{stamp}__{file.name}")
        with open(out, "wb") as f:
            f.write(file.getbuffer())
        added += 1
    return added

# ------------------ Session State ------------------
ss = st.session_state
ss.setdefault("app_ready", False)
ss.setdefault("inv_path", None)
ss.setdefault("data_df", None)

# ------------------ Sidebar ------------------
with st.sidebar:
    st.header("Inventory")
    use_last_inv = st.checkbox("Use last saved inventory (data/listings)", value=True)
    inv_upload = st.file_uploader("Upload new inventory (.csv/.xlsx)", type=["csv", "xlsx"])

    st.header("Carfax PDFs")
    use_cache_only = st.checkbox("Use existing Carfax cache (skip parsing)", value=False)
    cf_zip_upload = st.file_uploader("Upload Carfax ZIP (optional)", type=["zip"])
    cf_pdf_uploads = st.file_uploader("Upload individual Carfax PDF(s)", type=["pdf"], accept_multiple_files=True)
    st.caption("All Carfax PDFs are saved to data/carfaxes/ and reused automatically.")

    st.divider()
    view_mode = st.radio("View Mode", ["🧱 Card View", "📊 Table View"], horizontal=True)

    run_btn = st.button("Process / Refresh")
    reset_btn = st.button("Reset session (keep files & cache)")

if reset_btn:
    ss["app_ready"] = False
    ss["inv_path"] = None
    ss["data_df"] = None
    st.success("Session reset. Saved files & cache are intact.")
    st.stop()

# ------------------ Pre-process saving ------------------
if inv_upload is not None:
    ss["inv_path"] = save_uploaded_inventory(inv_upload)
    st.success(f"Inventory saved to {ss['inv_path']}")

added = 0
if cf_zip_upload is not None:
    added += save_uploaded_carfax_zip(cf_zip_upload)
if cf_pdf_uploads:
    added += save_uploaded_carfax_pdfs(cf_pdf_uploads)
if added:
    st.success(f"Saved {added} Carfax PDF(s) into {CARFAX_DIR}")

# ------------------ Early guard ------------------
if not ss.get("app_ready") and not run_btn:
    st.info("Upload or reuse inventory/Carfax files in the sidebar, then click **Process / Refresh**.")
    st.stop()

# ------------------ Main processing ------------------
try:
    inv_path = ss.get("inv_path")
    if not use_last_inv and inv_upload is None and inv_path is None:
        st.error("Please upload an inventory file or enable 'Use last saved inventory'.")
        st.stop()

    if use_last_inv:
        if inv_path is None or not os.path.exists(inv_path):
            auto = latest_file_in(LISTINGS_DIR)
            if auto is None:
                st.error("No saved inventory found in data/listings/. Please upload once.")
                st.stop()
            inv_path = auto
            ss["inv_path"] = inv_path

    # ---------- Load inventory from disk safely ----------
    try:
        if inv_path.lower().endswith(".csv"):
            raw = pd.read_csv(inv_path)
            st.success(f"✅ Loaded CSV file: {os.path.basename(inv_path)}")
        elif inv_path.lower().endswith(".xls"):
            raw = pd.read_excel(inv_path, engine="xlrd")
            st.success(f"✅ Loaded legacy Excel (.xls): {os.path.basename(inv_path)}")
        else:
            raw = pd.read_excel(inv_path, engine="openpyxl")
            st.success(f"✅ Loaded modern Excel (.xlsx): {os.path.basename(inv_path)}")
    except ValueError as e:
        st.error(f"⚠️ Could not read Excel file automatically: {e}")
        try:
            raw = pd.read_excel(inv_path, engine="openpyxl")
        except Exception as e2:
            st.error(f"❌ Failed to read Excel file: {e2}")
            st.stop()

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
        "Price": (raw.get("Website Basis") if "Website Basis" in raw.columns else raw.get("Price")),
        "KBBValue": (raw.get("MSRP / KBB") if "MSRP / KBB" in raw.columns else raw.get("KBB")),
        "Mileage": raw.get("Mileage"),
        "Days": (raw.get("Days In Inv") if "Days In Inv" in raw.columns else raw.get("DaysInInventory")),
        "CPO": raw.get("CPO"),
        "Warranty": (raw.get("Warr.") if "Warr." in raw.columns else raw.get("Warranty")),
        "Status": raw.get("Status")
    })

    # Numeric convenience columns for AI filtering
    inv["PriceNum"] = inv["Price"].apply(to_num)
    inv["MileageNum"] = inv["Mileage"].apply(to_num)

    # Carfax cache and folder parsing
    cache = load_cache()
    cf = parse_all_carfaxes_in_folder(cache, skip_new_parse=use_cache_only)

    # VIN-safe merge
    if cf is None or cf.empty or "VIN" not in cf.columns:
        cf = pd.DataFrame(columns=["VIN"])
    data = inv.merge(cf, on="VIN", how="left")
    data["CarfaxUploaded"] = data["VIN"].isin(cf["VIN"]) if not cf.empty else False

    # Placeholder scores (replace later with your real scoring pipeline)
    rng = np.random.default_rng(7)
    data["Score"] = rng.integers(70, 96, len(data))
    data["SafetyScore"] = rng.integers(70, 96, len(data))
    data["ValueCategory"] = rng.choice(["Under Market", "At Market", "Over Market"], len(data))
    data["SalesMood"] = np.where(data["Score"] >= 85, "🟢 Confident", "🟡 Balanced")

    st.success(f"Inventory in use: {os.path.basename(inv_path)}")
    ss["data_df"] = data
    ss["app_ready"] = True

except Exception as e:
    st.error("⚠️ Error during processing:")
    st.exception(e)
    st.code(traceback.format_exc())
    st.stop()

# ------------------ UI ------------------
data = ss["data_df"].copy()

st.divider()
st.subheader("🔍 Search & Filters")

query_text = st.text_input("Ask naturally (e.g., 'SUV under 25k with AWD and low miles'):")
use_ai = st.checkbox("Use AI Query", value=True)

fc1, fc2, fc3, fc4 = st.columns(4)
sc_min, sc_max = fc1.slider("Smart Score", 0, 100, (70, 100))
safety_cut = fc2.slider("Min Safety", 0, 100, 70)
make_sel = fc3.multiselect("Make", sorted(list(data["Make"].dropna().unique())))
val_sel = fc4.multiselect("Value Category", ["Under Market", "At Market", "Over Market"])

mask = pd.Series(True, index=data.index)
mask &= data["Score"].between(sc_min, sc_max)
mask &= data["SafetyScore"] >= safety_cut
if make_sel:
    mask &= data["Make"].isin(make_sel)
if val_sel:
    mask &= data["ValueCategory"].isin(val_sel)

# AI → structured filters (uses numeric PriceNum/MileageNum)
if use_ai and query_text.strip():
    with st.spinner("Analyzing your query..."):
        filters = interpret_query(query_text)
    st.caption(f"🧠 Interpreted filters: {filters}")

    if "Body" in filters and "Body" in data.columns:
        mask &= data["Body"].astype(str).str.contains(filters["Body"], case=False, na=False)
    if "DriveTrain" in filters and "Drive Train" in data.columns:
        mask &= data["Drive Train"].astype(str).str.contains(filters["DriveTrain"], case=False, na=False)
    if "Make" in filters and "Make" in data.columns:
        mask &= data["Make"].astype(str).str.contains(filters["Make"], case=False, na=False)
    if "Model" in filters and "Model" in data.columns:
        mask &= data["Model"].astype(str).str.contains(filters["Model"], case=False, na=False)
    if "PriceMax" in filters and "PriceNum" in data.columns:
        mask &= data["PriceNum"] <= filters["PriceMax"]
    if "PriceMin" in filters and "PriceNum" in data.columns:
        mask &= data["PriceNum"] >= filters["PriceMin"]
    if "MileageMax" in filters and "MileageNum" in data.columns:
        mask &= data["MileageNum"] <= filters["MileageMax"]
    if "MileageMin" in filters and "MileageNum" in data.columns:
        mask &= data["MileageNum"] >= filters["MileageMin"]

mask = mask.reindex(data.index, fill_value=False)
filtered = data.loc[mask].copy()

# KPIs
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
        for _, r in filtered.head(100).iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([2, 1, 2])
                with c1:
                    yr = int(r['Year']) if pd.notna(r['Year']) else ''
                    title = f"{yr} {r.get('Make','')} {r.get('Model','')} {r.get('Trim') or ''}".strip()
                    st.markdown(f"**{title}**")
                    # Bold price on its own line under title
                    st.markdown(f"<div style='font-size:1.1rem'><b>${r.get('Price')}</b></div>", unsafe_allow_html=True)
                    # Body & mileage as caption
                    st.caption(f"{r.get('Body','') or ''} • {r.get('Mileage')} mi")
                    # Carfax indicator
                    if r.get("CarfaxUploaded"):
                        st.success("Carfax ✅")
                    else:
                        st.warning("No Carfax")
                with c2:
                    st.metric("Smart", f"{r['Score']:.0f}")
                    st.metric("Safety", f"{r['SafetyScore']:.0f}")
                with c3:
                    st.caption(r.get("TalkTrack") or "")
                st.divider()
    else:
        cols = [
            "CarfaxUploaded", "VIN", "Year", "Make", "Model", "Trim", "Body",
            "Mileage", "Price", "KBBValue", "ValueCategory",
            "AccidentSeverity", "OwnerCount", "ServiceEvents",
            "SafetyScore", "Score", "Status"
        ]
        cols = [c for c in cols if c in filtered.columns]
        st.dataframe(filtered[cols], use_container_width=True, hide_index=True)

st.divider()
cA, cB = st.columns(2)
cA.download_button(
    "Download CSV",
    data=filtered.drop(columns=["PriceNum", "MileageNum"], errors="ignore").to_csv(index=False).encode("utf-8"),
    file_name="filtered_results.csv",
    mime="text/csv"
)
cB.download_button(
    "Download Excel",
    data=to_excel_bytes(filtered.drop(columns=["PriceNum", "MileageNum"], errors="ignore")),
    file_name="filtered_results.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
