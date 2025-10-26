# ==============================================================
# Used Lot Command Center — AI Edition (v7)
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
        return float(str(x).replace("$","").replace(",","").strip())
    except Exception:
        return None

def to_excel_bytes(df: pd.DataFrame) -> io.BytesIO:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        df.to_excel(xw, index=False)
    bio.seek(0)
    return bio

def latest_file_in(dirpath: str):
    files = [os.path.join(dirpath, f) for f in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, f))]
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

def parse_all_carfaxes_in_folder(cache: dict, skip_new_parse: bool) -> pd.DataFrame:
    if skip_new_parse:
        return pd.DataFrame({"VIN": list(cache.keys())}) if cache else pd.DataFrame(columns=["VIN"])

    results = {}
    pdfs = [f for f in os.listdir(CARFAX_DIR) if f.lower().endswith(".pdf")]
    for name in pdfs:
        full = os.path.join(CARFAX_DIR, name)
        m = VIN_RE.search(name.upper())
        vin = m.group(1) if m else None
        if vin:
            cached = get_cached(vin, cache)
            if cached:
                results[vin] = cached
                continue

        try:
            with open(full, "rb") as f:
                pdf_bytes = io.BytesIO(f.read())
            lines = extract_pdf_lines(pdf_bytes)
            rec = parse_carfax(lines, name)
            if rec:
                results[rec["VIN"]] = rec
                upsert_cache(rec["VIN"], rec, cache)
        except Exception:
            continue

    if not results:
        return pd.DataFrame(columns=[
            "VIN","AccidentSeverity","OwnerCount","ServiceEvents","UsageType","OdometerIssue","last_updated"
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
    use_last_inv = st.checkbox("Use last saved inventory (d
