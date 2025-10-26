# ==============================================================
# Used Lot Command Center — AI Edition (v4)
# Includes: Carfax Cache + AI Natural Query + View Toggle
# ==============================================================

import io, re, zipfile, json, os, requests
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

# ---------- Helpers ----------
VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")

def to_num(x):
    try: return float(str(x).replace("$","").replace(",","").strip())
    except: return None

def value_bucket(r):
    if r is None or (isinstance(r,float) and np.isnan(r)): return ""
    if r <= 0.90: return "Under Market"
    if r >= 1.10: return "Over Market"
    return "At Market"

# ---------- Carfax Parsing ----------
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
        if m: vin = m.group(1); break
    if not vin:
        m = VIN_RE.search((fname or "").upper())
        if m: vin = m.group(1)
    if not vin: return None

    joined = "\n".join(lines).lower()
    sev = "none"
    if "accident" in joined or "damage" in joined:
        if "severe" in joined: sev = "severe"
        elif "moderate" in joined: sev = "moderate"
        elif "minor" in joined: sev = "minor"

    owners = 1
    m = re.search(r"(\d+)\s+owner", joined)
    if m: owners = int(m.group(1))

    services = 0
    m = re.search(r"service\s+history\s+records?:?\s*(\d+)", joined)
    if m: services = int(m.group(1))

    usage = ""
    m = re.search(r"(personal|fleet|rental|commercial|taxi|lease)\s+use", joined)
    if m: usage = m.group(1)

    odo_issue = "Yes" if "odometer" in joined and ("mismatch" in joined or "tamper" in joined) else "No"
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
            if vin and (cached := get_cached(vin, cache)):
