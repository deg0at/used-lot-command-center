# -------------------------------------------------------------
# Used Lot Command Center — AI Edition (Fixed + Persistent Cache)
# -------------------------------------------------------------
import io, re, zipfile, json, os, requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime
from pypdf import PdfReader

# ---------- OpenAI (optional) ----------
try:
    from openai import OpenAI
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", None)
    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    OPENAI_API_KEY = None
    openai_client = None

# ---------- Local cache helpers ----------
from modules.carfax_cache import load_cache, get_cached, upsert_cache

# ---------- Regex & scoring utils ----------
VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
LEGEND_RE = re.compile(r"damage\s+severity\s+scale.*minor.*moderate.*severe", re.I)
ACC_TRIG = re.compile(r"(damage|accident)\s+reported", re.I)
SEV_WORD = re.compile(r"\b(minor|moderate|severe)\b", re.I)
CLEAN_PH = re.compile(r"no\s+accidents?\s+or\s+damage\s+reported", re.I)
OWNERS_RE = re.compile(r"(\d+)\s+previous owners?|\bCARFAX\s+(\d+)-Owner", re.I)
SERVICE_RE = re.compile(r"service\s+history\s+records?:?\s*(\d+)", re.I)
ODOMETER_BAD = re.compile(r"(odometer check.*(mismatch|inconsistent|roll|tamper))", re.I)
USAGE_RE = re.compile(r"(personal|fleet|rental|commercial|taxi|lease)\s+use", re.I)

def to_num(x):
    try: return float(str(x).replace("$","").replace(",","").strip())
    except: return None

def value_bucket(r):
    if r is None or (isinstance(r,float) and np.isnan(r)): return ""
    if r <= 0.90: return "Under Market"
    if r >= 1.10: return "Over Market"
    return "At Market"

def mileage_score(y, m):
    y = int(y) if pd.notna(y) else datetime.now().year
    age = max(1, datetime.now().year - y)
    exp = 13000 * age
    m = to_num(m) or 0
    ratio = (m/exp) if exp>0 else 1
    if ratio<=0.6: return 100
    if ratio>=1.8: return 0
    return 100 * (1 - (ratio-0.6)/(1.8-0.6))

def days_score(d):
    d = to_num(d)
    if d is None: return 50
    if d<=7: return 100
    if d>=60: return 0
    return 100 * (1 - (d-7)/(60-7))

def cpo_warranty_score(cpo, war):
    cpo = str(cpo or "").strip().lower() in ("yes","y","true","1")
    war = str(war or "").strip().lower() not in ("","no","none","expired")
    return 100 if cpo else (60 if war else 0)

def value_score(p, k):
    p, k = to_num(p), to_num(k)
    if not p or not k or k<=0: return 50, None
    r = p/k
    if r<=0.90: return 100, r
    if r>=1.10: return max(0, 60 - (r-1.10)*300), r
    return (100 - ((r-0.9)/0.2)*40), r

# ---------- Carfax parsing ----------
def extract_pdf_lines(file_like):
    reader = PdfReader(file_like)
    lines = []
    for pg in reader.pages:
        t = pg.extract_text() or ""
        lines.extend([ln for ln in t.splitlines() if ln.strip()])
    return [ln for ln in lines if not LEGEND_RE.search(ln)]

def parse_carfax(lines, fname=""):
    vin = None
    for ln in lines:
        m = VIN_RE.search(ln)
        if m: vin = m.group(1); break
    if not vin:
        m = VIN_RE.search((fname or "").upper())
        if m: vin = m.group(1)
    if not vin: return None

    joined = "\n".join(lines)
    severity = "none"
    if not CLEAN_PH.search(joined):
        for i, ln in enumerate(lines):
            if ACC_TRIG.search(ln):
                win = " ".join(lines[i:i+4])
                m = SEV_WORD.search(win)
                severity = m.group(1).lower() if m else "minor"
                break

    owners, services, usage = None, None, None
    for ln in lines:
        mo = OWNERS_RE.search(ln)
        if mo:
            try: owners = int(mo.group(1) or mo.group(2)); break
            except: pass
    for ln in lines:
        ms = SERVICE_RE.search(ln)
        if ms:
            try: services = int(ms.group(1)); break
            except: pass
    for ln in lines:
        mu = USAGE_RE.search(ln)
        if mu:
            usage = mu.group(0).lower().replace(" use",""); break

    odo_issue = "Yes" if ODOMETER_BAD.search(joined) else "No"
    return {
        "VIN": vin,
        "AccidentSeverity": severity,   # none/minor/moderate/severe
        "OwnerCount": owners or 1,
        "ServiceEvents": services or 5,
        "UsageType": usage or "",
        "OdometerIssue": odo_issue
    }

def parse_carfax_zip_with_cache(zip_file, cache: dict) -> pd.DataFrame:
    """
    Only parse PDFs whose VIN is NOT already in cache.
    Returns a DataFrame for ALL VINs found (cached + newly parsed).
    """
    results = {}

    with zipfile.ZipFile(zip_file) as z:
        # First pass: discover VINs from filenames to hit cache quickly
        pdf_names = [n for n in z.namelist() if n.lower().endswith(".pdf")]
        for name in pdf_names:
            m = VIN_RE.search(name.upper())
            if m:
                vin = m.group(1)
                cached = get_cached(vin, cache)
                if cached:
                    results[vin] = cached

        # Second pass: parse only missing VINs
        for name in pdf_names:
            m = VIN_RE.search(name.upper())
            if not m:  # fallback: open and try reading a VIN from inside
                with z.open(name) as f:
                    lines = extract_pdf_lines(f)
                rec = parse_carfax(lines, name)
                if rec:
                    vin = rec["VIN"]
                    if vin not in results:
                        results[vin] = rec
                        upsert_cache(vin, rec, cache)
                continue

            vin = m.group(1)
            if vin in results:
                continue  # already cached
            with z.open(name) as f:
                lines = extract_pdf_lines(f)
            rec = parse_carfax(lines, name)
            if rec:
                results[vin] = rec
                upsert_cache(vin, rec, cache)

    if not results:
        return pd.DataFrame(columns=["VIN","AccidentSeverity","OwnerCount","ServiceEvents","UsageType","OdometerIssue","last_updated"])
    return pd.DataFrame(results.values())

# ---------- NHTSA ----------
def nhtsa_recalls(vin):
    try:
        r = requests.get(f"https://api.nhtsa.gov/recalls/recallsByVin?vin={vin}", timeout=15)
        js = r.json()
        return len(js.get("results", []) or js.get("Results", []) or [])
    except:
        return 0

# ---------- IIHS (official heuristic → AI fallback) ----------
@st.cache_data(ttl=60*60*24, show_spinner=False)
def iihs_official(make, model, year):
    try:
        q = f"{year} {make} {model} IIHS rating"
        r = requests.get("https://duckduckgo.com/html/", params={"q": q}, timeout=12,
                         headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code != 200: return {}
        h = r.text.lower()
        s = None
        if "top safety pick+" in h: s = 95
        elif "top safety pick" in h: s = 90
        elif "good" in h and "acceptable" in h: s = 80
        elif "acceptable" in h: s = 70
        elif "marginal" in h: s = 55
        elif "poor" in h: s = 40
        return {"source":"IIHS Official","score":float(s)} if s else {}
    except:
        return {}

@st.cache_data(ttl=60*60*24, show_spinner=False)
def iihs_ai(make, model, year, nhtsa_score):
    if not openai_client: return {}
    prompt = f"""
Estimate a 0–100 IIHS-style safety score for a {year} {make} {model}.
Use known trends and the NHTSA-derived score {int(nhtsa_score)}.
Return JSON {{"score": int, "confidence": "high|medium|low"}} only.
"""
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":"Output JSON only."},
                      {"role":"user","content": prompt}],
            temperature=0.2, max_tokens=100
        )
        t = resp.choices[0].message.content.strip()
        d = json.loads(t) if t.startswith("{") else {}
        if "score" in d: d["source"] = "IIHS AI"
        return d
    except:
        return {}

def safety_blend(make, model, year, base):
    try:
        yr = int(year) if pd.notna(year) else None
        mk = str(make or "").strip()
        mo = str(model or "").strip()
        if not (yr and mk and mo):
            return base, "NHTSA Only", "—"

        off = iihs_official(mk, mo, yr)
        if off.get("score") is not None:
            return max(base, float(off["score"])), off["source"], "high"

        ai = iihs_ai(mk, mo, yr, float(base or 0))
        if ai.get("score") is not None:
            return max(base, float(ai["score"])), ai.get("source","IIHS AI"), ai.get("confidence","medium")

        return base, "NHTSA Only", "—"
    except:
        return base, "NHTSA Only", "—"

# ---------- Carfax quality + Smart Score ----------
def carfax_quality(r):
    sev = {"none":30,"minor":15,"moderate":-15,"severe":-40}.get(str(r.get("AccidentSeverity") or "none").lower(),0)
    owners = r.get("OwnerCount") or 1
    own_pts = 25 if owners==1 else (10 if owners==2 else -5)
    svc = min(r.get("ServiceEvents") or 0, 12)/12*10
    rec = -10*(r.get("RecallCount") or 0)
    use = -15 if str(r.get("UsageType") or "").lower() in ("fleet","rental","commercial","taxi") else 0
    odo = -25 if str(r.get("OdometerIssue") or "").lower()=="yes" else 0
    return float(np.clip(70 + sev + own_pts + svc + rec + use + odo, 0, 100))

def total_smart_row(r):
    v_score, mr = value_score(r.get("Price"), r.get("KBBValue"))
    total = (
        0.50 * carfax_quality(r) +
        0.20 * v_score +
        0.15 * mileage_score(r.get("Year"), r.get("Mileage")) +
        0.10 * days_score(r.get("Days")) +
        0.05 * cpo_warranty_score(r.get("CPO"), r.get("Warranty"))
    )
    return round(total,1), v_score, mr

# ---------- AI lead match & talk ----------
def ai_lead_match(row, pref):
    rb = 0
    price = to_num(row.get("Price"))
    if pref.get("budget_max") and price and price <= pref["budget_max"]: rb += 30
    if pref.get("body") and str(row.get("Body") or "").lower() == pref["body"].lower(): rb += 20
    if pref.get("make") and str(row.get("Make") or "").lower() == pref["make"].lower(): rb += 15
    if pref.get("safety_min") and (row.get("SafetyScore") or 0) >= pref["safety_min"]: rb += 20
    if pref.get("owners_max") and (row.get("OwnerCount") or 99) <= pref["owners_max"]: rb += 15

    if not openai_client:
        return rb, "(rule-based)"

    try:
        vehicle = {
            "price": price,
            "year": row.get("Year"), "make": row.get("Make"),
            "model": row.get("Model"), "trim": row.get("Trim"),
            "body": row.get("Body"),
            "safety": row.get("SafetyScore"),
            "owners": row.get("OwnerCount"),
            "carfax_sev": row.get("AccidentSeverity"),
            "value_cat": value_bucket(row.get("MarketRatio") if row.get("MarketRatio") is not None else np.nan),
            "score": row.get("Score")
        }
        prompt = f"""
Lead: {json.dumps(pref)}
Vehicle: {json.dumps(vehicle)}
Score the fit 0–100 and give a short reason.
Return JSON {{"score": int, "why": "string"}} only.
"""
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":"Output JSON only."},
                      {"role":"user","content": prompt}],
            temperature=0.2, max_tokens=120
        )
        t = resp.choices[0].message.content.strip()
        d = json.loads(t) if t.startswith("{") else {}
        if "score" in d: return int(d["score"]), d.get("why","")
        return rb, "(fallback)"
    except:
        return rb, "(fallback)"

def ai_talk(row):
    if not openai_client:
        parts = []
        ymk = f"{int(row.get('Year')) if pd.notna(row.get('Year')) else ''} {row.get('Make','')} {row.get('Model','')}".strip()
        parts.append(ymk)
        if (row.get("OwnerCount") or 0)==1: parts.append("1-owner")
        sev = str(row.get("AccidentSeverity") or "none")
        if sev=="none": parts.append("clean Carfax")
        elif sev=="minor": parts.append("minor incident disclosed")
        if (row.get("ServiceEvents") or 0) >= 6: parts.append("strong service history")
        if (row.get("SafetyScore") or 0) >= 85: parts.append("top safety")
        if value_bucket(row.get("MarketRatio"))=="Under Market": parts.append("under book")
        return " • ".join([p for p in parts if p])[:180]

    v = {k: row.get(k) for k in ["Year","Make","Model","Trim","OwnerCount","AccidentSeverity","ServiceEvents","ValueCategory","RecallCount","SafetyScore"]}
    prompt = f"Write one 20-25 word professional sentence to present this car: {json.dumps(v)}. Calm, transparent, no emojis."
    try:
        r = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":"Short, professional, single sentence."},
                      {"role":"user","content": prompt}],
            temperature=0.4, max_tokens=60
        )
        return r.choices[0].message.content.strip()
    except:
        return "Great choice with solid history and value."

# ---------- Safe table helper (prevents KeyErrors) ----------
def show_table(df, wanted_cols, title=None, height=420):
    if title: st.subheader(title)
    if df is None or df.empty:
        st.info("No rows to display.")
        return
    available = [c for c in wanted_cols if c in df.columns]
    missing = [c for c in wanted_cols if c not in df.columns]
    if missing:
        st.caption("Note: missing columns → " + ", ".join(missing))
    st.dataframe(df[available], use_container_width=True, height=height)

# ===================== UI ===================== #
st.set_page_config(page_title="Used Lot Command Center — AI", layout="wide")
st.title("🚗 Used Lot Command Center — AI Edition")

with st.sidebar:
    st.header("Upload Files")
    inv_file = st.file_uploader("Inventory (.csv or .xlsx)", type=["csv","xlsx"])
    carfax_zip = st.file_uploader("Carfax ZIP (PDFs)", type=["zip"])
    st.caption("Tip: PDF filenames should include the 17-char VIN.")
    st.divider()
    st.header("Lead Preferences")
    budget = st.number_input("Max Budget", 0, 200000, 0, 500)
    body_pref = st.text_input("Body Type (e.g., SUV)")
    make_pref = st.text_input("Preferred Make (e.g., Honda)")
    safety_min = st.number_input("Min Safety Score", 0, 100, 0, 5)
    owners_max = st.number_input("Max Owners", 1, 10, 3)
    run_btn = st.button("Process")

if not run_btn:
    st.info("Upload inventory + (optional) Carfax ZIP and click **Process**.")
    st.stop()
if not inv_file:
    st.error("Inventory required.")
    st.stop()

# Load inventory
try:
    raw = pd.read_csv(inv_file) if inv_file.name.lower().endswith(".csv") else pd.read_excel(inv_file)
except Exception as e:
    st.error(f"Could not read inventory: {e}")
    st.stop()

raw.columns = [c.strip() for c in raw.columns]
vin_candidates = [c for c in raw.columns if "vin" in c.lower()]
if not vin_candidates:
    st.error("No VIN column found in inventory.")
    st.stop()
vin_col = vin_candidates[0]

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

# Parse Carfax with persistent cache
cache = load_cache()
cf = pd.DataFrame()
if carfax_zip is not None:
    with st.spinner("Parsing Carfax (skips cached VINs)…"):
        cf = parse_carfax_zip_with_cache(carfax_zip, cache)

data = inv.merge(cf, on="VIN", how="left")

# NHTSA recalls → base safety
with st.spinner("Fetching NHTSA recall data…"):
    data["RecallCount"] = [nhtsa_recalls(v) for v in data["VIN"]]
data["SafetyScore"] = [max(0, 85 - (rc*7)) for rc in data["RecallCount"]]

# IIHS blend
new_safety, srcs, confs = [], [], []
with st.spinner("Blending IIHS safety (official/AI)…"):
    for _, r in data.iterrows():
        s, src, conf = safety_blend(r.get("Make"), r.get("Model"), r.get("Year"), r.get("SafetyScore"))
        new_safety.append(s); srcs.append(src); confs.append(conf)
data["SafetyScore"] = new_safety
data["SafetySource"] = srcs
data["SafetyConfidence"] = confs

# Smart Score & mood
scores, val_scores, ratios, cx_scores = [], [], [], []
for _, r in data.iterrows():
    total, vs, mr = total_smart_row(r)
    scores.append(total); val_scores.append(vs); ratios.append(mr); cx_scores.append(carfax_quality(r))
data["Score"] = scores
data["SmartValue"] = [round(x,1) for x in val_scores]
data["MarketRatio"] = [round(x,3) if x is not None else np.nan for x in ratios]
data["ValueCategory"] = [value_bucket(x) for x in data["MarketRatio"]]
data["CarfaxQuality"] = [round(x,1) for x in cx_scores]

def mood(r):
    if (r["Score"]>=90) and (str(r.get("AccidentSeverity") or "none")=="none") and (r["RecallCount"]==0):
        return "🟢 Confident"
    if r["Score"]>=75: return "🟡 Balanced"
    return "🔴 Review"
data["SalesMood"] = data.apply(mood, axis=1)

# Lead match + talk track
lead_pref = {
    "budget_max": budget or None,
    "body": body_pref or None,
    "make": make_pref or None,
    "safety_min": safety_min or None,
    "owners_max": owners_max or None
}
lm_scores, lm_whys, tt_lines = [], [], []
with st.spinner("Generating lead matches & talk tracks…"):
    for _, r in data.iterrows():
        s, why = ai_lead_match(r, lead_pref)
        lm_scores.append(int(s)); lm_whys.append(why)
        tt_lines.append(ai_talk(r))
data["LeadMatch"] = lm_scores
data["LeadWhy"] = lm_whys
data["TalkTrack"] = tt_lines

# ---------- KPIs ----------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Vehicles", f"{len(data)}")
col2.metric("Avg Smart", f"{np.mean(data['Score']):.1f}")
col3.metric("Avg Safety", f"{np.mean(data['SafetyScore']):.0f}")
col4.metric("Clean Carfax %", f"{(data['AccidentSeverity'].fillna('none')=='none').mean()*100:.0f}%")

st.divider()

# ---------- Filters ----------
st.subheader("Filters")
fc1, fc2, fc3, fc4 = st.columns(4)
sc_min, sc_max = fc1.slider("Smart Score", 0, 100, (70, 100))
safety_cut = fc2.slider("Min Safety", 0, 100, 70)
make_sel = fc3.multiselect("Make", sorted(list(data["Make"].dropna().unique())))
val_sel = fc4.multiselect("Value Category", ["Under Market","At Market","Over Market"])

mask = (data["Score"].between(sc_min, sc_max)) & (data["SafetyScore"]>=safety_cut)
if make_sel: mask &= data["Make"].isin(make_sel)
if val_sel: mask &= data["ValueCategory"].isin(val_sel)
filtered = data[mask].copy()

# ---------- Tables (safe) ----------
top = filtered.sort_values(["Score","SmartValue"], ascending=False).head(15)
top_cols = [
    "SalesMood","LeadMatch","LeadWhy","TalkTrack",
    "StockNumber","VIN","Year","Make","Model","Trim","Body","Mileage","Price","KBBValue",
    "MarketRatio","ValueCategory","AccidentSeverity","OwnerCount","ServiceEvents","UsageType",
    "OdometerIssue","RecallCount","SafetyScore","SafetySource","SafetyConfidence","CarfaxQuality","Score"
]
show_table(top, top_cols, title="🔥 Top Smart Deals", height=420)

inv_cols = [
    "SalesMood","LeadMatch","LeadWhy","TalkTrack",
    "StockNumber","VIN","Year","Make","Model","Trim","Body","Color","Mileage","Price","KBBValue",
    "MarketRatio","ValueCategory","Days","AccidentSeverity","OwnerCount","ServiceEvents","UsageType",
    "OdometerIssue","RecallCount","SafetyScore","SafetySource","SafetyConfidence","CarfaxQuality","Score","Status","CPO","Warranty"
]
show_table(filtered, inv_cols, title="📊 Full Inventory (filtered)")

# ---------- Downloads ----------
st.subheader("⬇️ Export")
stamp = datetime.now().strftime("%Y%m%d_%H%M")

def to_excel_bytes(df):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xw:
        df.to_excel(xw, index=False)
    bio.seek(0); return bio

st.download_button(
    "Download filtered CSV",
    data=filtered.to_csv(index=False).encode("utf-8"),
    file_name=f"used_lot_filtered_{stamp}.csv",
    mime="text/csv"
)
st.download_button(
    "Download filtered Excel",
    data=to_excel_bytes(filtered),
    file_name=f"used_lot_filtered_{stamp}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.success("Ready. Upload fresh files any time — cached VINs skip re-parse automatically.")
