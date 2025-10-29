# ==============================================================
# Used Lot Command Center — Hybrid AI Edition (v11)
# - Persistent storage (listings, carfaxes, caches)
# - Auto-parse only new Carfax PDFs; cached forever
# - Per-VIN AI story buttons (cached)
# - GPT-powered AI Search (fallback parser if no key)
# - Safe/NaN-proof scoring + summaries + owner ratings
# ==============================================================

import io, os, re, json, zipfile, traceback
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
from pypdf import PdfReader

# ------------------ App Config ------------------
st.set_page_config(page_title="Used Lot Command Center — Hybrid AI", layout="wide")
st.title("🚗 Used Lot Command Center — Hybrid AI")

DATA_DIR = "data"
CARFAX_DIR = os.path.join(DATA_DIR, "carfaxes")
LISTINGS_DIR = os.path.join(DATA_DIR, "listings")
CARFAX_CACHE_PATH = os.path.join(DATA_DIR, "carfax_cache.json")
STORY_CACHE_PATH  = os.path.join(DATA_DIR, "story_cache.json")

# Ensure folders/files exist
os.makedirs(CARFAX_DIR, exist_ok=True)
os.makedirs(LISTINGS_DIR, exist_ok=True)
for fpath in [CARFAX_CACHE_PATH, STORY_CACHE_PATH]:
    if not os.path.exists(fpath):
        with open(fpath, "w") as f: f.write("{}")

VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")

# ------------------ OpenAI (optional) ------------------
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
AI_AVAILABLE = bool(OPENAI_API_KEY)
try:
    import openai
    if AI_AVAILABLE:
        openai.api_key = OPENAI_API_KEY
except Exception:
    openai = None
    AI_AVAILABLE = False

# ------------------ Utilities ------------------
def load_json(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_json(path: str, data: dict):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

def latest_file_in(dirpath: str):
    files = [os.path.join(dirpath, f) for f in os.listdir(dirpath)
             if os.path.isfile(os.path.join(dirpath, f))]
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

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

def find_carfax_file(vin: str):
    """Return absolute path to first PDF whose filename contains the VIN."""
    if not vin: return None
    for f in sorted(os.listdir(CARFAX_DIR)):
        if vin in f:
            return os.path.join(CARFAX_DIR, f)
    return None

# ------------------ PDF Parsing ------------------
def extract_pdf_lines(file_like_binary) -> list:
    # file_like_binary must be binary (BytesIO or rb file)
    reader = PdfReader(file_like_binary)
    lines = []
    for pg in reader.pages:
        txt = pg.extract_text() or ""
        lines.extend([ln for ln in txt.splitlines() if ln.strip()])
    return lines

def parse_carfax(lines, fname=""):
    # VIN discovery
    vin = None
    for ln in lines:
        m = VIN_RE.search(ln)
        if m:
            vin = m.group(1)
            break
    if not vin:
        m = VIN_RE.search((fname or "").upper())
        if m: vin = m.group(1)
    if not vin:
        return None

    # Lowercased joined text + keep a readable snippet
    joined = "\n".join(lines).lower()
    snippet = "\n".join(lines[:500])  # keep manageable context for AI story

    # Accident severity
    sev = "none"
    if "accident" in joined or "damage" in joined:
        if "severe" in joined: sev = "severe"
        elif "moderate" in joined: sev = "moderate"
        elif "minor" in joined: sev = "minor"

    # Owner count (simple extraction)
    owners = 1
    m = re.search(r"(\d+)\s+owner", joined)
    if m:
        try: owners = int(m.group(1))
        except: pass

    # Service events
    services = 0
    m = re.search(r"service\s+history\s+records?:?\s*(\d+)", joined)
    if m:
        try: services = int(m.group(1))
        except: pass

    # Usage type
    usage = ""
    m = re.search(r"(personal|fleet|rental|commercial|taxi|lease)\s+use", joined)
    if m:
        usage = m.group(1)

    # Odometer issue
    odo_issue = "Yes" if ("odometer" in joined and ("mismatch" in joined or "tamper" in joined or "inconsistent" in joined)) else "No"

    # Major parts replaced
    major_parts = []
    keywords = {
        "engine": ["engine replaced", "engine rebuilt", "motor replaced"],
        "transmission": ["transmission replaced", "gearbox replaced", "clutch replaced"],
        "brakes": ["brake rotors replaced", "brakes replaced"],
        "suspension": ["shock", "strut", "suspension replaced"],
        "battery": ["battery replaced"],
        "cooling": ["radiator replaced", "cooling system serviced"]
    }
    for part, phrases in keywords.items():
        if any(p in joined for p in phrases):
            major_parts.append(part.title())

    # Per-owner rough ratings
    owners_data = re.split(r"(?:owner\s+\d+)", joined)
    owner_ratings = []
    for idx, section in enumerate(owners_data[1:], start=1):
        svc = len(re.findall(r"\bservice\b", section))
        acc = len(re.findall(r"\b(accident|damage)\b", section))
        maj = any(k in section for k in ["engine", "transmission", "gearbox", "suspension"])
        rating = 80
        rating -= acc * 15
        rating += svc * 2
        if maj: rating -= 10
        rating = max(0, min(100, rating))
        owner_ratings.append({"Owner": idx, "Score": rating})

    return {
        "VIN": vin,
        "AccidentSeverity": sev,
        "OwnerCount": owners,
        "ServiceEvents": services,
        "UsageType": usage,
        "OdometerIssue": odo_issue,
        "MajorParts": ", ".join(major_parts) if major_parts else "None",
        "OwnerRatings": owner_ratings,
        "CarfaxText": snippet
    }

def parse_new_carfax_pdfs():
    """
    Parse only Carfax PDFs that produce a VIN not already in cache.
    If a PDF already yields a cached VIN, skip.
    """
    existing_vins = set(carfax_cache.keys())
    pdfs = [f for f in os.listdir(CARFAX_DIR) if f.lower().endswith(".pdf")]
    parsed_new = 0
    for name in sorted(pdfs):
        full = os.path.join(CARFAX_DIR, name)

        # If VIN in filename and already cached, skip
        m = VIN_RE.search(name.upper())
        if m and (m.group(1) in existing_vins):
            continue

        # Parse PDF
        try:
            with open(full, "rb") as f:
                data = f.read()
            lines = extract_pdf_lines(io.BytesIO(data))
            rec = parse_carfax(lines, name)
            if rec and rec["VIN"] not in existing_vins:
                upsert_carfax_cache(rec["VIN"], rec)
                existing_vins.add(rec["VIN"])
                parsed_new += 1
        except Exception:
            # Continue on errors; keep robust
            continue
    return parsed_new

def carfax_cache_as_df() -> pd.DataFrame:
    if not carfax_cache:
        return pd.DataFrame(columns=["VIN"])
    # Normalize OwnerRatings into objects (ensure list)
    rows = []
    for vin, rec in carfax_cache.items():
        row = {**rec}
        if isinstance(row.get("OwnerRatings"), str):
            try:
                row["OwnerRatings"] = json.loads(row["OwnerRatings"])
            except Exception:
                row["OwnerRatings"] = []
        rows.append(row)
    return pd.DataFrame(rows)

# ------------------ Cache upserts ------------------
carfax_cache = load_json(CARFAX_CACHE_PATH)  # {VIN: parsed dict}
story_cache  = load_json(STORY_CACHE_PATH)   # {VIN: story str}

def upsert_carfax_cache(vin: str, rec: dict):
    rec = {**rec, "last_updated": datetime.now().isoformat(timespec="seconds")}
    carfax_cache[vin] = rec
    save_json(CARFAX_CACHE_PATH, carfax_cache)

def upsert_story_cache(vin: str, story: str):
    story_cache[vin] = story
    save_json(STORY_CACHE_PATH, story_cache)

# ------------------ Scoring & Summaries ------------------
def estimate_service_interval(row: pd.Series):
    """Estimate miles per service event if mileage data exists."""
    try:
        miles = row.get("MileageNum", 0)
        svc = row.get("ServiceEvents", 0)
        if miles and svc and svc > 0:
            return round(miles / svc, -2)  # nearest 100
    except Exception:
        pass
    return None

def carfax_quality_score(row: pd.Series):
    """0–100 quality score from accidents, owners, service, major parts."""
    sev = str(row.get("AccidentSeverity") or "none").lower()
    owners = row.get("OwnerCount") or 1
    services = row.get("ServiceEvents") or 0
    major_parts = row.get("MajorParts") or "None"

    score = 80
    # Accidents
    if sev == "minor": score -= 5
    elif sev == "moderate": score -= 15
    elif sev == "severe": score -= 30
    # Owners
    if owners > 2: score -= 10
    elif owners == 2: score -= 5
    # Service
    if services >= 6: score += 10
    elif services <= 1: score -= 10
    # Major parts
    if isinstance(major_parts, str) and major_parts.strip() and major_parts != "None":
        score -= 10 * len([p for p in major_parts.split(",") if p.strip()])

    score = max(0, min(100, score))
    label = "Excellent" if score >= 85 else "Good" if score >= 70 else "Fair" if score >= 55 else "Poor"
    return score, label

def calc_vehicle_story(row: pd.Series):
    """Vehicle Story Index (VSI): blends history + owner behavior + service cadence."""
    score = 80
    sev = str(row.get("AccidentSeverity") or "none").lower()
    if sev == "minor": score -= 5
    elif sev == "moderate": score -= 15
    elif sev == "severe": score -= 25

    owners = row.get("OwnerCount") or 1
    services = row.get("ServiceEvents") or 0
    interval = row.get("AvgServiceInterval", None)
    parts = row.get("MajorParts") or "None"
    ratings = row.get("OwnerRatings") or []

    # Owner stability
    if owners > 2:
        score -= 5 * (owners - 2)
    # Service cadence
    if services >= 5: score += 5
    if interval and isinstance(interval, (int,float)):
        if interval < 7000: score += 5
        elif interval > 10000: score -= 5
    # Major parts penalty
    if isinstance(parts, str) and parts.strip() and parts != "None":
        score -= 5 * len([p for p in parts.split(",") if p.strip()])

    # Per-owner consistency (if available)
    if isinstance(ratings, list) and ratings:
        owner_avg = sum(o.get("Score", 0) for o in ratings) / len(ratings)
        score = (score + owner_avg) / 2

    score = max(0, min(100, score))
    label = "Excellent" if score >= 85 else "Good" if score >= 70 else "Fair" if score >= 55 else "Poor"
    return score, label

def summarize_carfax(row: pd.Series):
    """Readable summary line from parsed fields — fully safe against None/NaN."""
    parts = []

    sev = row.get("AccidentSeverity")
    if isinstance(sev, str):
        sev_lower = sev.lower()
        if sev_lower != "none":
            parts.append(f"{sev_lower.title()} accident reported")
        else:
            parts.append("No accidents reported")
    else:
        parts.append("No accident data")

    owners = row.get("OwnerCount")
    if owners and not pd.isna(owners):
        owners_int = int(owners)
        parts.append(f"{owners_int} owner{'s' if owners_int != 1 else ''}")

    services = row.get("ServiceEvents")
    if services and not pd.isna(services):
        services_int = int(services)
        parts.append(f"{services_int} service record{'s' if services_int != 1 else ''}")

    usage = row.get("UsageType")
    if isinstance(usage, str) and usage.strip():
        parts.append(f"{usage.title()} use")

    odo = row.get("OdometerIssue")
    if isinstance(odo, str) and odo.strip().lower() == "yes":
        parts.append("⚠️ Odometer issue")

    return " • ".join(parts) if parts else "No Carfax data available."


# ------------------ AI helpers ------------------
def ai_interpret_query(prompt: str) -> dict:
    """Natural query -> filters. Falls back to rule engine if API unavailable."""
    def fallback_rules(q: str) -> dict:
        q = q.lower()
        filters = {}
        if "suv" in q: filters["Body"] = "SUV"
        if "truck" in q: filters["Body"] = "Truck"
        if "sedan" in q: filters["Body"] = "Sedan"
        if "coupe" in q: filters["Body"] = "Coupe"
        if "van" in q: filters["Body"] = "Van"
        if "awd" in q or "4wd" in q: filters["DriveTrain"] = "AWD"
        if "fwd" in q: filters["DriveTrain"] = "FWD"
        if "rwd" in q: filters["DriveTrain"] = "RWD"
        m = re.search(r"under\s*\$?(\d[\d,]*)", q)
        if m: filters["PriceMax"] = float(m.group(1).replace(",",""))
        m = re.search(r"over\s*\$?(\d[\d,]*)", q)
        if m: filters["PriceMin"] = float(m.group(1).replace(",",""))
        m = re.search(r"under\s*(\d[\d,]*)\s*miles?", q)
        if m: filters["MileageMax"] = float(m.group(1).replace(",",""))
        m = re.search(r"(\d{4})\s*to\s*(\d{4})", q)
        if m: filters["YearMin"], filters["YearMax"] = int(m.group(1)), int(m.group(2))
        return filters

    if not (AI_AVAILABLE and openai):
        return fallback_rules(prompt)

    try:
        system = "You are a car search interpreter. Output ONLY a compact JSON object of filters."
        user = (
            "Parse this free-text query into filters for inventory. Allowed keys: "
            "Body, Make, Model, DriveTrain, PriceMax, PriceMin, MileageMax, MileageMin, YearMin, YearMax. "
            "Return JSON only, no prose.\n"
            f"Query: {prompt}"
        )
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":system},{"role":"user","content":user}],
            temperature=0.1,
        )
        txt = resp["choices"][0]["message"]["content"]
        try:
            return json.loads(txt)
        except Exception:
            return fallback_rules(prompt)
    except Exception:
        return fallback_rules(prompt)

def ai_talk_track(row: pd.Series) -> str:
    """Short persuasive talk track; template fallback."""
    base = f"{row.get('Year','')} {row.get('Make','')} {row.get('Model','')} {row.get('Trim') or ''}".strip()
    fallback = (
        f"{base}: well-kept with {row.get('Mileage','?')} miles, "
        f"{'clean history' if (str(row.get('AccidentSeverity','none'))=='none') else 'documented history'}, "
        f"priced at ${row.get('Price')}. Strong value for its condition."
    )
    if not (AI_AVAILABLE and openai):
        return fallback
    try:
        ctx = f"""
        Vehicle: {base}
        Accident Severity: {row.get('AccidentSeverity')}
        Owner Count: {row.get('OwnerCount')}
        Service Events: {row.get('ServiceEvents')}
        Safety Score: {row.get('SafetyScore')}
        Story Score: {row.get('StoryScore')}
        Value Category: {row.get('ValueCategory')}
        Price: {row.get('Price')}
        """
        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content":"You are a concise, friendly car sales pro."},
                {"role":"user","content":f"Write a two-sentence talk track in plain language:\n{ctx}"}
            ],
            temperature=0.6,
        )
        return res["choices"][0]["message"]["content"].strip()
    except Exception:
        return fallback

def ai_vehicle_story(vin: str, carfax_text: str) -> str:
    """AI story with per-VIN caching."""
    if vin in story_cache:
        return story_cache[vin]
    if not carfax_text:
        story = "(No Carfax text available for story.)"
        upsert_story_cache(vin, story)
        return story
    if not (AI_AVAILABLE and openai):
        # fallback summary
        snippet = (carfax_text[:220] + "…") if len(carfax_text) > 220 else carfax_text
        story = f"(AI disabled) Summary snippet: {snippet}"
        upsert_story_cache(vin, story)
        return story

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            temperature=0.6,
            messages=[
                {"role": "system", "content":
                 "You are an expert automotive storyteller for a dealership. "
                 "Summarize Carfax reports into 2–3 concise, compelling sentences highlighting maintenance cadence, "
                 "ownership stability, and notable events. Be trustworthy and specific without exaggeration."},
                {"role": "user", "content": f"VIN: {vin}\nCarfax Report (text extract):\n{carfax_text}"}
            ]
        )
        story = response["choices"][0]["message"]["content"].strip()
        upsert_story_cache(vin, story)
        return story
    except Exception as e:
        story = f"(AI story unavailable: {e})"
        upsert_story_cache(vin, story)
        return story

# ------------------ Upload Savers ------------------
def save_uploaded_inventory(file) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = file.name
    out = os.path.join(LISTINGS_DIR, f"{stamp}__{base}")
    with open(out, "wb") as f:
        f.write(file.getbuffer())
    return out

def save_uploaded_carfax_zip(file) -> int:
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
    added = 0
    for file in files:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = os.path.join(CARFAX_DIR, f"{stamp}__{file.name}")
        with open(out, "wb") as f:
            f.write(file.getbuffer())
        added += 1
    return added

# ------------------ Session ------------------
ss = st.session_state
ss.setdefault("inv_path", None)
ss.setdefault("data_df", None)

# ------------------ Sidebar ------------------
with st.sidebar:
    st.header("Inventory")
    use_last_inv = st.checkbox("Use last saved inventory (data/listings)", value=True)
    inv_upload   = st.file_uploader("Upload new inventory (.csv/.xls/.xlsx)", type=["csv","xls","xlsx"])

    st.header("Carfax PDFs")
    cf_zip_upload  = st.file_uploader("Upload Carfax ZIP (optional)", type=["zip"])
    cf_pdf_uploads = st.file_uploader("Upload individual Carfax PDF(s)", type=["pdf"], accept_multiple_files=True)
    st.caption("All PDFs persist in data/carfaxes/. Parsed data caches to data/carfax_cache.json.")

    st.divider()
    st.markdown("### AI Settings")
    st.caption("AI Search & AI Stories require an OpenAI API key in .streamlit/secrets.toml")
    ai_enabled = AI_AVAILABLE and (openai is not None)
    st.write(f"AI features enabled: **{'Yes' if ai_enabled else 'No'}**")

    st.divider()
    process_btn = st.button("🔄 Process / Refresh")
    reset_btn   = st.button("Reset session (keep files & cache)")

if reset_btn:
    ss["inv_path"] = None
    ss["data_df"] = None
    st.success("Session state reset. Files & caches are preserved.")
    st.stop()

# Save uploads immediately (persistent)
added_pdfs = 0
if inv_upload is not None:
    ss["inv_path"] = save_uploaded_inventory(inv_upload)
    st.sidebar.success(f"Inventory saved to {ss['inv_path']}")

if cf_zip_upload is not None:
    try:
        added_pdfs += save_uploaded_carfax_zip(cf_zip_upload)
    except zipfile.BadZipFile:
        st.sidebar.error("The uploaded file is not a valid ZIP. Please re-save and upload again.")

if cf_pdf_uploads:
    added_pdfs += save_uploaded_carfax_pdfs(cf_pdf_uploads)

if added_pdfs:
    st.sidebar.success(f"Saved {added_pdfs} Carfax PDF(s) into {CARFAX_DIR}")

# ------------------ Auto-Load & Parse ------------------
# 1) Choose inventory file (latest if none uploaded)
inv_path = ss.get("inv_path")
if use_last_inv:
    if inv_path is None or not os.path.exists(inv_path):
        auto = latest_file_in(LISTINGS_DIR)
        if auto:
            inv_path = auto
            ss["inv_path"] = inv_path

if inv_path is None or not os.path.exists(inv_path):
    st.info("Upload an inventory file (or ensure one exists in data/listings/).")
    st.stop()

# 2) Auto-parse any new Carfax PDFs (once)
if process_btn or added_pdfs or True:
    new_count = parse_new_carfax_pdfs()
    if new_count:
        st.info(f"Parsed {new_count} new Carfax PDF(s) and updated cache.")

# 3) Build Carfax DataFrame from cache
cf_df = carfax_cache_as_df()

# ------------------ Load Inventory ------------------
try:
    ext = os.path.splitext(inv_path)[1].lower()
    if ext == ".csv":
        raw = pd.read_csv(inv_path)
        st.success(f"✅ Loaded CSV: {os.path.basename(inv_path)}")
    elif ext in (".xls", ".xlsx"):
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        raw = pd.read_excel(inv_path, engine=engine)
        st.success(f"✅ Loaded Excel: {os.path.basename(inv_path)}")
    else:
        try:
            raw = pd.read_csv(inv_path)
            st.warning(f"⚠️ Unknown extension; parsed as CSV: {os.path.basename(inv_path)}")
        except Exception:
            raw = pd.read_excel(inv_path, engine="openpyxl")
            st.warning(f"⚠️ Unknown extension; parsed as Excel: {os.path.basename(inv_path)}")
except Exception as e:
    st.error(f"❌ Could not read inventory: {e}")
    st.stop()

raw.columns = [c.strip() for c in raw.columns]
vin_col = next((c for c in raw.columns if "vin" in c.lower()), None)
if not vin_col:
    st.error("No VIN column found in inventory.")
    st.stop()

# Normalize inventory columns
inv = pd.DataFrame({
    "VIN": raw[vin_col].astype(str).str.upper().str.strip(),
    "Year": raw.get("Year"),
    "Make": raw.get("Make"),
    "Model": raw.get("Model"),
    "Trim": raw.get("Trim"),
    "Body": raw.get("Body"),
    "Drive Train": raw.get("Drive Train") if "Drive Train" in raw.columns else raw.get("DriveTrain"),
    "Price": (raw.get("Website Basis") if "Website Basis" in raw.columns else raw.get("Price")),
    "KBBValue": (raw.get("MSRP / KBB") if "MSRP / KBB" in raw.columns else raw.get("KBB")),
    "Mileage": raw.get("Mileage"),
    "Days": (raw.get("Days In Inv") if "Days In Inv" in raw.columns else raw.get("DaysInInventory")),
    "CPO": raw.get("CPO"),
    "Warranty": (raw.get("Warr.") if "Warr." in raw.columns else raw.get("Warranty")),
    "Status": raw.get("Status"),
})

inv["PriceNum"]   = inv["Price"].apply(to_num)
inv["MileageNum"] = inv["Mileage"].apply(to_num)

# ------------------ Merge Inventory + Carfax Cache ------------------
if cf_df is None or cf_df.empty or "VIN" not in cf_df.columns:
    cf_df = pd.DataFrame(columns=["VIN"])

data = inv.merge(cf_df, on="VIN", how="left")
data["CarfaxUploaded"] = data["VIN"].isin(cf_df["VIN"]) if not cf_df.empty else False

# Derived metrics
data["AvgServiceInterval"] = data.apply(estimate_service_interval, axis=1)
data["CarfaxQualityScore"], data["CarfaxQualityLabel"] = zip(*data.apply(carfax_quality_score, axis=1))
data["StoryScore"], data["StoryLabel"] = zip(*data.apply(calc_vehicle_story, axis=1))

# Stub SafetyScore if missing
rng = np.random.default_rng(7)
if "SafetyScore" not in data.columns or data["SafetyScore"].isna().all():
    data["SafetyScore"] = rng.integers(72, 95, len(data))

# Smart Score (blend Carfax, Safety, Mileage)
data["Score"] = np.round(
    (data["CarfaxQualityScore"]*0.5 + data["SafetyScore"]*0.3 + (100 - (data["MileageNum"].fillna(0)/2000).clip(0,100))*0.2),
    0
)

# Value category
data["ValueCategory"] = np.where(
    data["PriceNum"].notna() & data["KBBValue"].notna() & (data["PriceNum"] < data["KBBValue"]*0.95), "Under Market",
    np.where(data["PriceNum"].notna() & data["KBBValue"].notna() & (data["PriceNum"] > data["KBBValue"]*1.05), "Over Market", "At Market")
)
data["SalesMood"] = np.where(data["Score"]>=85,"🟢 Confident","🟡 Balanced")

# Save to session
ss["data_df"] = data.copy()

# ------------------ TABS ------------------
tab_overview, tab_ai, tab_alerts = st.tabs(
    ["📊 Overview", "🧠 AI Search", "⚠ Alerts"]
)

# ========== Overview ==========
with tab_overview:
    data = ss["data_df"].copy()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Vehicles", f"{len(data)}")
    c2.metric("Avg Smart", f"{data['Score'].mean():.1f}")
    c3.metric("Carfax Attached", f"{(data['CarfaxUploaded'].mean()*100 if len(data) else 0):.0f}%")
    c4.metric("Avg Mileage", f"{data['MileageNum'].mean():.0f}" if data['MileageNum'].notna().any() else "—")

    st.divider()
    st.subheader("Inventory Table")
    cols = [
        "CarfaxUploaded","VIN","Year","Make","Model","Trim","Body","Drive Train",
        "Mileage","Price","KBBValue","ValueCategory",
        "AccidentSeverity","OwnerCount","ServiceEvents","MajorParts",
        "CarfaxQualityLabel","CarfaxQualityScore","StoryLabel","StoryScore",
        "SafetyScore","Score","Days","Status"
    ]
    cols = [c for c in cols if c in data.columns]
    st.dataframe(data[cols], use_container_width=True, hide_index=True)

# ========== AI Search ==========
with tab_ai:
    data = ss["data_df"].copy()
    st.subheader("Natural Language Search & Cards")
    st.caption("Tip: add your OpenAI key in .streamlit/secrets.toml for best results.")

    query_text = st.text_input("Try: 'SUV under 25k with AWD and under 40k miles (2018 to 2022)'")
    fc1, fc2, fc3, fc4 = st.columns(4)
    sc_min, sc_max = fc1.slider("Smart Score", 0, 100, (70, 100))
    safety_cut = fc2.slider("Min Safety", 0, 100, 70)
    make_sel   = fc3.multiselect("Make", sorted(list(data["Make"].dropna().unique())))
    val_sel    = fc4.multiselect("Value Category", ["Under Market","At Market","Over Market"])

    mask = pd.Series(True, index=data.index)
    mask &= data["Score"].between(sc_min, sc_max)
    mask &= data["SafetyScore"] >= safety_cut
    if make_sel: mask &= data["Make"].isin(make_sel)
    if val_sel:  mask &= data["ValueCategory"].isin(val_sel)

    # AI or fallback interpretation
    if query_text.strip():
        filters = ai_interpret_query(query_text)
        st.caption(f"🧠 Filters: {filters}")
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
        if "YearMin" in filters and "Year" in data.columns:
            mask &= pd.to_numeric(data["Year"], errors="coerce") >= filters["YearMin"]
        if "YearMax" in filters and "Year" in data.columns:
            mask &= pd.to_numeric(data["Year"], errors="coerce") <= filters["YearMax"]

    filtered = data.loc[mask].copy()
    st.divider()
    st.subheader(f"Results ({len(filtered)})")
    view_as_cards = st.toggle("Card View", value=True)

    if filtered.empty:
        st.warning("No vehicles match your filters.")
    else:
        if view_as_cards:
            for _, r in filtered.head(120).iterrows():
                with st.container(border=True):
                    c1, c2, c3 = st.columns([2,1,2])

                    # LEFT: Title + Summary + Files + AI story button
                    with c1:
                        yr = int(r['Year']) if pd.notna(r['Year']) else ''
                        title = f"{yr} {r.get('Make','')} {r.get('Model','')} {r.get('Trim') or ''}".strip()
                        st.markdown(f"**{title}**")
                        st.markdown(f"<div style='font-size:1.1rem'><b>${r.get('Price')}</b></div>", unsafe_allow_html=True)
                        st.caption(f"{r.get('Body','') or ''} • {r.get('Mileage')} mi • {r.get('Drive Train') or ''}")

                        # Carfax summary & story label
                        summary = summarize_carfax(r)
                        svc_interval = r.get("AvgServiceInterval")
                        svc_info = f" • Avg service every ~{int(svc_interval):,} mi" if svc_interval else ""
                        major_parts = r.get("MajorParts", "None")
                        major_info = f" • Major parts replaced: {major_parts}" if isinstance(major_parts, str) and major_parts and major_parts != "None" else ""
                        st.markdown(f"_{summary}{svc_info}{major_info}_")
                        st.caption(f"**Vehicle Story: {r.get('StoryLabel','?')} ({int(r.get('StoryScore',0))}/100)**")

                        # Owner ratings (if any)
                        if isinstance(r.get("OwnerRatings"), list) and r["OwnerRatings"]:
                            owners_summary = " • ".join([f"Owner {o['Owner']}: {o['Score']}/100" for o in r["OwnerRatings"]])
                            st.caption(f"Owner Ratings: {owners_summary}")

                        # Carfax file access
                        cf_path = find_carfax_file(r['VIN'])
                        if cf_path and os.path.exists(cf_path):
                            with open(cf_path, "rb") as f:
                                st.download_button("📄 Download Carfax PDF", f.read(), file_name=os.path.basename(cf_path), mime="application/pdf", key=f"dl_{r['VIN']}")
                        else:
                            st.caption("No Carfax file found for this VIN.")

                        # Per-VIN AI Story button
                        if st.button(f"🧠 Generate Story for {r['VIN']}", key=f"story_{r['VIN']}"):
                            with st.spinner("Generating story..." if ai_enabled else "Loading cached/fallback story..."):
                                story = ai_vehicle_story(r["VIN"], r.get("CarfaxText","") or "")
                            st.write(f"**AI Story:** {story}")

                    # MIDDLE: Key metrics
                    with c2:
                        st.metric("Smart", f"{r['Score']:.0f}")
                        st.metric("Safety", f"{r['SafetyScore']:.0f}")
                        st.metric("Carfax", f"{int(r['CarfaxQualityScore'])}/100")
                        st.metric("Value", r.get("ValueCategory","—"))

                    # RIGHT: Talk track
                    with c3:
                        tt = ai_talk_track(r) if ai_enabled else ""
                        if tt:
                            st.caption(tt)

                    st.divider()
        else:
            cols = [
                "CarfaxUploaded","VIN","Year","Make","Model","Trim","Body","Drive Train",
                "Mileage","Price","KBBValue","ValueCategory",
                "AccidentSeverity","OwnerCount","ServiceEvents","MajorParts",
                "CarfaxQualityLabel","CarfaxQualityScore","StoryLabel","StoryScore",
                "SafetyScore","Score","Days","Status"
            ]
            cols = [c for c in cols if c in filtered.columns]
            st.dataframe(filtered[cols], use_container_width=True, hide_index=True)

    st.divider()
    cA, cB = st.columns(2)
    cA.download_button(
        "Download CSV",
        data=filtered.drop(columns=["PriceNum","MileageNum"], errors="ignore").to_csv(index=False).encode("utf-8"),
        file_name="filtered_results.csv",
        mime="text/csv"
    )
    cB.download_button(
        "Download Excel",
        data=to_excel_bytes(filtered.drop(columns=["PriceNum","MileageNum"], errors="ignore")),
        file_name="filtered_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ========== Alerts ==========
with tab_alerts:
    data = ss["data_df"].copy()
    st.subheader("Daily Alerts & Opportunities")
    alerts = []

    # Over Market
    if "ValueCategory" in data.columns:
        over = data[data["ValueCategory"]=="Over Market"]
        if not over.empty:
            alerts.append(f"⚠️ {len(over)} vehicles flagged 'Over Market' — consider price review.")

    # Stale inventory
    if "Days" in data.columns and data["Days"].notna().any():
        stale = data[pd.to_numeric(data["Days"], errors="coerce") >= 30]
        if not stale.empty:
            alerts.append(f"🕒 {len(stale)} vehicles 30+ days in inventory — spotlight or reprice.")

    # Strong promo picks
    strong = data[(data["Score"]>=90) & (data["ValueCategory"]!="Over Market")]
    if not strong.empty:
        alerts.append(f"🔥 {len(strong)} high-score vehicles suitable for promo.")

    if alerts:
        for a in alerts:
            st.write(a)
        st.divider()
        st.write("**Top Promo Candidates**")
        cols = [c for c in ["VIN","Year","Make","Model","Trim","Price","Score","StoryLabel","CarfaxQualityLabel","ValueCategory","CarfaxUploaded"] if c in data.columns]
        st.dataframe(strong.sort_values("Score", ascending=False)[cols].head(15), use_container_width=True, hide_index=True)
    else:
        st.success("No critical alerts. Inventory looks balanced today ✅")
