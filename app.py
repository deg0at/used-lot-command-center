# ==============================================================
# Used Lot Car Finder — Inventory Insights (v12)
# - Persistent storage (listings, carfaxes, caches)
# - Auto-parse only new Carfax PDFs; cached forever
# - Per-VIN AI story buttons (cached)
# - Guided vehicle finder with card/table views
# - Safe/NaN-proof scoring + summaries + owner ratings
# ==============================================================

import io, os, re, json, zipfile, traceback, base64, logging
from typing import Callable, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
from pypdf import PdfReader

# ------------------ App Config ------------------
st.set_page_config(page_title="Used Lot Car Finder", layout="wide")
st.title("🚗 Used Lot Car Finder")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CARFAX_DIR = os.path.join(DATA_DIR, "carfaxes")
LISTINGS_DIR = os.path.join(DATA_DIR, "listings")
CARFAX_CACHE_PATH = os.path.join(DATA_DIR, "carfax_cache.json")
STORY_CACHE_PATH  = os.path.join(DATA_DIR, "story_cache.json")
CARFAX_PARSE_INDEX_PATH = os.path.join(DATA_DIR, "carfax_parse_index.json")

# Ensure folders/files exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CARFAX_DIR, exist_ok=True)
os.makedirs(LISTINGS_DIR, exist_ok=True)
for fpath in [CARFAX_CACHE_PATH, STORY_CACHE_PATH, CARFAX_PARSE_INDEX_PATH]:
    if not os.path.exists(fpath):
        with open(fpath, "w") as f: f.write("{}")


# Session state helpers for vehicle table/card views
if "inventory_view_mode" not in st.session_state:
    st.session_state["inventory_view_mode"] = "table"
if "inventory_selected_vin" not in st.session_state:
    st.session_state["inventory_selected_vin"] = None
if "inventory_selected_row_position" not in st.session_state:
    st.session_state["inventory_selected_row_position"] = None
if "inventory_table_selection" not in st.session_state:
    st.session_state["inventory_table_selection"] = {"rows": []}

VIN_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
URL_RE = re.compile(r"https?://[^\s\"'>)]+", re.IGNORECASE)


def extract_first_url(value: str) -> Optional[str]:
    """Return the first HTTP(S) URL found within *value*, if any."""

    if not value:
        return None

    if isinstance(value, str):
        txt = value.strip()
    else:
        txt = str(value).strip()

    if not txt or txt.lower() in {"nan", "none", "null"}:
        return None

    if txt.lower().startswith(("http://", "https://")):
        return txt

    match = URL_RE.search(txt)
    if match:
        return match.group(0)

    return None


def clean_carfax_link(value) -> str:
    """Normalize any Carfax link value into a direct URL or empty string."""

    link = extract_first_url(value)
    return link or ""

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
    """Return absolute path to Carfax PDF for a VIN, using parse index fallback."""

    if not vin:
        return None

    vin = vin.upper().strip()

    # Prefer direct filename match (covers files manually named with VIN)
    for fname in sorted(os.listdir(CARFAX_DIR)):
        if vin in fname.upper():
            path = os.path.join(CARFAX_DIR, fname)
            if os.path.isfile(path):
                return path

    # Fallback: look up by VIN in the parse index (handles renamed/hashed files)
    matches = []
    for fname, meta in carfax_parse_index.items():
        if not isinstance(meta, dict):
            continue
        if str(meta.get("vin", "")).upper() != vin:
            continue
        path = os.path.join(CARFAX_DIR, fname)
        if not os.path.isfile(path):
            continue
        mtime = meta.get("mtime") or _safe_file_mtime(path) or 0
        matches.append((mtime, path))

    if matches:
        # Return the most recently modified file for the VIN
        matches.sort(key=lambda x: x[0], reverse=True)
        return matches[0][1]

    return None


def clear_inventory_selection_state(trigger_rerun: bool = False):
    """Reset session state used for toggling between the table and card views."""

    st.session_state["inventory_view_mode"] = "table"
    st.session_state["inventory_selected_vin"] = None
    st.session_state["inventory_selected_row_position"] = None
    st.session_state["inventory_table_selection"] = {"rows": []}
    st.session_state.pop("inventory_table", None)

    if trigger_rerun:
        st.experimental_rerun()


def render_vehicle_card(
    row: pd.Series,
    ai_enabled: bool,
    show_full_details: bool = False,
    compare_checkbox_key: Optional[str] = None,
    show_similar_controls: bool = False,
    similar_checkbox_keys: Optional[dict] = None,
    similar_defaults: Optional[dict] = None,
):
    """Render a single vehicle card with summary metrics and optional full details.

    When *compare_checkbox_key* is provided, a compare checkbox is displayed in the
    upper-right corner of the card. When *show_similar_controls* is ``True`` the
    card will also render "show similar" checkboxes using the provided
    *similar_checkbox_keys* and *similar_defaults* to manage state.

    Returns:
        Optional[dict]: Information about the currently selected "show similar"
        filters, keyed by "vin" and each enabled filter type. ``None`` when the
        caller does not request similar vehicle controls.
    """

    vin_value = str(row.get("VIN") or "Unknown VIN")

    def _clean_text(value) -> str:
        if isinstance(value, str):
            return value.strip()
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value)

    similar_info = None

    card_container = st.container(border=True)
    with card_container:
        layout_cols = card_container.columns([30, 1])
        main_col = layout_cols[0]
        with layout_cols[1]:
            if compare_checkbox_key:
                st.checkbox("Compare", key=compare_checkbox_key)
            else:
                st.empty()

        with main_col:
            c1, c2, c3 = main_col.columns([2, 1, 2])

            with c1:
                year_val = row.get("Year")
                try:
                    yr = int(float(year_val)) if pd.notna(year_val) else ""
                except (TypeError, ValueError):
                    yr = ""

                title = f"{yr} {row.get('Make','')} {row.get('Model','')} {row.get('Trim') or ''}".strip()
                st.markdown(f"**{title or 'Vehicle'}**")
                price_raw = row.get("Price")
                price_text = _clean_text(price_raw)
                if not price_text:
                    price_text = "—"
                st.markdown(
                    f"<div style='font-size:1.1rem'><b>{price_text}</b></div>",
                    unsafe_allow_html=True,
                )

                body_text = _clean_text(row.get("Body"))
                drivetrain_text = _clean_text(row.get("Drive Train"))
                mileage_raw = row.get("Mileage")
                mileage_text = ""
                if isinstance(mileage_raw, str):
                    mileage_text = mileage_raw.strip()
                elif mileage_raw is not None:
                    try:
                        if not pd.isna(mileage_raw):
                            mileage_text = f"{int(float(mileage_raw)):,}"
                    except Exception:
                        mileage_text = str(mileage_raw)
                mileage_display = ""
                if mileage_text:
                    lower_mileage = mileage_text.lower()
                    if lower_mileage.endswith("mi") or "mile" in lower_mileage:
                        mileage_display = mileage_text
                    else:
                        mileage_display = f"{mileage_text} mi"
                descriptors = " • ".join(
                    [
                        txt
                        for txt in [body_text, mileage_display, drivetrain_text]
                        if txt
                    ]
                )
                if descriptors:
                    st.caption(descriptors)

                st.caption(f"VIN: {vin_value}")

                summary = summarize_carfax(row)
                svc_interval = row.get("AvgServiceInterval")
                svc_info = ""
                try:
                    if svc_interval and not pd.isna(svc_interval):
                        svc_info = f" • Avg service every ~{int(float(svc_interval)):,} mi"
                except Exception:
                    svc_info = ""
                major_parts = row.get("MajorParts", "None")
                major_info = (
                    f" • Major parts replaced: {major_parts}"
                    if isinstance(major_parts, str) and major_parts and major_parts != "None"
                    else ""
                )
                st.markdown(f"_{summary}{svc_info}{major_info}_")
                st.caption(
                    f"**Vehicle Story: {row.get('StoryLabel','?')} ({int(row.get('StoryScore',0))}/100)**"
                )

                owner_ratings = row.get("OwnerRatings")
                if isinstance(owner_ratings, list) and owner_ratings:
                    owners_summary = " • ".join(
                        [
                            f"Owner {o.get('Owner')}: {o.get('Score')}/100"
                            for o in owner_ratings
                            if isinstance(o, dict)
                        ]
                    )
                    if owners_summary:
                        st.caption(f"Owner Ratings: {owners_summary}")

                carfax_link = clean_carfax_link(row.get("CarfaxLink", ""))
                cf_path = find_carfax_file(vin_value)
                pdf_bytes = None

                if cf_path and os.path.exists(cf_path):
                    try:
                        with open(cf_path, "rb") as f:
                            pdf_bytes = f.read()
                    except OSError:
                        pdf_bytes = None

                if carfax_link:
                    st.markdown(
                        (
                            "<a style='display:inline-block;margin-bottom:0.5rem;padding:0.35rem 0.75rem;"
                            "background-color:#1f77b4;color:white;border-radius:4px;text-decoration:none;'"
                            f" href='{carfax_link}' target='_blank'>🔍 View Carfax</a>"
                        ),
                        unsafe_allow_html=True,
                    )
                elif pdf_bytes:
                    carfax_b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                    st.markdown(
                        (
                            "<a style='display:inline-block;margin-bottom:0.5rem;padding:0.35rem 0.75rem;"
                            "background-color:#1f77b4;color:white;border-radius:4px;text-decoration:none;'"
                            f" href='data:application/pdf;base64,{carfax_b64}' target='_blank'>🔍 View Carfax</a>"
                        ),
                        unsafe_allow_html=True,
                    )

                if pdf_bytes:
                    st.download_button(
                        "📄 Download Carfax PDF",
                        pdf_bytes,
                        file_name=os.path.basename(cf_path) if cf_path else f"{vin_value}_carfax.pdf",
                        mime="application/pdf",
                        key=f"dl_{vin_value}",
                    )

                if not carfax_link and not pdf_bytes:
                    st.caption("No Carfax file found for this VIN.")

                if st.button(f"🧠 Generate Story for {vin_value}", key=f"story_{vin_value}"):
                    with st.spinner(
                        "Generating story..." if ai_enabled else "Loading cached/fallback story..."
                    ):
                        story = ai_vehicle_story(row.get("VIN", ""), row.get("CarfaxText", "") or "")
                    st.write(f"**AI Story:** {story}")

            with c2:
                score = row.get("Score")
                safety = row.get("SafetyScore")
                carfax_score = row.get("CarfaxQualityScore")
                value_cat = row.get("ValueCategory", "—")

                try:
                    smart_value = f"{float(score):.0f}"
                except Exception:
                    smart_value = _clean_text(score) or "—"

                try:
                    safety_value = f"{float(safety):.0f}"
                except Exception:
                    safety_value = _clean_text(safety) or "—"

                try:
                    carfax_value = f"{int(float(carfax_score))}/100"
                except Exception:
                    carfax_value = _clean_text(carfax_score) or "—"

                st.metric("Smart", smart_value)
                st.metric("Safety", safety_value)
                st.metric("Carfax", carfax_value)
                st.metric("Value", value_cat)

            with c3:
                tt = ai_talk_track(row) if ai_enabled else ""
                if tt:
                    st.caption(tt)

            if show_similar_controls and similar_checkbox_keys:
                base_vin = None
                if isinstance(similar_defaults, dict):
                    base_vin = similar_defaults.get("base")

                sim_labels = [
                    ("Make", "make"),
                    ("Model", "model"),
                    ("Price", "price"),
                ]
                sim_cols = main_col.columns(3)
                sim_values = {}
                for (label, key_name), col in zip(sim_labels, sim_cols):
                    key = (similar_checkbox_keys or {}).get(key_name)
                    if not key:
                        sim_values[key_name] = False
                        continue

                    default_val = False
                    if base_vin == vin_value and isinstance(similar_defaults, dict):
                        default_val = bool(similar_defaults.get(key_name, False))

                    if key not in st.session_state:
                        st.session_state[key] = default_val
                    elif base_vin != vin_value and st.session_state.get(key):
                        st.session_state[key] = False
                    elif base_vin == vin_value and default_val and not st.session_state.get(key):
                        st.session_state[key] = default_val

                    sim_values[key_name] = col.checkbox(label, key=key)

                similar_info = {
                    "vin": vin_value,
                    **sim_values,
                }

            if show_full_details:
                details_df = pd.DataFrame(
                    {
                        "Field": list(row.index),
                        "Value": [row.get(col) for col in row.index],
                    }
                )
                st.markdown("#### Full Vehicle Details")
                st.dataframe(details_df, use_container_width=True, hide_index=True)

    return similar_info


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

def parse_new_carfax_pdfs(progress_callback: Optional[Callable[[int, int, Optional[str], int], None]] = None):
    """
    Parse only Carfax PDFs that produce a VIN not already in cache.
    If a PDF already yields a cached VIN, skip.
    """
    existing_vins = set(carfax_cache.keys())
    index_dirty = False

    # Prune index entries for files that no longer exist
    for fname in list(carfax_parse_index.keys()):
        if not os.path.exists(os.path.join(CARFAX_DIR, fname)):
            carfax_parse_index.pop(fname, None)
            index_dirty = True
    if index_dirty:
        save_json(CARFAX_PARSE_INDEX_PATH, carfax_parse_index)
        index_dirty = False

    pdfs = [f for f in os.listdir(CARFAX_DIR) if f.lower().endswith(".pdf")]
    total = len(pdfs)
    parsed_new = 0
    for idx, name in enumerate(sorted(pdfs), start=1):
        full = os.path.join(CARFAX_DIR, name)

        # Skip re-parsing if file unchanged and VIN already cached
        mtime = _safe_file_mtime(full)
        idx_entry = carfax_parse_index.get(name)
        if (
            mtime is not None
            and idx_entry
            and idx_entry.get("mtime") == mtime
            and idx_entry.get("vin") in carfax_cache
        ):
            existing_vins.add(idx_entry["vin"])
            if progress_callback:
                progress_callback(idx, total, name, parsed_new)
            continue

        # If VIN in filename and already cached, skip
        m = VIN_RE.search(name.upper())
        if m and (m.group(1) in existing_vins):
            if mtime is not None:
                record_parsed_carfax(name, m.group(1), mtime)
            if progress_callback:
                progress_callback(idx, total, name, parsed_new)
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
                record_parsed_carfax(name, rec["VIN"], mtime)
            elif rec and rec["VIN"] in carfax_cache:
                # Ensure index knows about this file -> VIN link
                existing_vins.add(rec["VIN"])
                record_parsed_carfax(name, rec["VIN"], mtime)
        except FileNotFoundError:
            # File disappeared between listing and open
            pass
        except Exception:
            # Continue on errors; keep robust
            continue
        finally:
            if progress_callback:
                progress_callback(idx, total, name, parsed_new)
    if progress_callback:
        progress_callback(total, total, None, parsed_new)
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
carfax_cache = load_json(CARFAX_CACHE_PATH)          # {VIN: parsed dict}
story_cache  = load_json(STORY_CACHE_PATH)           # {VIN: story str}
carfax_parse_index = load_json(CARFAX_PARSE_INDEX_PATH)  # {filename: {vin, mtime}}

def _safe_file_mtime(path: str) -> Optional[float]:
    try:
        return round(os.path.getmtime(path), 6)
    except (FileNotFoundError, OSError):
        return None

def upsert_carfax_cache(vin: str, rec: dict):
    rec = {**rec, "last_updated": datetime.now().isoformat(timespec="seconds")}
    carfax_cache[vin] = rec
    save_json(CARFAX_CACHE_PATH, carfax_cache)

def record_parsed_carfax(filename: str, vin: str, mtime: Optional[float] = None):
    if not filename or not vin:
        return
    base = os.path.basename(filename)
    if mtime is None:
        mtime = _safe_file_mtime(os.path.join(CARFAX_DIR, base))
    if mtime is None:
        return
    existing = carfax_parse_index.get(base, {})
    if existing.get("vin") == vin and existing.get("mtime") == mtime:
        return
    carfax_parse_index[base] = {
        "vin": vin,
        "mtime": mtime,
    }
    save_json(CARFAX_PARSE_INDEX_PATH, carfax_parse_index)

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
    raw = file.getvalue()
    if hasattr(file, "seek"):
        file.seek(0)
    with open(out, "wb") as f:
        f.write(raw)
    return out

def _normalize_carfax_name(name: str) -> str:
    base = os.path.basename(name)
    if "__" in base:
        return base.split("__", 1)[1]
    return base

def _existing_carfax_basenames() -> set:
    existing = set()
    try:
        for fname in os.listdir(CARFAX_DIR):
            existing.add(_normalize_carfax_name(fname))
    except Exception:
        pass
    return existing

def save_uploaded_carfax_zip(file) -> Tuple[int, int]:
    added = 0
    skipped = 0
    existing = _existing_carfax_basenames()
    file_bytes = io.BytesIO(file.getvalue())
    if hasattr(file, "seek"):
        file.seek(0)
    with zipfile.ZipFile(file_bytes) as z:
        for name in z.namelist():
            if name.lower().endswith(".pdf"):
                base = _normalize_carfax_name(os.path.basename(name))
                if base in existing:
                    skipped += 1
                    continue
                raw = z.read(name)
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                out = os.path.join(CARFAX_DIR, f"{stamp}__{base}")
                with open(out, "wb") as f:
                    f.write(raw)
                added += 1
                existing.add(base)
    return added, skipped

def save_uploaded_carfax_pdfs(files) -> Tuple[int, int]:
    added = 0
    skipped = 0
    existing = _existing_carfax_basenames()
    for file in files:
        base = _normalize_carfax_name(file.name)
        if base in existing:
            skipped += 1
            continue
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = os.path.join(CARFAX_DIR, f"{stamp}__{base}")
        raw = file.getvalue()
        if hasattr(file, "seek"):
            file.seek(0)
        with open(out, "wb") as f:
            f.write(raw)
        added += 1
        existing.add(base)
    return added, skipped

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
    st.caption("AI stories and talk tracks require an OpenAI API key in .streamlit/secrets.toml")
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
skipped_duplicates = 0
if inv_upload is not None:
    ss["inv_path"] = save_uploaded_inventory(inv_upload)
    st.sidebar.success(f"Inventory saved to {ss['inv_path']}")

if cf_zip_upload is not None:
    try:
        added, skipped = save_uploaded_carfax_zip(cf_zip_upload)
        added_pdfs += added
        skipped_duplicates += skipped
    except zipfile.BadZipFile:
        st.sidebar.error("The uploaded file is not a valid ZIP. Please re-save and upload again.")

if cf_pdf_uploads:
    added, skipped = save_uploaded_carfax_pdfs(cf_pdf_uploads)
    added_pdfs += added
    skipped_duplicates += skipped

if added_pdfs:
    st.sidebar.success(f"Saved {added_pdfs} Carfax PDF(s) into {CARFAX_DIR}")
if skipped_duplicates:
    st.sidebar.info(f"Skipped {skipped_duplicates} duplicate Carfax PDF filename(s).")

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
progress_placeholder = st.sidebar.empty()
status_placeholder = st.sidebar.empty()

PROGRESS_UI_STATE_KEY = "_carfax_progress_ui_active"
if PROGRESS_UI_STATE_KEY not in st.session_state:
    st.session_state[PROGRESS_UI_STATE_KEY] = True


def _safe_sidebar_update(callback, *args, **kwargs):
    """Safely call sidebar UI updates, disabling them if the client disconnects."""

    if not st.session_state.get(PROGRESS_UI_STATE_KEY, True):
        return

    try:
        callback(*args, **kwargs)
    except Exception as exc:  # Broad catch to swallow WebSocket disconnect noise
        st.session_state[PROGRESS_UI_STATE_KEY] = False
        logging.warning("Disabling Carfax progress UI updates after failure: %s", exc)


def _update_carfax_progress(current: int, total: int, filename: Optional[str], parsed_new: int):
    if total == 0:
        _safe_sidebar_update(progress_placeholder.empty)
        if filename is None and current == 0:
            _safe_sidebar_update(status_placeholder.info, "No Carfax PDFs found to parse.")
        return

    percent = int((current / total) * 100)
    if filename is not None:
        _safe_sidebar_update(
            progress_placeholder.progress,
            percent,
            text=f"Parsing Carfax PDFs… {current}/{total}"
        )
        _safe_sidebar_update(
            status_placeholder.info,
            f"Processing {os.path.basename(filename)} — {parsed_new} new parsed so far."
        )
    else:
        _safe_sidebar_update(progress_placeholder.empty)
        if parsed_new:
            _safe_sidebar_update(
                status_placeholder.success,
                f"Finished parsing Carfax PDFs. Added {parsed_new} new report{'s' if parsed_new != 1 else ''}."
            )
        else:
            _safe_sidebar_update(
                status_placeholder.info,
                "Finished parsing Carfax PDFs. No new reports detected."
            )

if process_btn or added_pdfs or True:
    new_count = parse_new_carfax_pdfs(progress_callback=_update_carfax_progress)
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
        try:
            raw = pd.read_excel(inv_path, engine=engine)
        except (zipfile.BadZipFile, ValueError) as exc:
            try:
                raw = pd.read_csv(inv_path)
            except Exception as csv_exc:
                raise csv_exc from exc
            st.warning(
                f"⚠️ Excel file appeared corrupted or mis-labeled ({exc}); loaded as CSV instead."
            )
            st.success(f"✅ Loaded CSV fallback: {os.path.basename(inv_path)}")
        else:
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

# Identify potential Carfax link column (varied vendor exports use different labels)
carfax_candidate_names = [
    "CarfaxLink", "Carfax Link", "CarFaxLink", "CarFax Link",
    "CarfaxURL", "CarFaxURL", "Carfax Url", "CarFax Url",
    "Vehicle History", "VehicleHistory", "Vehicle History Report",
    "VehicleHistoryReport", "History Report", "HistoryReport",
    "VehicleHistoryURL", "Vehicle History URL", "Carfax Report", "CarfaxReport",
]
carfax_link_col = next((c for c in carfax_candidate_names if c in raw.columns), None)
if not carfax_link_col:
    carfax_link_col = next(
        (
            c
            for c in raw.columns
            if "carfax" in c.lower()
            and any(k in c.lower() for k in ("link", "url", "report", "history"))
        ),
        None,
    )

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
    "CarfaxLink": (raw[carfax_link_col].apply(clean_carfax_link) if carfax_link_col else ""),
})

inv["PriceNum"]   = inv["Price"].apply(to_num)
inv["MileageNum"] = inv["Mileage"].apply(to_num)

# Filter out vehicles that aren't yet priced for sale
zero_price_mask = inv["PriceNum"].eq(0)
if zero_price_mask.any():
    inv = inv.loc[~zero_price_mask].copy()

# ------------------ Merge Inventory + Carfax Cache ------------------
if cf_df is None or cf_df.empty or "VIN" not in cf_df.columns:
    cf_df = pd.DataFrame(columns=["VIN"])

data = inv.merge(cf_df, on="VIN", how="left")
data["CarfaxUploaded"] = data["VIN"].isin(cf_df["VIN"]) if not cf_df.empty else False
if "CarfaxLink" in data.columns:
    has_carfax_link = data["CarfaxLink"].astype(str).str.contains(r"^https?://", case=False, regex=True, na=False)
    data["CarfaxUploaded"] = data["CarfaxUploaded"].fillna(False) | has_carfax_link

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

# Separate pending statuses from active inventory
if "Status" in data.columns:
    status_norm = data["Status"].astype(str).str.strip()
    status_clean = status_norm.str.lower().str.replace(r"[^a-z0-9]", "", regex=True)
    pending_ro_mask = status_clean.str.contains("pendingro", na=False)
    pending_deal_mask = status_clean.str.contains("pendingdeal", na=False)
else:
    pending_ro_mask = pd.Series(False, index=data.index)
    pending_deal_mask = pd.Series(False, index=data.index)

data["pend. RO"] = np.where(pending_ro_mask, 1, 0)
data["pend. deal"] = np.where(pending_deal_mask, 1, 0)

ss["pending_ro_df"] = data.loc[pending_ro_mask].copy()
ss["pending_deal_df"] = data.loc[pending_deal_mask].copy()

active_mask = ~(pending_ro_mask | pending_deal_mask)
data = data.loc[active_mask].copy()

# Save to session
ss["data_df"] = data.copy()

# ------------------ TABS ------------------
tab_overview, tab_listings, tab_finder = st.tabs(
    ["📊 Overview", "📋 Listings", "🔎 Vehicle Finder"]
)

# ========== Overview ==========
with tab_overview:
    data = ss["data_df"].copy()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Vehicles", f"{len(data)}")
    c2.metric("Avg Smart", f"{data['Score'].mean():.1f}" if not data.empty else "—")
    c3.metric(
        "Carfax Attached",
        f"{(data['CarfaxUploaded'].mean()*100 if len(data) else 0):.0f}%"
    )
    c4.metric(
        "Avg Mileage",
        f"{data['MileageNum'].mean():.0f}" if data['MileageNum'].notna().any() else "—"
    )

    st.divider()
    st.subheader("Top 10 Leaderboard")
    if data.empty:
        st.info("Upload inventory to see performance leaderboards.")
    else:
        top_cols = [
            "VIN","Year","Make","Model","Trim","Body","Price","Mileage",
            "Score","SafetyScore","CarfaxQualityScore","ValueCategory"
        ]
        top_cols = [c for c in top_cols if c in data.columns]
        top_sort_cols = [c for c in ["Score", "CarfaxQualityScore"] if c in data.columns]
        if top_sort_cols:
            top10 = data.sort_values(top_sort_cols, ascending=[False] * len(top_sort_cols)).head(10)
        else:
            top10 = data.head(10)
        st.dataframe(top10[top_cols], use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Best Values Under $15K")
    if data.empty or "PriceNum" not in data.columns or data["PriceNum"].dropna().empty:
        st.caption("No price data available to calculate sub-$15K values just yet.")
    else:
        under_15k = data[data["PriceNum"].between(0, 15000, inclusive="both")].copy()
        if under_15k.empty:
            st.caption("No vehicles currently priced under $15,000.")
        else:
            if "KBBValue" in under_15k.columns:
                kbb_numeric = pd.to_numeric(under_15k["KBBValue"], errors="coerce")
                under_15k.loc[:, "Savings"] = kbb_numeric - under_15k["PriceNum"]
            leaderboard_cols = [
                "VIN","Year","Make","Model","Trim","Price","Score","ValueCategory","Savings"
            ]
            leaderboard_cols = [c for c in leaderboard_cols if c in under_15k.columns]
            sort_cols = [c for c in ["Score", "Savings"] if c in under_15k.columns]
            if sort_cols:
                under_15k.sort_values(sort_cols, ascending=[False] + [False] * (len(sort_cols) - 1), inplace=True)
            st.dataframe(under_15k.head(10)[leaderboard_cols], use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("New Parts Spotlight")
    if data.empty or "MajorParts" not in data.columns:
        st.caption("No major part replacement information available.")
    else:
        parts_df = data.copy()
        parts_df["MajorPartsCount"] = (
            parts_df["MajorParts"]
            .fillna("")
            .apply(lambda val: len([p.strip() for p in str(val).split(",") if p.strip() and p.strip().lower() != "none"]))
        )
        multi_parts = parts_df[parts_df["MajorPartsCount"] >= 2].copy()
        if multi_parts.empty:
            st.caption("No vehicles with multiple new part replacements detected in Carfax reports.")
        else:
            spotlight_cols = [
                "VIN","Year","Make","Model","Trim","MajorParts","MajorPartsCount","Price","Mileage"
            ]
            spotlight_cols = [c for c in spotlight_cols if c in multi_parts.columns]
            multi_parts.sort_values("MajorPartsCount", ascending=False, inplace=True)
            st.dataframe(multi_parts.head(10)[spotlight_cols], use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Alerts & Opportunities")
    alerts = []
    if "ValueCategory" in data.columns:
        over = data[data["ValueCategory"] == "Over Market"]
        if not over.empty:
            alerts.append(f"⚠️ {len(over)} vehicles flagged 'Over Market' — consider price review.")
    if "Days" in data.columns and data["Days"].notna().any():
        stale = data[pd.to_numeric(data["Days"], errors="coerce") >= 30]
        if not stale.empty:
            alerts.append(f"🕒 {len(stale)} vehicles 30+ days in inventory — spotlight or reprice.")
    strong = data[(data["Score"] >= 90) & (data["ValueCategory"] != "Over Market")]
    if not strong.empty:
        alerts.append(f"🔥 {len(strong)} high-score vehicles suitable for promo.")

    if alerts:
        for msg in alerts:
            st.write(msg)
        st.divider()
        st.write("**Top Promo Candidates**")
        promo_cols = [
            "VIN","Year","Make","Model","Trim","Price","Score","StoryLabel",
            "CarfaxQualityLabel","ValueCategory","CarfaxUploaded"
        ]
        promo_cols = [c for c in promo_cols if c in data.columns]
        st.dataframe(
            strong.sort_values("Score", ascending=False)[promo_cols].head(15),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No critical alerts. Inventory looks balanced today ✅")

    pending_ro_df = ss.get("pending_ro_df")
    if isinstance(pending_ro_df, pd.DataFrame) and not pending_ro_df.empty:
        st.divider()
        st.subheader("Pending RO Vehicles")
        st.caption(
            "These vehicles are awaiting reconditioning orders and are excluded from the active inventory above."
        )
        pending_cols = [
            "VIN","Year","Make","Model","Trim","Price","Score","Days","Status","pend. RO"
        ]
        pending_cols = [c for c in pending_cols if c in pending_ro_df.columns]
        st.dataframe(pending_ro_df[pending_cols], use_container_width=True, hide_index=True)

# ========== Listings ==========
with tab_listings:
    data = ss["data_df"].copy()
    st.subheader("Inventory Listings")
    st.caption("Scroll the page to review the entire active inventory without an embedded window.")

    if data.empty:
        st.info("No active inventory available. Upload a listings file to get started.")
    else:
        cols = [
            "CarfaxUploaded","VIN","Year","Make","Model","Trim","Body","Drive Train",
            "Mileage","Price","KBBValue","ValueCategory",
            "AccidentSeverity","OwnerCount","ServiceEvents","MajorParts",
            "CarfaxQualityLabel","CarfaxQualityScore","StoryLabel","StoryScore",
            "SafetyScore","Score","Days","Status","pend. RO","pend. deal"
        ]
        cols = [c for c in cols if c in data.columns]
        table_height = max(360, int(42 * (len(data) + 1)))
        st.dataframe(
            data[cols],
            use_container_width=True,
            hide_index=True,
            height=table_height,
        )

# ========== Vehicle Finder ==========
with tab_finder:
    data = ss["data_df"].copy()
    st.subheader("Vehicle Finder")
    st.caption("Use the filters below to quickly zero-in on the right inventory by make, model, and vehicle type.")

    ss.setdefault("finder_compare_active", False)
    ss.setdefault("finder_compare_vins", [])
    ss.setdefault("finder_similar_selection", {"base": None, "make": False, "model": False, "price": False})

    make_options = sorted(data["Make"].dropna().unique()) if "Make" in data.columns else []
    body_options = sorted(data["Body"].dropna().unique()) if "Body" in data.columns else []

    with st.expander("Filters", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        selected_make = fc1.selectbox("Make", ["All"] + make_options)

        if selected_make != "All" and "Model" in data.columns:
            model_candidates = data.loc[data["Make"] == selected_make, "Model"].dropna().unique()
        elif "Model" in data.columns:
            model_candidates = data["Model"].dropna().unique()
        else:
            model_candidates = []
        model_options = sorted(model_candidates)
        selected_model = fc2.selectbox("Model", ["All"] + model_options)

        selected_body = fc3.selectbox("Vehicle Type", ["All"] + body_options)

        sc_col, safety_col, price_col = st.columns(3)
        sc_min, sc_max = sc_col.slider("Smart Score Range", 0, 100, (70, 100))
        min_safety = safety_col.slider("Minimum Safety Score", 0, 100, 70)

        if "PriceNum" in data.columns and data["PriceNum"].notna().any():
            price_min = int(np.floor(data["PriceNum"].dropna().min() / 1000) * 1000)
            price_max = int(np.ceil(data["PriceNum"].dropna().max() / 1000) * 1000)
            if price_min == price_max:
                selected_price = (price_min, price_max)
                price_col.caption(f"All vehicles currently priced at ${price_min:,.0f}.")
            else:
                selected_price = price_col.slider(
                    "Price Range",
                    price_min,
                    price_max,
                    (price_min, price_max),
                    step=500,
                )
        else:
            selected_price = (None, None)
            price_col.caption("No price data available in this inventory upload.")

    mask = pd.Series(True, index=data.index)
    mask &= data["Score"].between(sc_min, sc_max)
    mask &= data["SafetyScore"] >= min_safety
    if selected_make != "All" and "Make" in data.columns:
        mask &= data["Make"] == selected_make
    if selected_model != "All" and "Model" in data.columns:
        mask &= data["Model"] == selected_model
    if selected_body != "All" and "Body" in data.columns:
        mask &= data["Body"] == selected_body
    if selected_price[0] is not None and selected_price[1] is not None and "PriceNum" in data.columns:
        mask &= data["PriceNum"].between(selected_price[0], selected_price[1])

    base_filtered = data.loc[mask].copy()
    sort_cols = [c for c in ["Score", "CarfaxQualityScore"] if c in base_filtered.columns]
    if sort_cols:
        base_filtered.sort_values(sort_cols, ascending=[False] * len(sort_cols), inplace=True)

    available_vins = base_filtered["VIN"].astype(str).tolist() if "VIN" in base_filtered.columns else []

    def _clear_similar_state(vins):
        for v in vins:
            for key_name in ["make", "model", "price"]:
                key = f"similar_{v}_{key_name}"
                if key in st.session_state:
                    st.session_state[key] = False

    sim_sel = ss["finder_similar_selection"]
    if sim_sel["base"] and sim_sel["base"] not in available_vins:
        _clear_similar_state(available_vins + [sim_sel["base"]])
        ss["finder_similar_selection"] = {"base": None, "make": False, "model": False, "price": False}
        sim_sel = ss["finder_similar_selection"]

    active_update = None
    for vin in available_vins:
        make_val = st.session_state.get(f"similar_{vin}_make", False)
        model_val = st.session_state.get(f"similar_{vin}_model", False)
        price_val = st.session_state.get(f"similar_{vin}_price", False)
        if make_val or model_val or price_val:
            active_update = {"base": vin, "make": make_val, "model": model_val, "price": price_val}
            break

    if active_update:
        if active_update != sim_sel:
            ss["finder_similar_selection"] = active_update
            sim_sel = active_update
        _clear_similar_state([v for v in available_vins if v != sim_sel["base"]])
    else:
        base_vin = sim_sel.get("base")
        if base_vin and not any(
            st.session_state.get(f"similar_{base_vin}_{name}", False) for name in ["make", "model", "price"]
        ):
            _clear_similar_state(available_vins + [base_vin])
            ss["finder_similar_selection"] = {"base": None, "make": False, "model": False, "price": False}
            sim_sel = ss["finder_similar_selection"]

    filtered = base_filtered.copy()
    sim_sel = ss["finder_similar_selection"]
    similar_labels = []
    if sim_sel["base"] and any(sim_sel[k] for k in ["make", "model", "price"]):
        base_row = base_filtered[base_filtered["VIN"].astype(str) == sim_sel["base"]]
        if not base_row.empty:
            base_row = base_row.iloc[0]
            similar_mask = pd.Series(True, index=filtered.index)
            if sim_sel["make"] and "Make" in filtered.columns:
                similar_mask &= filtered["Make"] == base_row.get("Make")
                similar_labels.append("make")
            if sim_sel["model"] and "Model" in filtered.columns:
                similar_mask &= filtered["Model"] == base_row.get("Model")
                similar_labels.append("model")
            if sim_sel["price"]:
                base_price = base_row.get("PriceNum")
                if base_price is None or (isinstance(base_price, float) and np.isnan(base_price)):
                    base_price = to_num(base_row.get("Price"))
                if base_price is not None and not (isinstance(base_price, float) and np.isnan(base_price)):
                    lower = base_price * 0.8
                    upper = base_price * 1.2
                    if "PriceNum" in filtered.columns:
                        similar_mask &= filtered["PriceNum"].between(lower, upper)
                    else:
                        price_series = filtered.get("Price")
                        if price_series is not None:
                            numeric_prices = price_series.apply(to_num)
                            similar_mask &= numeric_prices.between(lower, upper)
                    similar_labels.append("price (±20%)")
            filtered = filtered.loc[similar_mask].copy()

    display_rows = []
    for idx, row in filtered.iterrows():
        vin_val = row.get("VIN")
        vin_key = str(vin_val) if vin_val is not None and not (isinstance(vin_val, float) and np.isnan(vin_val)) else f"row_{idx}"
        display_rows.append((idx, row, vin_key))
    display_vins = [vin for _, _, vin in display_rows]

    if sim_sel["base"] and similar_labels:
        st.info(
            f"Showing vehicles similar to VIN {sim_sel['base']} by {', '.join(similar_labels)}."
        )

    compare_defaults = set(ss.get("finder_compare_vins", []))
    compare_defaults = {vin for vin in compare_defaults if vin in display_vins}
    if compare_defaults != set(ss.get("finder_compare_vins", [])):
        ss["finder_compare_vins"] = list(compare_defaults)
    compare_active = ss.get("finder_compare_active", False)
    if compare_active and len(compare_defaults) < 2:
        compare_active = False
        ss["finder_compare_active"] = False

    for vin in display_vins:
        key = f"compare_{vin}"
        if key not in st.session_state:
            st.session_state[key] = vin in compare_defaults
        elif vin in compare_defaults and not st.session_state[key]:
            st.session_state[key] = True

    selected_checkbox_vins = [
        vin for vin in display_vins if st.session_state.get(f"compare_{vin}", False)
    ]

    compare_cols = st.columns([1, 1, 6])
    compare_feedback = compare_cols[2].empty()
    with compare_cols[0]:
        compare_clicked = st.button("Compare Selected", type="primary")
    with compare_cols[1]:
        clear_clicked = st.button("Clear Comparison")

    if compare_clicked:
        if len(selected_checkbox_vins) >= 2:
            ss["finder_compare_active"] = True
            ss["finder_compare_vins"] = selected_checkbox_vins
            compare_active = True
            compare_defaults = set(selected_checkbox_vins)
        else:
            compare_feedback.warning("Select at least two vehicles to compare.")
            ss["finder_compare_active"] = False
            ss["finder_compare_vins"] = []
            compare_active = False
            compare_defaults = set()

    if clear_clicked:
        for vin in display_vins:
            key = f"compare_{vin}"
            if key in st.session_state:
                st.session_state[key] = False
        ss["finder_compare_active"] = False
        ss["finder_compare_vins"] = []
        compare_active = False
        compare_defaults = set()

    compare_vins = list(compare_defaults)
    if compare_active and compare_vins:
        filtered = filtered[filtered["VIN"].astype(str).isin(compare_vins)].copy()
        st.success(f"Comparing {len(compare_vins)} vehicles. Clear comparison to show all matches.")
        display_rows = []
        for idx, row in filtered.iterrows():
            vin_val = row.get("VIN")
            vin_key = str(vin_val) if vin_val is not None and not (isinstance(vin_val, float) and np.isnan(vin_val)) else f"row_{idx}"
            display_rows.append((idx, row, vin_key))
        display_vins = [vin for _, _, vin in display_rows]

    st.divider()
    st.subheader(f"Matching Vehicles ({len(filtered)})")

    if filtered.empty:
        st.warning("No vehicles match the selected filters. Try widening your range or clearing filters.")
    else:
        cards_per_row = 3
        for start in range(0, len(display_rows), cards_per_row):
            row_chunk = display_rows[start:start + cards_per_row]
            cols = st.columns(cards_per_row)
            for col, (idx, row, vin_key) in zip(cols, row_chunk):
                with col:
                    render_vehicle_card(
                        row,
                        ai_enabled,
                        compare_checkbox_key=f"compare_{vin_key}",
                        show_similar_controls=True,
                        similar_checkbox_keys={
                            "make": f"similar_{vin_key}_make",
                            "model": f"similar_{vin_key}_model",
                            "price": f"similar_{vin_key}_price",
                        },
                        similar_defaults=ss["finder_similar_selection"].copy(),
                    )

    st.divider()
    st.subheader("Best Match Comparison")

    comparison_pool = filtered.copy()
    best_pair = None
    best_pair_key = None

    if not comparison_pool.empty:
        if {"Make", "Model"}.issubset(comparison_pool.columns):
            grouped = comparison_pool.groupby(["Make", "Model"])
            best_avg = -np.inf
            for key, grp in grouped:
                if len(grp) < 2:
                    continue
                top_two = grp.sort_values("Score", ascending=False).head(2)
                avg_score = top_two["Score"].mean()
                if avg_score > best_avg:
                    best_avg = avg_score
                    best_pair = top_two
                    best_pair_key = key
        if best_pair is None and len(comparison_pool) >= 2:
            sort_cols = [c for c in ["Score", "CarfaxQualityScore"] if c in comparison_pool.columns]
            if sort_cols:
                best_pair = comparison_pool.sort_values(sort_cols, ascending=[False] * len(sort_cols)).head(2)
            else:
                best_pair = comparison_pool.head(2)

    if best_pair is None or len(best_pair) < 2:
        st.info("Pick a make/model with at least two vehicles to see an automatic side-by-side comparison.")
    else:
        if best_pair_key:
            mk, mdl = best_pair_key
            st.caption(f"Top performers for {mk} {mdl} based on Smart Score.")
        cols = st.columns(2)
        for col, (_, row) in zip(cols, best_pair.iterrows()):
            with col:
                year_val = row.get('Year')
                if pd.notna(year_val):
                    try:
                        year_text = f"{int(float(year_val))} "
                    except (TypeError, ValueError):
                        year_text = ""
                else:
                    year_text = ""
                title_text = f"{year_text}{row.get('Make','')} {row.get('Model','')}".strip()
                st.markdown(f"### {title_text}")
                st.metric("Smart Score", f"{row['Score']:.0f}")
                st.metric("Safety Score", f"{row['SafetyScore']:.0f}")
                st.metric("Price", row.get("Price", "—"))
                st.metric("Mileage", row.get("Mileage", "—"))
                st.caption(summarize_carfax(row))

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
