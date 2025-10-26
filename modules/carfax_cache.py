import os, json, time
import streamlit as st

CACHE_FILE = "carfax_cache.json"

def load_cache() -> dict:
    """Load cached Carfax data from disk or init empty dict."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Could not read cache, starting fresh: {e}")
            return {}
    return {}

def save_cache(cache: dict) -> None:
    """Write cache back to disk (safe overwrite)."""
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f, indent=2)
    os.replace(tmp, CACHE_FILE)

def get_cached(vin: str, cache: dict):
    """Return VIN record if already parsed."""
    return cache.get(vin)

def upsert_cache(vin: str, data: dict, cache: dict) -> None:
    """Add or refresh a VIN record and persist."""
    data = dict(data or {})
    data["last_updated"] = time.strftime("%Y-%m-%d")
    cache[vin] = data
    save_cache(cache)
