import json, re, pandas as pd
import streamlit as st
from openai import OpenAI

client = None
try:
    client = OpenAI(api_key=st.secrets.get("OPENAI_API_KEY"))
except Exception:
    client = None

@st.cache_data(ttl=3600, show_spinner=False)
def interpret_query(query: str) -> dict:
    """Uses OpenAI to interpret natural query text into structured filters."""
    if not client:
        # fallback if no API key
        q = query.lower()
        d = {}
        if "suv" in q: d["Body"] = "SUV"
        if "truck" in q: d["Body"] = "Truck"
        if "sedan" in q: d["Body"] = "Sedan"
        m = re.search(r"under\s*\$?(\d{2,5})", q)
        if m: d["PriceMax"] = int(m.group(1))
        m = re.search(r"over\s*\$?(\d{2,5})", q)
        if m: d["PriceMin"] = int(m.group(1))
        m = re.search(r"under\s*(\d{2,5})k\s*miles", q)
        if m: d["MileageMax"] = int(m.group(1))*1000
        if "awd" in q: d["DriveTrain"] = "AWD"
        return d

    prompt = f"""
You are a car sales assistant. Interpret this customer's query: "{query}".
Return structured JSON only with keys you find, from:
["Body","PriceMax","PriceMin","MileageMax","MileageMin","Make","Model","DriveTrain"].
Example:
{{"Body":"SUV","PriceMax":25000,"DriveTrain":"AWD"}}
If unclear, omit the key.
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":"Output only valid JSON."},
                      {"role":"user","content":prompt}],
            temperature=0.1, max_tokens=150
        )
        txt = resp.choices[0].message.content.strip()
        return json.loads(txt)
    except:
        return {}
