import os, json, openai

openai.api_key = os.getenv("OPENAI_API_KEY", "")

CACHE_FILE = "data/story_cache.json"
if not os.path.exists("data"):
    os.makedirs("data")
if not os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "w") as f:
        json.dump({}, f)

def load_cache():
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(data):
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)

def generate_vehicle_story_ai(vin, carfax_text):
    cache = load_cache()
    if vin in cache:
        return cache[vin]

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            temperature=0.6,
            messages=[
                {"role": "system", "content": (
                    "You are an expert automotive storyteller for a dealership. "
                    "Summarize Carfax reports into 2–3 compelling sentences that highlight maintenance, "
                    "ownership stability, and condition. Be concise, trustworthy, and positive without exaggerating."
                )},
                {"role": "user", "content": f"VIN: {vin}\nCarfax Report:\n{carfax_text}"}
            ]
        )
        story = response["choices"][0]["message"]["content"].strip()
        cache[vin] = story
        save_cache(cache)
        return story

    except Exception as e:
        return f"(AI story unavailable: {e})"
