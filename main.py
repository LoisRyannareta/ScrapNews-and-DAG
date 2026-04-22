from fastapi import FastAPI
import json

app = FastAPI()

# =========================
# LOAD DATA
# =========================
def load_data():
    with open("wired_articles.json", "r", encoding="utf-8") as f:
        return json.load(f)

# =========================
# ROOT
# =========================
@app.get("/")
def home():
    return {"message": "API Wired Running"}

# =========================
# ARTICLES
# =========================
@app.get("/articles")
def get_articles():
    data = load_data()

    return {
        "status": "success",
        "total": len(data),
        "data": data
    }