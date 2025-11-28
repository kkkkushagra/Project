import math
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# ---------------------------------------------
# 🔹 Helper: Clean JSON (avoid NaN/Inf errors)
# ---------------------------------------------
def sanitize_for_json(data):
    if isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return data

    if isinstance(data, dict):
        return {k: sanitize_for_json(v) for k, v in data.items()}

    if isinstance(data, list):
        return [sanitize_for_json(v) for v in data]

    if isinstance(data, np.ndarray):
        return sanitize_for_json(data.tolist())

    return data


# ---------------------------------------------
# 🔹 Load clause playbook
# ---------------------------------------------
try:
    clause_playbook = pd.read_csv("clause_playbook.csv")
except Exception as e:
    raise RuntimeError(f"❌ Failed to load clause_playbook.csv: {e}")

if "standard_clause" not in clause_playbook.columns:
    raise RuntimeError("❌ Missing column: 'standard_clause' in clause_playbook.csv")

# ---------------------------------------------
# 🔹 Load LegalBERT model ONCE during startup
# ---------------------------------------------
print("🔄 Loading LegalBERT model... Please wait...")
model = SentenceTransformer("nlpaueb/legal-bert-base-uncased")
print("✅ LegalBERT loaded successfully.")

# Precompute embeddings
print("🔄 Computing embeddings for clause playbook...")
clause_embeddings = model.encode(
    clause_playbook["standard_clause"].tolist(),
    convert_to_numpy=True,
    show_progress_bar=True
)
clause_embeddings = np.nan_to_num(clause_embeddings)
print("✅ Clause embeddings generated successfully.")


# ---------------------------------------------
# 🔹 Initialize FastAPI App
# ---------------------------------------------
app = FastAPI(
    title="AI Contract Review & Redlining Backend",
    description="Backend powered by LegalBERT + AI Clause Suggestion Models",
    version="3.0.0"
)

# ---------------------------------------------
# 🔹 Enable CORS (frontend → backend connection)
# ---------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],               # allow frontend access
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------
# 🔹 Import Routers (UPLOAD, EXTRACT, REDLINE)
# ---------------------------------------------
from routers import upload, extract, predict_clause, redline

app.include_router(upload.router)
app.include_router(extract.router)
app.include_router(predict_clause.router)   # /predict_clause/*
app.include_router(redline.router)          # /redline/*


# ---------------------------------------------
# 🔹 Root endpoint
# ---------------------------------------------
@app.get("/")
def root():
    return {"status": "Backend is running successfully 🚀"}


# ---------------------------------------------
# 🔹 Text clause prediction endpoint (safe test)
#  (ONLY for debugging → Frontend uses /redline/analyze)
# ---------------------------------------------
@app.post("/predict_clause_local/")
async def predict_clause_local(data: dict):
    text = data.get("text", "")
    if not text.strip():
        raise HTTPException(status_code=400, detail="Text input is required.")

    query_embedding = model.encode([text], convert_to_numpy=True)
    query_embedding = np.nan_to_num(query_embedding)

    similarities = cosine_similarity(query_embedding, clause_embeddings)
    similarities = np.nan_to_num(similarities)

    best_idx = int(np.argmax(similarities))
    best_row = clause_playbook.iloc[best_idx]

    return JSONResponse(content=sanitize_for_json({
        "matched_clause": str(best_row["standard_clause"]),
        "risk_level": str(best_row["Risk_Level"]),
        "action_required": str(best_row["Action_Required"]),
        "similarity_score": float(similarities[0][best_idx])
    }))
