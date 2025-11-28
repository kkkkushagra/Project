import math
import numpy as np
import torch
import pandas as pd
from fastapi import APIRouter, HTTPException
from sentence_transformers import SentenceTransformer, util

router = APIRouter(prefix="/predict_clause", tags=["Predict Clause"])

# Function to clean invalid float values (NaN / Inf)
def clean_floats(obj):
    """Recursively replace NaN and Inf with None."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, list):
        return [clean_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: clean_floats(v) for k, v in obj.items()}
    elif isinstance(obj, np.ndarray):
        return clean_floats(obj.tolist())
    return obj


# Load clause playbook safely
try:
    df = pd.read_csv("clause_playbook.csv")
except Exception as e:
    raise RuntimeError(f"Failed to load clause playbook: {e}")

required_cols = {"standard_clause", "Risk_Level", "Action_Required"}
if not required_cols.issubset(df.columns):
    raise RuntimeError(f"CSV missing required columns: {required_cols - set(df.columns)}")

df = df.dropna(subset=["standard_clause"]).reset_index(drop=True)

# Load LegalBERT model
model = SentenceTransformer("nlpaueb/legal-bert-base-uncased")

# Precompute embeddings for all standard clauses
clause_embeddings = model.encode(df["standard_clause"].tolist(), convert_to_tensor=True)


@router.post("/")
async def predict_clause(data: dict):
    """Predict the most similar clause from playbook."""
    text = data.get("text", "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text field is required.")

    # Encode input text
    query_embedding = model.encode(text, convert_to_tensor=True)

    # Compute cosine similarity safely
    with torch.no_grad():
        similarity_scores = util.pytorch_cos_sim(query_embedding, clause_embeddings)[0]
        # Replace NaN / Inf with safe values
        similarity_scores = torch.nan_to_num(similarity_scores, nan=-1.0, posinf=1.0, neginf=-1.0)

    # Get best match
    best_idx = int(similarity_scores.argmax())
    best_score = float(similarity_scores[best_idx])

    # Handle invalid cases
    if math.isnan(best_score) or best_score < 0:
        raise HTTPException(status_code=500, detail="Invalid similarity score. Please check input text.")

    result = {
        "matched_clause": df["standard_clause"].iloc[best_idx],
        "risk_level": df["Risk_Level"].iloc[best_idx],
        "action_required": df["Action_Required"].iloc[best_idx],
        "similarity_score": round(best_score, 3)
    }

    # Clean and return
    return clean_floats(result)
