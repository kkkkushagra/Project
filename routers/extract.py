from fastapi import APIRouter, UploadFile, File
import pdfplumber
from docx import Document
import os

router = APIRouter(prefix="/extract", tags=["Extract"])

def extract_text_from_pdf(file_path):
    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n"
    return text

def extract_text_from_docx(file_path):
    text = ""
    doc = Document(file_path)
    for para in doc.paragraphs:
        text += para.text + "\n"
    return text

@router.post("/")
async def extract_text(file: UploadFile = File(...)):
    file_path = f"temp_{file.filename}"
    with open(file_path, "wb") as f:
        f.write(await file.read())

    if file.filename.endswith(".pdf"):
        text = extract_text_from_pdf(file_path)
    elif file.filename.endswith(".docx"):
        text = extract_text_from_docx(file_path)
    else:
        return {"error": "Unsupported file type. Please upload PDF or DOCX."}

    os.remove(file_path)
    return {"extracted_text": text[:2000]}  # return preview (first 2000 chars)
