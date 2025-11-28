# routers/upload.py
from fastapi import APIRouter, UploadFile, File
import os

router = APIRouter(prefix="/upload", tags=["Upload"])

UPLOAD_FOLDER = "uploaded_contracts"

# Create folder if not exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@router.post("/")
async def upload_contract(file: UploadFile = File(...)):
    try:
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())

        return {"message": "File uploaded successfully", "filename": file.filename, "path": os.path.abspath(file_path)}
    except Exception as e:
        return {"error": f"File upload failed: {str(e)}"}
