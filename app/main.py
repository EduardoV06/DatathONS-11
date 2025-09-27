from fastapi import FastAPI
from pydantic import BaseModel
import requests
import pandas as pd
from io import BytesIO
from docling.document_converter import DocumentConverter
import re

app = FastAPI()

class PDFRequest(BaseModel):
    url: str

class DatasetDictRequest(BaseModel):
    dataset_name: str

@app.post("/process-pdf/")
async def process_pdf(request: PDFRequest):
    response = requests.get(request.url)
    pdf_bytes = response.content

    converter = DocumentConverter()
    doc = converter.convert(request.url).document
    markdown = doc.export_to_markdown()

    pattern = r"\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|"
    matches = re.findall(pattern, markdown)

    header = [col.strip() for col in matches[0]]
    data = [tuple(col.strip() for col in row) for row in matches[1:]]

    df = pd.DataFrame(data, columns=header)

    return df.to_dict(orient="records")

@app.post("/fetch-dict/")
async def fetch_dict(request: DatasetDictRequest):
    ...