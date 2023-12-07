from fastapi import FastAPI, UploadFile, HTTPException
from typing import Dict, Any, List
import csv
from io import StringIO
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

app = FastAPI()

data = []
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/api/python")
async def root():
    return {"message": "Hello from fastAPI backend"}

@app.get("/api/dataset")
async def getData():
    return data

@app.post("/api/dataset")
def upload(json_data: List[Dict[Any, Any]]):
    for i in json_data:
        data.append(i)

@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile):
    if not file:
        raise HTTPException(status_code=400, detail="No file provided")

    try:
        contents = file.file.read().decode('utf-8')
        file.file.close()

        # Assuming CSV file has a header row
        rows = csv.DictReader(StringIO(contents))

        # Upload data to BigQuery
        project_id = "your-google-cloud-project-id"
        dataset_id = "your-bigquery-dataset-id"
        table_id = "your-bigquery-table-id"

        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        rows_to_insert = [dict(row) for row in rows]
        errors = client.insert_rows(table, rows_to_insert)

        if errors:
            raise HTTPException(status_code=500, detail=f"Failed to insert rows: {errors}")

        return {"message": "Data uploaded successfully"}

except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
# def upload(file: UploadFile):  # Initialize data as a dictionary

#     if not file:
#         return {"message": "No upload file sent"}
#     try:
#         contents = file.file.read()
#         print(contents)

#         # with StringIO(contents.decode('utf-8')) as buffer:
#         #     csvReader = csv.DictReader(buffer)

#         #     for row in csvReader:
#         #         # key = row.get('Id')  # Assuming a column named 'Id' to be the primary key
#         #         data.append(row)

#     except Exception as e:
#         return {"error": f"An error occurred: {str(e)}"}

#     finally:
#         file.file.close()

#     print(data)
#     return data
