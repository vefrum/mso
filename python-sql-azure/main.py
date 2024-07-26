from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import pandas as pd
from io import StringIO
from pydantic import BaseModel
from datetime import date

class WorkCentre(BaseModel):
    work_center_name: str
    capacity_unit: str
    cost_rate_per_hour: float
    cost_rate_per_hour_base: str
    work_center_description: str
    capacity: int
    last_updated_date: date
    #workcentre_id: str  
    workcentre_id: "WC99999"
 
load_dotenv() 
connection_string = os.getenv("AZURE_SQL_CONNECTIONSTRING") 
 
# Establish the database connection 
connection = pyodbc.connect(connection_string) 
cursor = connection.cursor() 
 
# FastAPI instance 
app = FastAPI() 
print("Server Running") 
 
# Helper function to execute a query and return results as a list of dictionaries 
def execute_query(query: str):
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]

    # Create a list of dictionaries, each representing a row
    rows = []
    for row in data:
        row_dict = {columns[i]: row[i] for i in range(len(columns))}
        rows.append(row_dict)

    # Return the result in the desired format
    result = {"value": rows}

    print(type(result))
    return result
 
@app.get("/BOM") 
async def get_bom(): 
    query = "SELECT * FROM dbo.BOM$" 
    return execute_query(query) 
 
@app.get("/routings") 
async def get_routings(): 
    query = "SELECT * FROM dbo.Routings$" 
    return execute_query(query) 
 
@app.get("/partmasterrecords") 
async def get_part_master_records(): 
    query = "SELECT * FROM dbo.Part_Master_Records$" 
    return execute_query(query) 
 
@app.get("/orders") 
async def get_orders(): 
    query = "SELECT * FROM dbo.Orders$" 
    return execute_query(query) 
 
@app.get("/workcentre") 
async def get_work_centre(): 
    query = "SELECT * FROM dbo.Work_Centre$"
    df = pd.read_sql(query, connection)
  # Convert DataFrame to CSV
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)  # Rewind the buffer to the beginning
  # Create a StreamingResponse with the CSV data
    response = StreamingResponse(output, media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response

@app.post("/workcentre")
async def create_workcentre(workcentre: WorkCentre):
    print(workcentre)
    response = {
        "message": "WorkCentre created successfully",
        "data": workcentre
    }
    return response
    


