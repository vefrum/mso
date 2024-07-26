from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import pandas as pd
from io import StringIO
from pydantic import BaseModel
from datetime import date

# Global counter for Workcentre ID
workcentre_counter = 5

class WorkCentre(BaseModel):
    workcentre_name: str
    capacity_unit: str
    cost_rate_h: float
    #cost_rate_per_hour_base: str
    workcentre_description: str
    capacity: int
    last_updated_date: date
    workcentre_id: str = None
    


 
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
    df = pd.read_sql(query, connection)
  # Convert DataFrame to CSV
    output = StringIO()
    df.to_csv(output, index=False)
    output.seek(0)  # Rewind the buffer to the beginning
  # Create a StreamingResponse with the CSV data
    response = StreamingResponse(output, media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export_orders.csv"
    return response
 
@app.get("/workcentre") 
async def get_work_centre(): 
    query = "SELECT * FROM dbo.Workcentre$"
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
    
    global workcentre_counter
    
    # Generate the Workcentre ID
    workcentre_id = f"WC{str(workcentre_counter).zfill(3)}"
    workcentre.workcentre_id = workcentre_id
    workcentre_counter += 1
    
    insert_query = """
    INSERT INTO dbo.Work_Centre$ (workcentre_id, workcentre_name, workcentre_description, capacity,capacity_unit, cost_rate_h, last_updated_date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (
        workcentre.workcentre_id,
        workcentre.workcentre_name, 
        workcentre.workcentre_description,
        workcentre.capacity,
        workcentre.capacity_unit,
       
        workcentre.cost_rate_h,
        workcentre.last_updated_date
    ))
    
    connection.commit()
    cursor.close()
    connection.close()

    response = {
        "message": "WorkCentre created successfully",
        "data": workcentre
    }
    return response
    
