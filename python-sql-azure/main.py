from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI 
 
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
    result = [dict(zip(columns, row)) for row in data] 
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
    return execute_query(query)