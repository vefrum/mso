from dotenv import load_dotenv 
import os 
import pyodbc 
from typing import Dict, Union 
from fastapi import FastAPI, HTTPException 
import logging 
import traceback 
import json
 
# Load environment variables 
load_dotenv() 
connection_string = os.getenv("AZURE_SQL_CONNECTIONSTRING") 
 
# Connect to the database 
try: 
    connection = pyodbc.connect(connection_string) 
    cursor = connection.cursor() 
except Exception as e: 
    logging.error(f"Error connecting to database: {e}") 
    raise 
 
# Create FastAPI app 
app = FastAPI() 
print("Server Running") 
 
# Helper function to convert cursor rows to a dictionary keyed by a unique column 
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
 
# Define endpoints 
@app.get("/BOM") 
async def get_bom(): 
    try: 
        query="SELECT * FROM dbo.BOM$"
        children = execute_query(query)
        return children 
    except Exception as e: 
        logging.error(f"Error in /BOM endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/routings") 
async def get_routings(): 
    try: 
        query="SELECT * FROM dbo.Routings$$"
        children = execute_query(query)
        return children 
    except Exception as e: 
        logging.error(f"Error in /routings endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/partmasterrecords") 
async def get_partmasterrecords(): 
    try: 
        query="SELECT * FROM dbo.Part_Master_Records$$"
        children = execute_query(query)
        return children 
    except Exception as e: 
        logging.error(f"Error in /partmasterrecords endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/orders") 
async def get_orders(): 
    try: 
        query="SELECT * FROM dbo.Orders$"
        children = execute_query(query)
        return children 
    except Exception as e: 
        logging.error(f"Error in /orders endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/workcentre") 
async def get_work_centre(): 
    try: 
        query="SELECT * FROM dbo.Work_Centre$$"
        children = execute_query(query)
        return children 
    except Exception as e: 
        logging.error(f"Error in /orders endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
