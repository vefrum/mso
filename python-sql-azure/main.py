from dotenv import load_dotenv 
import os 
import pyodbc 
from typing import Dict, Union 
from fastapi import FastAPI, HTTPException 
import logging 
import traceback 
 
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
def rows_to_dict(cursor, key_column: str) -> Dict[Union[str, int], Dict[str, Union[str, int, float]]]: 
    columns = [column[0] for column in cursor.description] 
    result = {} 
    for row in cursor.fetchall(): 
        row_dict = dict(zip(columns, row)) 
        result[row_dict[key_column]] = row_dict 
    return result 
 
# Define endpoints 
@app.get("/BOM") 
async def get_bom(): 
    try: 
        cursor.execute("SELECT * FROM dbo.BOM$") 
        children = rows_to_dict(cursor, "bom_id")  # Replace "bom_id" with the actual unique column name 
        return children 
    except Exception as e: 
        logging.error(f"Error in /BOM endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/routings") 
async def get_routings(): 
    try: 
        cursor.execute("SELECT * FROM dbo.Routings$$") 
        children = rows_to_dict(cursor, "routing_id")  # Replace "routing_id" with the actual unique column name 
        return children 
    except Exception as e: 
        logging.error(f"Error in /routings endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/partmasterrecords") 
async def get_partmasterrecords(): 
    try: 
        cursor.execute("SELECT * FROM dbo.Part_Master_Records$$") 
        children = rows_to_dict(cursor, "part_id")  # Replace "part_id" with the actual unique column name 
        return children 
    except Exception as e: 
        logging.error(f"Error in /partmasterrecords endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/orders") 
async def get_orders(): 
    try: 
        cursor.execute("SELECT * FROM dbo.Orders$") 
        children = rows_to_dict(cursor, "order_id")  # Replace "order_id" with the actual unique column name 
        return children 
    except Exception as e: 
        logging.error(f"Error in /orders endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error") 
 
@app.get("/workcentre") 
async def get_workcentre(): 
    try: 
        cursor.execute("SELECT * FROM dbo.Work_Centre$$") 
        children = rows_to_dict(cursor, "workcentre_id")  # Replace "workcentre_id" with the actual unique column name 
        return {"count":91,"name":"nathaniel","country":[{"country_id":"NG","probability":0.22877134457826348},{"country_id":"NE","probability":0.1335755695032996},{"country_id":"GH","probability":0.11050342568000239},{"country_id":"TT","probability":0.04007267085098988},{"country_id":"ID","probability":0.0377036420696571}]}
    except Exception as e: 
        logging.error(f"Error in /workcentre endpoint: {e}") 
        logging.error(traceback.format_exc()) 
        raise HTTPException(status_code=500, detail="Internal Server Error")
