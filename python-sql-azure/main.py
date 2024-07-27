import datetime
from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import pandas as pd
from io import StringIO
from pydantic import BaseModel
from datetime import date

# Global counter for Workcentre ID (this part need to change to get the highest workcentre ID then +1 to it)
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
    
class Order(BaseModel):
    order_id: str
    part_name: str
    part_id: str
    part_qty: int
    order_date: date
    due_date: date
    order_last_updated: datetime

class BOM(BaseModel):
    BOM_id: str
    parent_id: str
    child_id: str
    child_qty: int
    child_leadtime: int
    BOM_last_updated: datetime

class Routing(BaseModel):
    routing_id: str
    part_name: str
    parent_id: str
    child_id: str
    operations_sequence: int
    child_qty: int
    workcentre_id: str
    process_description: str
    setup_time: int
    runtime: int
    routings_last_update: datetime

class Part(BaseModel):
    part_id: str
    part_name: str
    inventory: int
    POM: str
    UOM: str
    part_description: str
    unit_cost: float
    lead_time: int
    part_last_updated: datetime
 
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

@app.post("/BOM")
async def create_bom(bom: BOM):
    # Insert data into the database
    
    insert_query = """
    INSERT INTO dbo.dbo.BOM$(BOM_id, parent_id, child_id, child_qty, child_leadtime, BOM_last_updated)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (
        bom.BOM_id,
        bom.parent_id,
        bom.child_id,
        bom.child_qty,
        bom.child_leadtime,
        bom.BOM_last_updated
    ))
    
    connection.commit()
    cursor.close()
    connection.close()

    response = {
        "message": "BOM created successfully",
        "data": bom
    }
    return response
 
@app.get("/routings") 
async def get_routings(): 
    query = "SELECT * FROM dbo.Routings$" 
    return execute_query(query) 


@app.post("/routings")
async def create_routing(routing: Routing):
    # Insert data into the database
    insert_query = """
    INSERT INTO dbo.Routings$(routing_id, part_name, parent_id, child_id, operations_sequence, child_qty, workcentre_id, process_description, setup_time, runtime, routings_last_update)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (
        routing.routing_id,
        routing.part_name,
        routing.parent_id,
        routing.child_id,
        routing.operations_sequence,
        routing.child_qty,
        routing.workcentre_id,
        routing.process_description,
        routing.setup_time,
        routing.runtime,
        routing.routings_last_update
    ))
    
    connection.commit()
    cursor.close()
    connection.close()

    response = {
        "message": "Routing created successfully",
        "data": routing
    }
    return response
 
@app.get("/partmasterrecords") 
async def get_part_master_records(): 
    query = "SELECT * FROM dbo.Part_Master_Records$" 
    return execute_query(query) 

@app.post("/partmasterrecords")
async def create_part(part: Part):
    # Insert data into the database
    
    insert_query = """
    INSERT INTO dbo.Part_Master_Records$(part_id, part_name, inventory, POM, UOM, part_description, unit_cost, lead_time, part_last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (
        part.part_id,
        part.part_name,
        part.inventory,
        part.POM,
        part.UOM,
        part.part_description,
        part.unit_cost,
        part.lead_time,
        part.part_last_updated
    ))
    
    connection.commit()
    cursor.close()
    connection.close()

    response = {
        "message": "Part created successfully",
        "data": part
    }
    return response
 

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


@app.post("/orders")
async def create_order(order: Order):
    # Insert data into the database
    
    insert_query = """
    INSERT INTO dbo.dbo.Orders$(order_id, part_name, part_id, part_qty, order_date, due_date, order_last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_query, (
        order.order_id,
        order.part_name,
        order.part_id,
        order.part_qty,
        order.order_date,
        order.due_date,
        order.order_last_updated
    ))
    
    connection.commit()
    cursor.close()
    connection.close()

    response = {
        "message": "Order created successfully",
        "data": order
    }
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
    INSERT INTO dbo.Workcentre$(workcentre_id, workcentre_name, workcentre_description, capacity,capacity_unit, cost_rate_h,workcentre_last_updated)
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
    
