import datetime
from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import StreamingResponse
import pandas as pd
import json
import requests
from io import StringIO
from pydantic import BaseModel
from datetime import date, datetime
from typing import List


# Global counter for Workcentre ID (this part need to change to get the highest workcentre ID then +1 to it)
workcentre_counter = 5
bom_counter = 470
order_counter = 1000
part_counter = 145  
routing_counter = 913

class WorkCentre(BaseModel):
    workcentre_name: str
    capacity_unit: str
    cost_rate_h: float
    workcentre_description: str
    capacity: int
    workcentre_last_updated: datetime
    workcentre_id: str = None
    status: str = None
    
class Order(BaseModel):
    order_id: str = None
    part_id: str
    part_qty: int
    order_date: date
    due_date: date
    order_last_updated: datetime

class BOM(BaseModel):
    BOM_id: str = None
    part_id: str
    child_id: str
    child_qty: float
    child_leadtime: float
    BOM_last_updated: datetime
    status: str = None

class Routing(BaseModel):
    routing_id: str = None 
    BOM_id: str
    operations_sequence: int
    workcentre_id: str
    process_description: str
    setup_time: int
    runtime: int
    routings_last_update: datetime
    status: str = None 

class Part(BaseModel):
    part_id: str = None
    part_name: str
    inventory: int
    POM: str
    UOM: str
    part_description: str
    unit_cost: float
    lead_time: int
    part_last_updated: datetime
    status: str = None

class PartIDUpdate(BaseModel):
    old_part_id: str
    new_part_id: str

class UpdateBOMRequest(BaseModel):
    bom: BOM
    routing: List[Routing]
 
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

    global bom_counter

    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "workcentre_id_exists": "BOM_id already exists and cannot be added",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            return {"error": error_messages["connection_unavailable"]}

        # Check if the cursor is initialized
        if cursor is None:
            return {"error": error_messages["cursor_uninitialized"]}

        # Fetch the latest BOM_id from the database
        query = "SELECT TOP 1 BOM_id FROM dbo.BOM$ ORDER BY BOM_id DESC"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            latest_bom_id = result[0]  # e.g., "B470"
            # Extract the integer part of the BOM_id
            bom_counter = int(latest_bom_id[1:])  # Ignore the "B" prefix
        else:
            bom_counter = 0  # Default to 0 if no records are found

        # Increment the counter for the new BOM_id
        bom_counter += 1
        BOM_id = f"B{str(bom_counter).zfill(3)}"
        bom.BOM_id = BOM_id

        # Check if BOM_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.BOM$ WHERE BOM_id = ?"
        cursor.execute(check_query, (bom.BOM_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="BOM_id already exists and cannot be added")
        
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.BOM$ (BOM_id, part_id, child_id, child_qty, child_leadtime, BOM_last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            bom.BOM_id,
            bom.part_id,
            bom.child_id,
            bom.child_qty,
            bom.child_leadtime,
            bom.BOM_last_updated
        ))
        
        connection.commit()

        response = {
            "message": "BOM created successfully",
            "data": bom
        }
        return response    

    except pyodbc.IntegrityError:
        return {"error": error_messages["integrity_error"]}
    except pyodbc.DatabaseError as e:
        return {"error": f"{error_messages['database_error']}: {str(e)}"}
    except Exception as e:
        return {"error": f"{error_messages['unexpected_error']}: {str(e)}"}


# @app.delete("/BOM/{bom_id}")
# async def delete_bom(bom: BOM):

#     global bom_counter
    
#     # Decrease BOM ID
#     BOM_id = f"B{str(bom_counter).zfill(3)}"
#     bom.BOM_id = BOM_id
#     bom_counter -= 1

#     delete_query = "DELETE FROM dbo.BOM$ WHERE BOM_id = ?"
#     cursor.execute(delete_query, (BOM_id,))
#     if cursor.rowcount == 0:
#         raise HTTPException(status_code=404, detail="BOM not found")
#     connection.commit()

#     response = {
#         "message": "BOM deleted successfully",
#         "BOM_id": BOM_id
#     }
#     return response

@app.delete("/bom/{BOM_id}")
async def delete_bom(BOM_id: str):

    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "referenced_entry": "Cannot delete BOM entry because it is referenced by Routings$ table.",
        "bom_not_found": "BOM not found",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            raise HTTPException(status_code=503, detail=error_messages["connection_unavailable"])

        # Check if the cursor is initialized
        if cursor is None:
            raise HTTPException(status_code=503, detail=error_messages["cursor_uninitialized"])

        # Check for referencing entries in dbo.Routings$
        check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE BOM_id = ?"
        cursor.execute(check_query, (BOM_id,))
        referencing_count = cursor.fetchone()[0]

        if referencing_count > 0:
            raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])

        # Delete BOM entry
        delete_query = "DELETE FROM dbo.BOM$ WHERE BOM_id = ?"
        cursor.execute(delete_query, (BOM_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=error_messages["bom_not_found"])
        
        # Commit the transaction
        connection.commit()

        # If no exceptions, return success response
        response = {
            "message": error_messages["unexpected_error"],
            "BOM_id": BOM_id
        }
        return response

    except pyodbc.IntegrityError:
        raise HTTPException(status_code=400, detail=error_messages["integrity_error"])
    except pyodbc.DatabaseError:
        raise HTTPException(status_code=500, detail=error_messages["database_error"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{error_messages['unexpected_error']}: {str(e)}")

@app.put("/BOM/{BOM_id}")
async def update_bom(BOM_id: str, update_request: UpdateBOMRequest = Body(...)):

    bom = update_request.bom
    routing = update_request.routing[0]

    # Fetch the last BOM_id and increment it
    last_id_query = "SELECT TOP 1 BOM_id FROM dbo.BOM$ ORDER BY CAST(SUBSTRING(BOM_id, 2, LEN(BOM_id)-1) AS INT) DESC"
    cursor.execute(last_id_query)
    last_id_row = cursor.fetchone()

    if not last_id_row:
        # If no existing BOMs, start with a base ID, e.g., "B001"
        new_BOM_id = "B001"
    else:
        last_id = last_id_row[0]
        # Assuming the format "B###", extract the numeric part, increment, and reformat
        prefix, number = last_id[0], int(last_id[1:])
        new_BOM_id = f"{prefix}{str(number + 1).zfill(3)}"

    update_status_query = """
    UPDATE dbo.BOM$
    SET status = 'inactive'
    WHERE BOM_id = ? 
    """
    cursor.execute(update_status_query, (BOM_id,))

    bom.BOM_id = new_BOM_id

    insert_query = """
    INSERT INTO dbo.BOM$ (BOM_id, part_id, child_id, child_qty, child_leadtime, BOM_last_updated,status)
    VALUES (?, ?, ?, ?, ?, ?,?)
    """
    cursor.execute(insert_query, (
        bom.BOM_id,
        bom.part_id,
        bom.child_id,
        bom.child_qty,
        bom.child_leadtime,
        bom.BOM_last_updated,
        'active'
    ))
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"BOM_id {BOM_id} not found")
    
    connection.commit()

    last_routing_id_query = "SELECT TOP 1 routing_id FROM dbo.Routings$ ORDER BY CAST(SUBSTRING(routing_id, 2, LEN(routing_id)-1) AS INT) DESC"
    cursor.execute(last_routing_id_query)
    last_routing_id_row = cursor.fetchone()

    if not last_routing_id_row:
        new_routing_id = "R001"
    else:
        last_routing_id = last_routing_id_row[0]
        prefix, number = last_routing_id[0], int(last_routing_id[1:])
        new_routing_id = f"{prefix}{str(number + 1).zfill(3)}"

    # fetch_routing_query = "SELECT * FROM dbo.Routings$ WHERE BOM_id = ?"
    # cursor.execute(fetch_routing_query, (BOM_id,))
    # routing_row = cursor.fetchone()

    # if not routing_row:
    #     raise HTTPException(status_code=404, detail=f"Routing not found for BOM_id {BOM_id}")

    # Assuming routing_row contains necessary fields for the new Routing
    # (existing_routing_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update, _) = routing_row

    # update_routing_status_query = """
    # UPDATE dbo.Routings$
    # SET status = 'inactive'
    # WHERE routing_id = ? 
    # """
    # cursor.execute(update_routing_status_query, (routing.routing_id,))

    insert_routing_query = """
    INSERT INTO dbo.Routings$ (routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(insert_routing_query, (
        new_routing_id,
        bom.BOM_id,
        routing.operations_sequence,
        routing.workcentre_id,
        routing.process_description,
        routing.setup_time,
        routing.runtime,
        routing.routings_last_update,
        'active'
    ))

    connection.commit()
    response = {
        "message": "BOM and Routing updated successfully with new BOM_id and routing_id",
        "BOM_data": {
            "BOM_id": bom.BOM_id,
            "part_id": bom.part_id,
            "child_id": bom.child_id,
            "child_qty": bom.child_qty,
            "child_leadtime": bom.child_leadtime,
            "BOM_last_updated": bom.BOM_last_updated,
            "status": 'active'
        },
        "Routing_data": {
            "routing_id": new_routing_id,
            "BOM_id": bom.BOM_id,
            "operations_sequence": routing.operations_sequence,
            "workcentre_id": routing.workcentre_id,
            "process_description": routing.process_description,
            "setup_time": routing.setup_time, 
            "runtime": routing.runtime,
            "routings_last_update": routing.routings_last_update,
            "status": 'active'
        }
    }
    return response
 
@app.get("/routings") 
async def get_routings(): 
    query = "SELECT * FROM dbo.Routings$" 
    return execute_query(query) 

@app.post("/routings")
async def create_routing(routing: Routing):

    global routing_counter

    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "workcentre_id_exists": "routing_id already exists and cannot be added",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            return {"error": error_messages["connection_unavailable"]}

        # Check if the cursor is initialized
        if cursor is None:
            return {"error": error_messages["cursor_uninitialized"]}

        # Fetch the latest routing_id from the database
        query = "SELECT TOP 1 routing_id FROM dbo.Routings$ ORDER BY routing_id DESC"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            latest_routing_id = result[0]  # e.g., "R913"
            # Extract the integer part of the routing_id
            routing_counter = int(latest_routing_id[1:])  # Ignore the "R" prefix
        else:
            routing_counter = 0  # Default to 0 if no records are found

        # Increment the counter for the new routing_id
        routing_counter += 1
        routing_id = f"R{str(routing_counter).zfill(3)}"
        routing.routing_id = routing_id

        # Check if routing_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE routing_id = ?"
        cursor.execute(check_query, (routing.routing_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="routing_id already exists and cannot be added")
        
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Routings$ (routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            routing.routing_id,
            routing.BOM_id,
            routing.operations_sequence,
            routing.workcentre_id,
            routing.process_description,
            routing.setup_time,
            routing.runtime,
            routing.routings_last_update
        ))
    
        connection.commit()

        response = {
            "message": "Routing created successfully",
            "data": routing
        }
        return response      

    except pyodbc.IntegrityError:
        return {"error": error_messages["integrity_error"]}
    except pyodbc.DatabaseError as e:
        return {"error": f"{error_messages['database_error']}: {str(e)}"}
    except Exception as e:
        return {"error": f"{error_messages['unexpected_error']}: {str(e)}"}

    
@app.put("/routings/{routing_id}")
async def update_routing(routing_id: str, routing: Routing):
  

    last_id_query = "SELECT TOP 1 routing_id FROM dbo.Routings$ ORDER BY CAST(SUBSTRING(routing_id, 2, LEN(routing_id)-1) AS INT) DESC"
    cursor.execute(last_id_query)
    last_id_row = cursor.fetchone()

    if not last_id_row:
        # If no existing BOMs, start with a base ID, e.g., "B001"
        new_routing_id = "R001"
    else:
        last_id = last_id_row[0]
        # Assuming the format "B###", extract the numeric part, increment, and reformat
        prefix, number = last_id[0], int(last_id[1:])
        new_routing_id = f"{prefix}{str(number + 1).zfill(3)}"

    update_status_query = """
    UPDATE dbo.Routings$
    SET status = 'inactive'
    WHERE routing_id = ? 
    """

    cursor.execute(update_status_query, (routing_id,))

    routing.routing_id = new_routing_id
    routing.status = "active"

    insert_query = """
    INSERT INTO dbo.Routings$ (routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update,status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?,?)
    """
    cursor.execute(insert_query, (
        routing.routing_id,
        routing.BOM_id,
        routing.operations_sequence,
        routing.workcentre_id,
        routing.process_description,
        routing.setup_time,
        routing.runtime,
        routing.routings_last_update,
        routing.status
    ))

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"routing_id {routing_id} not found")
    
    connection.commit()
    response = {
        "message": "Routings updated successfully with new routing_id",
        "data": {
            "routing_id": routing.routing_id,
            "BOM_id": routing.BOM_id,
            "operations_sequence": routing.operations_sequence,
            "workcentre_id": routing.workcentre_id,
            "process_description": routing.process_description,
            "setup_time": routing.setup_time, 
            "runtime": routing.runtime,
            "routings_last_update": routing.routings_last_update,
            "status": 'active'
        }
    }
    return response
 
@app.delete("/routing/{routing_id}")
async def delete_routing(routing_id: str):
    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "referenced_entry": "Cannot delete BOM entry because it is referenced by Routings$ table.",
        "bom_not_found": "BOM not found",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            raise HTTPException(status_code=503, detail=error_messages["connection_unavailable"])

        # Check if the cursor is initialized
        if cursor is None:
            raise HTTPException(status_code=503, detail=error_messages["cursor_uninitialized"])

        # Check if the routing_id exists in dbo.Routings$
        check_routing_query = "SELECT BOM_id FROM dbo.Routings$ WHERE routing_id = ?"
        cursor.execute(check_routing_query, (routing_id,))
        routing_row = cursor.fetchone()[0]

        if not routing_row:
            raise HTTPException(status_code=404, detail=error_messages["routing_not_found"])

        BOM_id = routing_row[0]

        # Check if the BOM_id exists in dbo.BOM$
        check_bom_query = "SELECT COUNT(*) FROM dbo.BOM$ WHERE BOM_id = ?"
        cursor.execute(check_bom_query, (BOM_id,))
        bom_count = cursor.fetchone()[0]

        if bom_count > 0:
            raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])

        # Delete BOM entry
        delete_query = "DELETE FROM dbo.Routing$ WHERE routing_id = ?"
        cursor.execute(delete_query, (routing_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=error_messages["routing_not_found"])
        
        # Commit the transaction
        connection.commit()

        # If no exceptions, return success response
        response = {
            "message": error_messages["unexpected_error"],
            "routing_id": routing_id
        }
        return response

    except pyodbc.IntegrityError:
        raise HTTPException(status_code=400, detail=error_messages["integrity_error"])
    except pyodbc.DatabaseError:
        raise HTTPException(status_code=500, detail=error_messages["database_error"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{error_messages['unexpected_error']}: {str(e)}")

    # delete_query = "DELETE FROM dbo.Routings$ WHERE routing_id = ?"
    # cursor.execute(delete_query, routing_id)
    # if cursor.rowcount == 0:
    #     raise HTTPException(status_code=404, detail="Routing not found")
    # connection.commit()

    # response = {
    #     "message": "Routing deleted successfully",
    #     "routing_id": routing_id
    # }
    # return response

@app.get("/partmasterrecords") 
async def get_part_master_records(): 
    query = "SELECT * FROM dbo.Part_Master_Records$" 
    return execute_query(query) 

@app.post("/partmasterrecords")
async def create_part(part: Part):

    global part_counter

    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "workcentre_id_exists": "part_id already exists and cannot be added",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            return {"error": error_messages["connection_unavailable"]}

        # Check if the cursor is initialized
        if cursor is None:
            return {"error": error_messages["cursor_uninitialized"]}

        # Fetch the latest part_id from the database
        query = "SELECT TOP 1 part_id FROM dbo.Part_Master_Records$ ORDER BY part_id DESC"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            latest_part_id = result[0]  # e.g., "WC145"
            # Extract the integer part of the part_id
            part_counter = int(latest_part_id[1:])  # Ignore the "WC" prefix
        else:
            part_counter = 0  # Default to 0 if no records are found

        # Increment the counter for the new part_id
        part_counter += 1
        part_id = f"P{str(part_counter).zfill(3)}"
        part.part_id = part_id
        
        # Check if the part_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.Part_Master_Records$ WHERE part_id = ?"
        cursor.execute(check_query, (part.part_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="part_id already exists and cannot be added")
        
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Part_Master_Records$ (part_id, part_name, inventory, POM, UOM, part_description, unit_cost, lead_time, part_last_updated)
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

        response = {
            "message": "Part created successfully",
            "data": part
        }
        return response

    except pyodbc.IntegrityError:
        return {"error": error_messages["integrity_error"]}
    except pyodbc.DatabaseError as e:
        return {"error": f"{error_messages['database_error']}: {str(e)}"}
    except Exception as e:
        return {"error": f"{error_messages['unexpected_error']}: {str(e)}"}

    

@app.put("/partmasterrecords/{part_id}")
async def update_part(part_id: str, part: Part):

    last_id_query = "SELECT TOP 1 part_id FROM dbo.Part_Master_Records$ ORDER BY CAST(SUBSTRING(part_id, 2, LEN(part_id)-1) AS INT) DESC"
    cursor.execute(last_id_query)
    last_id_row = cursor.fetchone()

    if not last_id_row:
        # If no existing parts, start with a base ID, e.g., "P001"
        new_part_id = "P001"
    else:
        last_id = last_id_row[0]
        # Assuming the format "P###", extract the numeric part, increment, and reformat
        prefix, number = last_id[0], int(last_id[1:])
        new_part_id = f"{prefix}{str(number + 1).zfill(3)}"

    # Set the old part record to inactive (if applicable)
    update_status_query = """
    UPDATE dbo.Part_Master_Records$
    SET status = 'inactive'
    WHERE part_id = ?
    """
    cursor.execute(update_status_query, (part_id,))

    # Set new part_id and status
    part.part_id = new_part_id
    part.status = "active"

    # Insert new part record with updated details
    insert_query = """
    INSERT INTO dbo.Part_Master_Records$ (part_id, part_name, inventory, POM, UOM, part_description, unit_cost, lead_time, part_last_updated, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        part.part_last_updated,
        part.status
    ))

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"part_id {part_id} not found")

    connection.commit()
    response = {
        "message": "Part Master Records updated successfully with new part_id",
        "data": {
            "part_id": part.part_id,
            "part_name": part.part_name,
            "inventory": part.inventory,
            "POM": part.POM,
            "UOM": part.UOM,
            "part_description": part.part_description,
            "unit_cost": part.unit_cost,
            "lead_time": part.lead_time,
            "part_last_updated": part.part_last_updated,
            "status": part.status
        }
    }
    return response


@app.delete("/partmasterrecords/{part_id}")
async def delete_part(part_id: str):

    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "referenced_entry": "Cannot delete BOM entry because it is referenced by Routings$ table.",
        "bom_not_found": "BOM not found",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            raise HTTPException(status_code=503, detail=error_messages["connection_unavailable"])

        # Check if the cursor is initialized
        if cursor is None:
            raise HTTPException(status_code=503, detail=error_messages["cursor_uninitialized"])

        # Check for referencing entries in dbo.BOM$
        check_query = "SELECT COUNT(*) FROM dbo.BOM$ WHERE part_id = ?"
        cursor.execute(check_query, (part_id,))
        referencing_count = cursor.fetchone()[0]

        if referencing_count > 0:
            raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])

        # Delete part entry
        delete_query = "DELETE FROM dbo.BOM$ WHERE part_id = ?"
        cursor.execute(delete_query, (part_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=error_messages["routing_not_found"])
        
        # Commit the transaction
        connection.commit()

        # If no exceptions, return success response
        response = {
            "message": error_messages["unexpected_error"],
            "part_id": part_id
        }
        return response

    except pyodbc.IntegrityError:
        raise HTTPException(status_code=400, detail=error_messages["integrity_error"])
    except pyodbc.DatabaseError:
        raise HTTPException(status_code=500, detail=error_messages["database_error"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{error_messages['unexpected_error']}: {str(e)}")

    # delete_query = "DELETE FROM dbo.Part_Master_Records$ WHERE part_id = ?"
    # cursor.execute(delete_query, part_id)
    # if cursor.rowcount == 0:
    #     raise HTTPException(status_code=404, detail="Part not found")
    # connection.commit()

    # response = {
    #     "message": "Part deleted successfully",
    #     "part_id": part_id
    # }
    # return response
 
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
    global order_counter
    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "workcentre_id_exists": "order_id already exists and cannot be added",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            return {"error": error_messages["connection_unavailable"]}

        # Check if the cursor is initialized
        if cursor is None:
            return {"error": error_messages["cursor_uninitialized"]}
        
        # Fetch the latest order_id from the database
        query = "SELECT TOP 1 order_id FROM dbo.Orders$ ORDER BY order_id DESC"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            latest_order_id = result[0]  # e.g., "WC1000"
            # Extract the integer part of the order_id
            order_counter = int(latest_order_id[1:])  # Ignore the "WC" prefix
        else:
            order_counter = 0  # Default to 0 if no records are found

        # Increment the counter for the new order_id
        order_counter += 1
        order_id = f"O{str(order_counter).zfill(3)}"
        order.order_id = order_id

        # Check if the order_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.Orders$ WHERE order_id = ?"
        cursor.execute(check_query, (order.order_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="order_id already exists and cannot be added")
    
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Orders$ (order_id, part_id, part_qty, order_date, due_date, order_last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            order.order_id,
            order.part_id,
            order.part_qty,
            order.order_date,
            order.due_date,
            order.order_last_updated
        ))
        connection.commit()

        response = {
            "message": "Order created successfully",
            "data": order
        }
        return response

    except pyodbc.IntegrityError:
        return {"error": error_messages["integrity_error"]}
    except pyodbc.DatabaseError as e:
        return {"error": f"{error_messages['database_error']}: {str(e)}"}
    except Exception as e:
        return {"error": f"{error_messages['unexpected_error']}: {str(e)}"}


@app.delete("/orders/{order_id}")
async def delete_order(order_id: str):
    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "referenced_entry": "Cannot delete BOM entry because it is referenced by Routings$ table.",
        "bom_not_found": "BOM not found",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            raise HTTPException(status_code=503, detail=error_messages["connection_unavailable"])

        # Check if the cursor is initialized
        if cursor is None:
            raise HTTPException(status_code=503, detail=error_messages["cursor_uninitialized"])

        # Check if the order_id exists in dbo.Orders$
        check_order_query = "SELECT part_id FROM dbo.Orders$ WHERE order_id = ?"
        cursor.execute(check_order_query, (order_id,))
        order_row = cursor.fetchone()[0]
        
        if not order_row:
            raise HTTPException(status_code=404, detail=error_messages["order_not_found"])
        
        part_id = order_row[0]

        # Check if the part_id exists in dbo.Part_Master_Records$
        check_part_query = "SELECT COUNT(*) FROM dbo.Part_Master_Records$ WHERE part_id = ?"
        cursor.execute(check_part_query, (part_id,))
        part_count = cursor.fetchone()[0]

        if part_count > 0:
            raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])

        # if referencing_count > 0:
        #     raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])

        # Delete BOM entry
        delete_query = "DELETE FROM dbo.Orders$ WHERE order_id = ?"
        cursor.execute(delete_query, (order_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=error_messages["order_not_found"])
        
        # Commit the transaction
        connection.commit()

        # If no exceptions, return success response
        response = {
            "message": "Order successfully deleted",
            "order_id": order_id
        }
        return response

    except pyodbc.IntegrityError:
        raise HTTPException(status_code=400, detail=error_messages["integrity_error"])
    except pyodbc.DatabaseError:
        raise HTTPException(status_code=500, detail=error_messages["database_error"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{error_messages['unexpected_error']}: {str(e)}")
    
    # delete_query = "DELETE FROM dbo.Orders$ WHERE order_id = ?"
    # cursor.execute(delete_query, order_id)
    # if cursor.rowcount == 0:
    #     raise HTTPException(status_code=404, detail="Order not found")
    # connection.commit()
    
    # response = {
    #     "message": "Order deleted successfully",
    #     "order_id": order_id
    # }
    # return response
 
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
    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "workcentre_id_exists": "workcentre_id already exists and cannot be added",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            return {"error": error_messages["connection_unavailable"]}

        # Check if the cursor is initialized
        if cursor is None:
            return {"error": error_messages["cursor_uninitialized"]}
        
        query = "SELECT TOP 1 workcentre_id FROM dbo.Workcentre$ ORDER BY workcentre_id DESC"
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            latest_workcentre_id = result[0]  # e.g., "WC005"
            # Extract the integer part of the workcentre_id
            workcentre_counter = int(latest_workcentre_id[2:])  # Ignore the "WC" prefix
        else:
            workcentre_counter = 0  # Default to 0 if no records are found

        # Increment the counter for the new workcentre_id
        workcentre_counter += 1
        workcentre_id = f"WC{str(workcentre_counter).zfill(3)}"
        workcentre.workcentre_id = workcentre_id
       
    
        check_query = "SELECT COUNT(*) FROM dbo.Workcentre$ WHERE workcentre_id = ?"
        cursor.execute(check_query, (workcentre.workcentre_id,))
        count = cursor.fetchone()[0]

        if count > 0:
        # cursor.close()
        # connection.close()
            raise HTTPException(status_code=400, detail="workcentre_id already exists and cannot be added")
    
    # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Workcentre$(workcentre_id, workcentre_name, workcentre_description, capacity, capacity_unit, cost_rate_h,workcentre_last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            workcentre.workcentre_id,
            workcentre.workcentre_name, 
            workcentre.workcentre_description,
            workcentre.capacity,
            workcentre.capacity_unit,
            workcentre.cost_rate_h,
            workcentre.workcentre_last_updated
        ))
    
        connection.commit()

        response = {
            "message": "WorkCentre created successfully",
            "data": workcentre
        }
        return response

    except pyodbc.IntegrityError:
        return {"error": error_messages["integrity_error","id":workcentre.workcentre_id]}
    except pyodbc.DatabaseError:
        return {"error": error_messages["database_error"]}
    except Exception as e:
        return {"error": f"{error_messages['unexpected_error']}: {str(e)}"}

        # Generate the Workcentre ID
        # workcentre_id = f"WC{str(workcentre_counter).zfill(3)}"
        # workcentre_dict = workcentre.dict()
        # workcentre_dict['workcentre_id'] = workcentre_id
        # workcentre_counter += 1

        # check_query = "SELECT COUNT(*) FROM dbo.Workcentre$ WHERE workcentre_id = ?"
        # cursor.execute(check_query, (workcentre_dict['workcentre_id'],))
        # count = cursor.fetchone()[0]

        # if count > 0:
        #     return {"error": error_messages["workcentre_id_exists"]}

        # # Insert data into the database
        # insert_query = """
        # INSERT INTO dbo.Workcentre$(workcentre_id, workcentre_name, workcentre_description, capacity, capacity_unit, cost_rate_h, workcentre_last_updated)
        # VALUES (?, ?, ?, ?, ?, ?, ?)
        # """
        # cursor.execute(insert_query, (
        #     workcentre_dict['workcentre_id'],
        #     workcentre_dict['workcentre_name'],
        #     workcentre_dict['workcentre_description'],
        #     workcentre_dict['capacity'],
        #     workcentre_dict['capacity_unit'],
        #     workcentre_dict['cost_rate_h'],
        #     workcentre_dict['workcentre_last_updated']
        # ))

        # connection.commit()

        # response = {
        #     "message": "WorkCentre created successfully",
        #     "data": workcentre_dict
        # }
        # return response
        # Generate the Workcentre ID
        

@app.put("/workcentre/{workcentre_id}")
async def update_workcentre(workcentre_id: str, workcentre: WorkCentre):

    last_id_query = "SELECT TOP 1 workcentre_id FROM dbo.Workcentre$ ORDER BY CAST(SUBSTRING(workcentre_id, 3, LEN(workcentre_id)-2) AS INT) DESC"
    cursor.execute(last_id_query)
    last_id_row = cursor.fetchone()

    if not last_id_row:
        # If no existing workcentres, start with a base ID, e.g., "WC001"
        new_workcentre_id = "WC001"
    else:
        last_id = last_id_row[0]
        # Assuming the format "WC###", extract the numeric part, increment, and reformat
        prefix, number = last_id[:2], int(last_id[2:])
        new_workcentre_id = f"{prefix}{str(number + 1).zfill(3)}"

        workcentre.workcentre_id = new_workcentre_id

    update_status_query = """
    UPDATE dbo.Workcentre$
    SET status = 'inactive'
    WHERE workcentre_id = ? 
    """

    cursor.execute(update_status_query, (workcentre_id,))

    workcentre.workcentre_id = new_workcentre_id
    workcentre.status = "active"

    insert_query = """
    INSERT INTO dbo.Workcentre$ (workcentre_id, workcentre_name, workcentre_description, capacity, capacity_unit, cost_rate_h, workcentre_last_updated, status)
    VALUES (?, ?, ?, ?, ?, ?, ?,?)
    """
    cursor.execute(insert_query, (
        workcentre.workcentre_id,
        workcentre.workcentre_name,
        workcentre.workcentre_description,
        workcentre.capacity,
        workcentre.capacity_unit,
        workcentre.cost_rate_h,
        workcentre.workcentre_last_updated,
        workcentre.status
    ))
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"workcentre_id {workcentre_id} not found")
    
    connection.commit()
    
    response = {
        "message": "Workcentre updated successfully with new workcentre_id",
        "data": {
            "workcentre_id": workcentre.workcentre_id,
            "workcentre_name": workcentre.workcentre_name,
            "workcentre_description": workcentre.workcentre_description,
            "capacity": workcentre.capacity,
            "capacity_unit": workcentre.capacity_unit,
            "cost_rate_h": workcentre.cost_rate_h,
            "workcentre_last_updated": workcentre.workcentre_last_updated,
            "status": workcentre.status
        }
    }
    return response

    
@app.delete("/workcentre/{workcentre_id}")
async def delete_workcentre(workcentre_id: str):
    error_messages = {
        "connection_unavailable": "Database connection is not available",
        "cursor_uninitialized": "Database cursor is not initialized",
        "referenced_entry": "Cannot delete BOM entry because it is referenced by Routings$ table.",
        "bom_not_found": "BOM not found",
        "integrity_error": "Database integrity error: Check constraints and foreign keys.",
        "database_error": "Database error occurred.",
        "unexpected_error": "An unexpected error occurred."
    }

    try:
        # Check if the database connection is established
        if connection is None:
            raise HTTPException(status_code=503, detail=error_messages["connection_unavailable"])

        # Check if the cursor is initialized
        if cursor is None:
            raise HTTPException(status_code=503, detail=error_messages["cursor_uninitialized"])

        # Check for referencing entries in dbo.Routings$
        check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE workcentre_id = ?"
        cursor.execute(check_query, (workcentre_id,))
        referencing_count = cursor.fetchone()[0]

        if referencing_count > 0:
            raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])

        # Delete BOM entry
        delete_query = "DELETE FROM dbo.Workcentre$ WHERE workcentre_id = ?"
        cursor.execute(delete_query, (workcentre_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=error_messages["workcentre_not_found"])
        
        # Commit the transaction
        connection.commit()

        # If no exceptions, return success response
        response = {
            "message": error_messages["unexpected_error"],
            "workcentre_id": workcentre_id
        }
        return response

    except pyodbc.IntegrityError:
        raise HTTPException(status_code=400, detail=error_messages["integrity_error"])
    except pyodbc.DatabaseError:
        raise HTTPException(status_code=500, detail=error_messages["database_error"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{error_messages['unexpected_error']}: {str(e)}")

    # delete_query = "DELETE FROM dbo.Workcentre$ WHERE workcentre_id = ?"
    # cursor.execute(delete_query, workcentre_id)
    # if cursor.rowcount == 0:
    #     raise HTTPException(status_code=404, detail="WorkCentre not found")
    # connection.commit()

    # response = {
    #     "message": "Workcentre deleted successfully",
    #     "workcentre_id": workcentre_id
    # }
    # return response

#####################################################################################################
# updating part_id across tables

@app.put("/partmasterrecords/update_part_id")
async def update_part_id(part_update: PartIDUpdate):
    old_part_id = part_update.old_part_id
    new_part_id = part_update.new_part_id

    try:
        # Update Part_Master_Records$
        update_query = "UPDATE dbo.Part_Master_Records$ SET part_id = ? WHERE part_id = ?"
        cursor.execute(update_query, new_part_id, old_part_id)
        
        # Update BOM$
        update_query = "UPDATE dbo.BOM$ SET part_id = ? WHERE part_id = ?"
        cursor.execute(update_query, new_part_id, old_part_id)
        
        # Update Routings$
        update_query = "UPDATE dbo.Routings$ SET BOM_id = ? WHERE BOM_id = ?"
        cursor.execute(update_query, new_part_id, old_part_id)
        
        # Update Orders$
        update_query = "UPDATE dbo.Orders$ SET part_id = ? WHERE part_id = ?"
        cursor.execute(update_query, new_part_id, old_part_id)

        connection.commit()

        response = {
            "message": "Part ID updated successfully across all relevant tables",
            "old_part_id": old_part_id,
            "new_part_id": new_part_id
        }
        return response

    except Exception as e:
        connection.rollback()
        raise HTTPException(status_code=500, detail=str(e))
