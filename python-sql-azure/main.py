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
from typing import List, Optional

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
    order_date: datetime
    due_date: datetime
    order_last_updated: datetime
    status: str = None

class BOM(BaseModel):
    BOM_id: Optional[str] = None
    part_id: str
    child_id: str
    child_qty: float
    child_leadtime: float
    BOM_last_updated: datetime
    status: str = None
    process_description: Optional[str] = None
    setup_time: Optional[int] = None
    runtime: Optional[int] = None
    routing_id: Optional[str] = None 
    operations_sequence: Optional[int] = None # follow previous BOM
    workcentre_id: Optional[str] = None # follow previous BOM
    
class Routing(BaseModel):
    routing_id: str = None 
    BOM_id: str
    operations_sequence: int
    workcentre_id: str
    process_description: Optional[str] = None
    setup_time: Optional[int] = None
    runtime: Optional[int] = None
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

# class PartIDUpdate(BaseModel):
#     old_part_id: str
#     new_part_id: str

# class UpdateBOMRequest(BaseModel):
#     bom: BOM
#     routing: List[Routing]
 
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

        check_bom_query = """
        SELECT BOM_id FROM dbo.BOM$
        WHERE part_id = ? AND child_id = ?
        """
        cursor.execute(check_bom_query, (bom.part_id, bom.child_id))
        existing_bom = cursor.fetchone()

        if existing_bom:
            return HTTPException(status_code=400, detail="part_id and child_id already belong to the same BOM_id and cannot be added.")
        
        cursor.execute("SELECT part_id, child_id FROM dbo.BOM$")
        all_bom_entries = cursor.fetchall()
        
        bom_dict = {}
        for part_id, child_id in all_bom_entries:
            if part_id in bom_dict:
                bom_dict[part_id].append(child_id)
            else:
                bom_dict[part_id] = [child_id]

        # Function to check for circular dependency using DFS
        def has_circular_dependency(new_child_id, old_part_id, visited=None):
            if visited is None:
                visited = set()
            if new_child_id in visited:
                return False
            visited.add(new_child_id)
            children = bom_dict.get(new_child_id, [])
            for child in children:
                if child == old_part_id:
                    return True
                if has_circular_dependency(child, old_part_id, visited):
                    return True
            visited.remove(new_child_id)
            return False

        # Check for circular dependency
        if has_circular_dependency(bom.child_id, bom.part_id):
            return HTTPException(status_code=400, detail="Action cannot be completed: this item can't exist as both a parent and a child.")
        
        check_parts_query = "SELECT COUNT(*) FROM dbo.Part_Master_Records$ WHERE part_id = ? OR part_id = ?"
        cursor.execute(check_parts_query, (bom.part_id, bom.child_id))
        count = cursor.fetchone()[0]

        if count < 2:
            return HTTPException(status_code=400, detail="part_id and/or child_id doesn't exist")

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
        bom.status ="active"

        current_datetime = datetime.now()
        bom.BOM_last_updated = current_datetime

        # Check if BOM_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.BOM$ WHERE bom_id = ?"
        cursor.execute(check_query, (bom.BOM_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            return HTTPException(status_code=400, detail="bom_id already exists and cannot be added")
        
        previous_bom_query = """
        SELECT BOM_id 
        FROM dbo.BOM$ 
        WHERE part_id = ? AND status = 'active'
        ORDER BY BOM_last_updated DESC
        """
        cursor.execute(previous_bom_query, (bom.part_id,))
        previous_bom_result = cursor.fetchone()

        if previous_bom_result:
            previous_bom_id = previous_bom_result[0]

            # Update the status of the original BOM_id to 'NA'
            update_bom_status_query = """
            UPDATE dbo.BOM$
            SET status = 'NA'
            WHERE BOM_id = ? AND status = 'active'
            """
            cursor.execute(update_bom_status_query, (previous_bom_id,))
        
            workcentre_query = """
            SELECT TOP 1 routing_id
            FROM dbo.Routings$ 
            WHERE BOM_id = ? 
            ORDER BY routing_id DESC
            """
            cursor.execute(workcentre_query, (previous_bom_id,))
            workcentre_result = cursor.fetchone()

            if workcentre_result:
                previous_routing_id = workcentre_result[0]
                

                # Update the status of the original routing_id to 'NA'
                update_routing_status_query = """
                UPDATE dbo.Routings$
                SET status = 'NA'
                WHERE routing_id = ? AND status = 'active'
                """
                cursor.execute(update_routing_status_query, (previous_routing_id,))
            
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.BOM$ (BOM_id, part_id, child_id, child_qty, child_leadtime, BOM_last_updated, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            bom.BOM_id,
            bom.part_id,
            bom.child_id,
            bom.child_qty,
            bom.child_leadtime,
            bom.BOM_last_updated,
            bom.status
        ))
        # # Generate a new routing_id
        # query = "SELECT TOP 1 routing_id FROM dbo.Routings$ ORDER BY routing_id DESC"
        # cursor.execute(query)
        # result = cursor.fetchone()

        # if result:
        #     latest_routing_id = result[0]
        #     routing_counter = int(latest_routing_id[1:])
        # else:
        #     routing_counter = 0

        # routing_counter += 1
        # routing_id = f"R{str(routing_counter).zfill(3)}"

        # # Insert data into the Routing table
        # insert_routing_query = """
        # INSERT INTO dbo.Routings$ (routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update, status)
        # VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        # """
        # cursor.execute(insert_routing_query, (
        #     routing_id,
        #     bom.BOM_id,
        #     1,  # Assuming operations_sequence starts at 1 for the new entry
        #     bom.workcentre_id,  # Retrieved or default workcentre_id
        #     bom.process_description,  # Replace with actual process description or parameter
        #     bom.setup_time,  # Setup time, adjust as necessary
        #     bom.runtime,  # Runtime, adjust as necessary
        #     current_datetime,
        #     "active"
        # ))

        connection.commit()

        response = {
            "message": "BOM and Routing created successfully",
            "data": bom
        }
        return response    

    except pyodbc.IntegrityError:
        return {"error": error_messages["integrity_error"]}
    except pyodbc.DatabaseError as e:
        return {"error": f"{error_messages['database_error']}: {str(e)}"}
    except Exception as e:
        return {"error": f"{error_messages['unexpected_error']}: {str(e)}"}


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
async def update_bom(bom: BOM):

    try:
        # Check if the part_id and child_id combination already exists under any BOM_id
        check_bom_query = """
        SELECT BOM_id FROM dbo.BOM$
        WHERE part_id = ? AND child_id = ? 
        """
        cursor.execute(check_bom_query, (bom.part_id, bom.child_id))
        existing_bom = cursor.fetchone()

        if existing_bom:
            return HTTPException(status_code=400, detail="part_id and child_id already belong to the same BOM_id and cannot be added.")

        # Fetch all BOM entries from the database
        cursor.execute("SELECT part_id, child_id FROM dbo.BOM$")
        all_bom_entries = cursor.fetchall()
        
        # Create a dictionary to map part_id to its children
        bom_dict = {}
        for part_id, child_id in all_bom_entries:
            if part_id in bom_dict:
                bom_dict[part_id].append(child_id)
            else:
                bom_dict[part_id] = [child_id]

        # Function to check for circular dependency using DFS
        def has_circular_dependency(new_child_id, old_part_id, visited=None):
            if visited is None:
                visited = set()
            if new_child_id in visited:
                return False
            visited.add(new_child_id)
            children = bom_dict.get(new_child_id, [])
            for child in children:
                if child == old_part_id:
                    return True
                if has_circular_dependency(child, old_part_id, visited):
                    return True
            visited.remove(new_child_id)
            return False

        # Check for circular dependency
        if has_circular_dependency(bom.child_id, bom.part_id):
            return HTTPException(status_code=400, detail="Action cannot be completed: this item can't exist as both a parent and a child.")

        last_id_query = "SELECT TOP 1 BOM_id FROM dbo.BOM$ ORDER BY CAST(SUBSTRING(BOM_id, 2, LEN(BOM_id)-1) AS INT) DESC"
        cursor.execute(last_id_query)
        last_id_row = cursor.fetchone()

        if not last_id_row:
            new_BOM_id = "B001"
        else:
            last_id = last_id_row[0]
            prefix, number = last_id[0], int(last_id[1:])
            new_BOM_id = f"{prefix}{str(number + 1).zfill(3)}"

        update_status_query = """
        UPDATE dbo.BOM$
        SET status = 'NA'
        WHERE BOM_id = ? AND status = 'active'
        """
        cursor.execute(update_status_query, (bom.BOM_id,))

        # Insert the new BOM entry with updated child_id or other changes
        bom.BOM_id = new_BOM_id
        bom.status = "active"

        insert_query = """
        INSERT INTO dbo.BOM$ (BOM_id, part_id, child_id, child_qty, child_leadtime, BOM_last_updated, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            bom.BOM_id,
            bom.part_id,
            bom.child_id,
            bom.child_qty,
            bom.child_leadtime,
            bom.BOM_last_updated,
            bom.status
        ))

        if cursor.rowcount == 0:
            return HTTPException(status_code=404, detail=f"BOM_id {bom.BOM_id} not found")
        
    #     query = "SELECT TOP 1 routing_id FROM dbo.Routings$ ORDER BY routing_id DESC"
    #     cursor.execute(query)
    #     result = cursor.fetchone()

    #     if result:
    #         latest_routing_id = result[0]  # e.g., "R470"
    #         routing_counter = int(latest_routing_id[1:])  # Extract integer part, ignore "R" prefix
    #     else:
    #         routing_counter = 0

    #     routing_counter += 1
    #     new_routing_id = f"R{str(routing_counter).zfill(3)}"

    #     combined_query = """
    #     UPDATE dbo.Routings$
    #     SET status = 'NA'
    #     OUTPUT 
    #         INSERTED.routing_id, 
    #         INSERTED.operations_sequence, 
    #         INSERTED.workcentre_id, 
    #         INSERTED.process_description, 
    #         INSERTED.setup_time, 
    #         INSERTED.runtime
    #     WHERE routing_id IN (
    #         SELECT TOP 1 routing_id 
    #         FROM dbo.Routings$
    #         WHERE BOM_id = ?
    #         ORDER BY routing_id DESC
    #     )
    #     """

    #     cursor.execute(combined_query, (bom.BOM_id,))
    #     latest_routing_details = cursor.fetchone()

    #     if latest_routing_details:
    # # Extracting the details from the combined result
    #         latest_routing_id, operations_sequence, workcentre_id, process_description, setup_time, runtime = latest_routing_details
    #     else:
    # # Fallback to default values if no routing details are found
    #         latest_routing_id = None
    #         operations_sequence = 1
    #         workcentre_id = "WC001"
    #         process_description = "Default Process Description"
    #         setup_time = 0
    #         runtime = 0

    #     insert_routing_query = """
    #     INSERT INTO dbo.Routings$ (routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update, status)
    #     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     """
    
    #     cursor.execute(insert_routing_query, (
    #         new_routing_id,
    #         bom.BOM_id,
    #         operations_sequence, #operations sequence = 1
    #         workcentre_id,
    #         process_description,
    #         setup_time,
    #         runtime,
    #         bom.BOM_last_updated,
    #         'active'
    #     ))

        connection.commit()

        response = {
            "message": "BOM and Routing created successfully",
            "BOM_data": bom,
            "Routing_id": new_routing_id
        }
        return response 
    
    except HTTPException as e:
        connection.rollback()
        return {"error": str(e)}
    except Exception as e:
        connection.rollback()
        return {"error": f"An unexpected error occurred: {str(e)}"}

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
        routing.status = "active"

        # Check if routing_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE routing_id = ?"
        cursor.execute(check_query, (routing.routing_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="routing_id already exists and cannot be added")
        
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Routings$ (routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update,status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            "status": routing.status
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
        part.status = "active"
        
        # Check if the part_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.Part_Master_Records$ WHERE part_id = ?"
        cursor.execute(check_query, (part.part_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="part_id already exists and cannot be added")
        
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Part_Master_Records$ (part_id, part_name, inventory, POM, UOM, part_description, unit_cost, lead_time, part_last_updated,status)
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

    # Fetch the existing part details
    existing_part_query = "SELECT part_name FROM dbo.Part_Master_Records$ WHERE part_id = ?"
    cursor.execute(existing_part_query, (part_id,))
    existing_part = cursor.fetchone()

    if not existing_part:
        raise HTTPException(status_code=404, detail=f"part_id {part_id} not found")
    
    # Check if part_name or part_id is being changed
    if part_id != part.part_id or existing_part[0] != part.part_name:
        raise HTTPException(status_code=400, detail="part_id and/or part_name cannot be changed")
    
    current_time = datetime.now()

    update_query = """
    UPDATE dbo.Part_Master_Records$
    SET inventory = ?, POM = ?, UOM = ?, part_description = ?, unit_cost = ?, lead_time = ?, part_last_updated = ?, status = ?
    WHERE part_id = ?
    """
    cursor.execute(update_query, (
        part.inventory,
        part.POM,
        part.UOM,
        part.part_description,
        part.unit_cost,
        part.lead_time,
        current_time,  # Set part_last_updated to the current time
        "Active",
        part_id
    ))

    connection.commit()
    response = {
        "message": "Part Master Records updated successfully",
        "data": {
            "part_id": part.part_id,
            "part_name": part.part_name,
            "inventory": part.inventory,
            "POM": part.POM,
            "UOM": part.UOM,
            "part_description": part.part_description,
            "unit_cost": part.unit_cost,
            "lead_time": part.lead_time,
            "part_last_updated": current_time,
            "status": "Active"
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
 

@app.get("/orderdetailsfull/{order_id}")
async def get_full_order_details(order_id: str):
    try:
        query = """
        SELECT 
            o.order_id, 
            o.part_id, 
            o.part_qty,
            o.order_date, 
            o.due_date, 
            o.order_last_updated,
            pmr.part_name, 
            pmr.part_description,
            pmr.inventory,
            pmr.POM,
            pmr.unit_cost,
            pmr.lead_time,
            b.BOM_id, 
            b.part_id,
            b.child_id,
            b.child_qty,
            b.child_leadtime,
            b.BOM_last_updated,
            r.routing_id, 
            r.routings_last_update,
            wc.workcentre_id, 
            wc.workcentre_name, 
            wc.workcentre_last_updated
        FROM 
            dbo.Orders$ o
        JOIN 
            dbo.Part_Master_Records$ pmr ON o.part_id = pmr.part_id
        JOIN 
            dbo.BOM$ b ON o.part_id = b.part_id
        JOIN 
            dbo.Routings$ r ON b.BOM_id = r.BOM_id
        JOIN 
            dbo.Workcentre$ wc ON r.workcentre_id = wc.workcentre_id
        WHERE 
            o.order_id = ?
            AND b.BOM_last_updated = (
                SELECT MAX(b2.BOM_last_updated)
                FROM dbo.BOM$ b2
                WHERE b2.part_id = o.part_id AND b2.BOM_last_updated <= o.order_date
            )
            AND r.routings_last_update = (
                SELECT MAX(r2.routings_last_update)
                FROM dbo.Routings$ r2
                WHERE r2.BOM_id = b.BOM_id AND r2.routings_last_update <= o.order_date
            )
            AND wc.workcentre_last_updated = (
                SELECT MAX(wc2.workcentre_last_updated)
                FROM dbo.Workcentre$ wc2
                WHERE wc2.workcentre_id = r.workcentre_id AND wc2.workcentre_last_updated <= o.order_date
            )
        """
        cursor.execute(query, (order_id,))
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Order not found")
        return result
    
    except HTTPException as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {str(e)}"}

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
            order_counter = int(latest_order_id[1:])  # Ignore the "O" prefix
        else:
            order_counter = 0  # Default to 0 if no records are found

        # Increment the counter for the new order_id
        order_counter += 1
        order_id = f"O{str(order_counter).zfill(3)}"
        order.order_id = order_id
        order.status ="processing"

        # Check if the order_id already exists
        check_query = "SELECT COUNT(*) FROM dbo.Orders$ WHERE order_id = ?"
        cursor.execute(check_query, (order.order_id,))
        count = cursor.fetchone()[0]

        if count > 0:
            raise HTTPException(status_code=400, detail="order_id already exists and cannot be added")
    
        # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Orders$ (order_id, part_id, part_qty, order_date, due_date, order_last_updated,status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            order.order_id,
            order.part_id,
            order.part_qty,
            order.order_date,
            order.due_date,
            order.order_last_updated,
            order.status 
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
        workcentre.status = "active"
       
    
        check_query = "SELECT COUNT(*) FROM dbo.Workcentre$ WHERE workcentre_id = ?"
        cursor.execute(check_query, (workcentre.workcentre_id,))
        count = cursor.fetchone()[0]

        if count > 0:
        # cursor.close()
        # connection.close()
            raise HTTPException(status_code=400, detail="workcentre_id already exists and cannot be added")
    
    # Insert data into the database
        insert_query = """
        INSERT INTO dbo.Workcentre$(workcentre_id, workcentre_name, workcentre_description, capacity, capacity_unit, cost_rate_h,workcentre_last_updated, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

#####################################################################################################
# updating part_id across tables

# @app.put("/partmasterrecords/update_part_id")
# async def update_part_id(part_update: PartIDUpdate):
#     old_part_id = part_update.old_part_id
#     new_part_id = part_update.new_part_id

#     try:
#         # Update Part_Master_Records$
#         update_query = "UPDATE dbo.Part_Master_Records$ SET part_id = ? WHERE part_id = ?"
#         cursor.execute(update_query, new_part_id, old_part_id)
        
#         # Update BOM$
#         update_query = "UPDATE dbo.BOM$ SET part_id = ? WHERE part_id = ?"
#         cursor.execute(update_query, new_part_id, old_part_id)
        
#         # Update Routings$
#         update_query = "UPDATE dbo.Routings$ SET BOM_id = ? WHERE BOM_id = ?"
#         cursor.execute(update_query, new_part_id, old_part_id)
        
#         # Update Orders$
#         update_query = "UPDATE dbo.Orders$ SET part_id = ? WHERE part_id = ?"
#         cursor.execute(update_query, new_part_id, old_part_id)

#         connection.commit()

#         response = {
#             "message": "Part ID updated successfully across all relevant tables",
#             "old_part_id": old_part_id,
#             "new_part_id": new_part_id
#         }
#         return response

#     except Exception as e:
#         connection.rollback()
#         raise HTTPException(status_code=500, detail=str(e))
