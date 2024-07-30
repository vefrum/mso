import datetime
from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import json
import requests
from io import StringIO
from pydantic import BaseModel
from datetime import date, datetime

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
    
class Order(BaseModel):
    order_id: str
    part_id: str
    part_qty: int
    order_date: date
    due_date: date
    order_last_updated: datetime

class BOM(BaseModel):
    BOM_id: str
    part_id: str
    child_id: str
    child_qty: float
    child_leadtime: float
    BOM_last_updated: datetime

class Routing(BaseModel):
    routing_id: str
    BOM_id: str
    operations_sequence: int
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

class PartIDUpdate(BaseModel):
    old_part_id: str
    new_part_id: str
 
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
    
    # Generate the bom ID
    BOM_id = f"WC{str(bom_counter).zfill(3)}"
    bom.BOM_id = BOM_id
    bom_counter += 1

    # Check if BOM_id already exists
    check_query = "SELECT COUNT(*) FROM dbo.BOM$ WHERE BOM_id = ?"
    cursor.execute(check_query, (bom.BOM_id,))
    count = cursor.fetchone()[0]

    if count > 0:
        # cursor.close()
        # connection.close()
        raise HTTPException(status_code=400, detail="_id already exists and cannot be added")

    # Insert data into the database
    insert_query = """
    INSERT INTO dbo.dbo.BOM$(BOM_id, part_id, child_id, child_qty, child_leadtime, BOM_last_updated)
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
async def update_bom(BOM_id: str, bom: BOM):
    # # Generate the new BOM_id with a prime symbol
    # new_BOM_id = BOM_id + "'"

    # check_query = "SELECT COUNT(*) FROM dbo.BOM$ WHERE BOM_id = ?"
    # cursor.execute(check_query, (new_BOM_id,))
    # count = cursor.fetchone()[0]

    # if count > 0:
    #     raise HTTPException(status_code=400, detail=f"BOM_id {new_BOM_id} already exists")
    
    # # Update bom object with new BOM_id
    # bom.BOM_id = new_BOM_id
    
    # update_query = """
    # UPDATE dbo.BOM$
    # SET part_id = ?, child_id = ?, child_qty = ?, child_leadtime = ?, BOM_last_updated = ?
    # WHERE BOM_id = ?
    # """
    # cursor.execute(update_query, (
    #     bom.part_id,
    #     bom.child_id,
    #     bom.child_qty,
    #     bom.child_leadtime,
    #     bom.BOM_last_updated,
    #     BOM_id
    # ))

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

        bom.BOM_id = new_BOM_id

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
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"BOM_id {BOM_id} not found")
    
    connection.commit()
    response = {
        "message": "BOM updated successfully with new BOM_id",
        "data": {
            "BOM_id": bom.BOM_id,
            "part_id": bom.part_id,
            "child_id": bom.child_id,
            "child_qty": bom.child_qty,
            "child_leadtime": bom.child_leadtime,
            "BOM_last_updated": bom.BOM_last_updated, 
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
    
    # Generate the Part ID
    routing_id = f"WC{str(routing_counter).zfill(3)}"
    routing.routing_id = routing_id
    routing_counter += 1

    # Check if routing_id already exists
    check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE routing_id = ?"
    cursor.execute(check_query, (routing.routing_id,))
    count = cursor.fetchone()[0]

    if count > 0:
        # cursor.close()
        # connection.close()
        raise HTTPException(status_code=400, detail="routing_id already exists and cannot be added")

    # Insert data into the database
    insert_query = """
    INSERT INTO dbo.Routings$(routing_id, BOM_id, operations_sequence, workcentre_id, process_description, setup_time, runtime, routings_last_update)
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

@app.put("/routings/{routing_id}")
async def update_routing(routing_id: str, routing: Routing):
    # # Generate the new BOM_id with a prime symbol
    # new_routing_id = routing_id + "'"

    # check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE routing_id = ?"
    # cursor.execute(check_query, (new_routing_id,))
    # count = cursor.fetchone()[0]

    # if count > 0:
    #     raise HTTPException(status_code=400, detail=f"BOM_id {new_routing_id} already exists")
    
    # # Update bom object with new BOM_id
    # routing.routing_id = new_routing_id

    # update_query = """
    # UPDATE dbo.Routings$
    # SET BOM_id = ?, operations_sequence = ?, workcentre_id = ?, process_description = ?, setup_time = ?, runtime = ?, routings_last_update = ?
    # WHERE routing_id = ?
    # """
    # cursor.execute(update_query, (
    #     routing.BOM_id,
    #     routing.operations_sequence,
    #     routing.workcentre_id,
    #     routing.process_description,
    #     routing.setup_time,
    #     routing.runtime,
    #     routing.routings_last_update,
    #     routing.routing_id
    # ))

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

        routing.routing_id = new_routing_id

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
            "runtime": routing.setup_time,
            "routings_last_update": routing.routings_last_update
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
    
    # Generate the Part ID
    part_id = f"WC{str(part_counter).zfill(3)}"
    part.part_id = part_id
    part_counter += 1
    
    check_query = "SELECT COUNT(*) FROM dbo.Part_Master_Records$ WHERE part_id = ?"
    cursor.execute(check_query, (part.part_id,))
    count = cursor.fetchone()[0]

    if count > 0:
        # cursor.close()
        # connection.close()
        raise HTTPException(status_code=400, detail="part_id already exists and cannot be added")

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

    response = {
        "message": "Part created successfully",
        "data": part
    }
    return response

@app.put("/partmasterrecords/{part_id}")
async def update_part(part_id: str, part: Part):
    # # Generate the new BOM_id with a prime symbol
    # new_part_id = part_id + "'"

    # check_query = "SELECT COUNT(*) FROM dbo.Routings$ WHERE part_id = ?"
    # cursor.execute(check_query, (new_part_id,))
    # count = cursor.fetchone()[0]

    # if count > 0:
    #     raise HTTPException(status_code=400, detail=f"BOM_id {new_part_id} already exists")
    
    # # Update bom object with new BOM_id
    # part.part_id = new_part_id

    update_query = """
    UPDATE dbo.Part_Master_Records$
    SET part_name = ?, inventory = ?, POM = ?, UOM = ?, part_description = ?, unit_cost = ?, lead_time = ?, part_last_updated = ?
    WHERE part_id = ?
    """
    cursor.execute(update_query, (
        part.part_name,
        part.inventory,
        part.POM,
        part.UOM,
        part.part_description,
        part.unit_cost,
        part.lead_time,
        part.part_last_updated,
        part.part_id
    ))
    # insert_query = """
    # INSERT INTO dbo.Part_Master_Records$ (part_id, part_name, inventory, POM, UOM, part_description, unit_cost, lead_time, part_last_updated)
    # VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    # """
    # cursor.execute(insert_query, (
    #     part.part_id,
    #     part.part_name,
    #     part.inventory,
    #     part.POM,
    #     part.UOM,
    #     part.part_description,
    #     part.unit_cost,
    #     part.lead_time,
    #     part.part_last_updated
    # ))
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
            "part_last_updated": part.part_last_updated
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
    
    # Generate the Workcentre ID
    order_id = f"WC{str(order_counter).zfill(3)}"
    order.order_id = order_id
    order_counter += 1

    check_query = "SELECT COUNT(*) FROM dbo.Orders$ WHERE order_id = ?"
    cursor.execute(check_query, (order.order_id,))
    count = cursor.fetchone()[0]

    if count > 0:
        # cursor.close()
        # connection.close()
        raise HTTPException(status_code=400, detail="order_id already exists and cannot be added")
    
    # Insert data into the database
    insert_query = """
    INSERT INTO dbo.dbo.Orders$(order_id, part_id, part_qty, order_date, due_date, order_last_updated)
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

@app.put("/orders/{order_id}")
async def update_order(order_id: str, order: Order):
    # Generate the new BOM_id with a prime symbol
    # new_order_id = order_id + "'"

    # check_query = "SELECT COUNT(*) FROM dbo.Orders$ WHERE order_id = ?"
    # cursor.execute(check_query, (new_order_id,))
    # count = cursor.fetchone()[0]

    # if count > 0:
    #     raise HTTPException(status_code=400, detail=f"order_id {new_order_id} already exists")
    
    # # Update bom object with new BOM_id
    # order.order_id = new_order_id

    # update_query = """
    # UPDATE dbo.Orders$
    # SET part_id = ?, part_qty = ?, order_date = ?, due_date = ?, order_last_updated = ?
    # WHERE order_id = ?
    # """
    # cursor.execute(update_query, (
    #     order.part_id,
    #     order.part_qty,
    #     order.order_date,
    #     order.due_date,
    #     order.order_last_updated,
    #     order.order_id
    # ))
    last_id_query = "SELECT TOP 1 order_id FROM dbo.Orders$ ORDER BY CAST(SUBSTRING(order_id, 2, LEN(order_id)-1) AS INT) DESC"
    cursor.execute(last_id_query)
    last_id_row = cursor.fetchone()

    if not last_id_row:
        # If no existing BOMs, start with a base ID, e.g., "B001"
        new_order_id = "O0001"
    else:
        last_id = last_id_row[0]
        # Assuming the format "B###", extract the numeric part, increment, and reformat
        prefix, number = last_id[0], int(last_id[1:])
        new_order_id = f"{prefix}{str(number + 1).zfill(4)}"

        order.order_id = new_order_id

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
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"order_id {order_id} not found")
    
    connection.commit()
    response = {
        "message": "Orders updated successfully with new order_id",
        "data": {
            "order_id": order.order_id,
            "part_id": order.part_id,
            "part_qty": order.part_qty,
            "order_date": order.order_date,
            "due_date": order.due_date,
            "order_last_updated": order.order_last_updated
        }
    }
    return response

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
            "message": error_messages["unexpected_error"],
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
    
    # Generate the Workcentre ID
    workcentre_id = f"WC{str(workcentre_counter).zfill(3)}"
    workcentre.workcentre_id = workcentre_id
    workcentre_counter += 1
    
    check_query = "SELECT COUNT(*) FROM dbo.Workcentre$ WHERE workcentre_id = ?"
    cursor.execute(check_query, (workcentre.workcentre_id,))
    count = cursor.fetchone()[0]

    if count > 0:
        # cursor.close()
        # connection.close()
        raise HTTPException(status_code=400, detail="workcentre_id already exists and cannot be added")
    
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

@app.put("/workcentre/{workcentre_id}")
async def update_workcentre(workcentre_id: str, workcentre: WorkCentre):
    # Generate the new BOM_id with a prime symbol
    # new_workcentre_id = workcentre_id + "'"

    # check_query = "SELECT COUNT(*) FROM dbo.Workcentre$ WHERE workcentre_id = ?"
    # cursor.execute(check_query, (new_workcentre_id,))
    # count = cursor.fetchone()[0]

    # if count > 0:
    #     raise HTTPException(status_code=400, detail=f"BOM_id {new_workcentre_id} already exists")
    
    # # Update bom object with new BOM_id
    # workcentre.workcentre_id = new_workcentre_id

    # update_query = """
    # UPDATE dbo.Workcentre$
    # SET workcentre_name = ?, workcentre_description = ?, capacity = ?, capacity_unit = ?, cost_rate_h = ?, workcentre_last_updated = ?
    # WHERE workcentre_id = ?
    # """
    # cursor.execute(update_query, (
    #     workcentre.workcentre_name,
    #     workcentre.workcentre_description,
    #     workcentre.capacity,
    #     workcentre.capacity_unit,
    #     workcentre.cost_rate_h,
    #     workcentre.last_updated_date,
    #     workcentre.workcentre_id
    # ))
    last_id_query = "SELECT TOP 1 order_id FROM dbo.Workcentre$ ORDER BY CAST(SUBSTRING(workcentre_id, 2, LEN(workcentre_id)-1) AS INT) DESC"
    cursor.execute(last_id_query)
    last_id_row = cursor.fetchone()

    if not last_id_row:
        # If no existing BOMs, start with a base ID, e.g., "B001"
        new_workcentre_id = "WC001"
    else:
        last_id = last_id_row[0]
        # Assuming the format "B###", extract the numeric part, increment, and reformat
        prefix, number = last_id[0], int(last_id[1:])
        new_workcentre_id = f"{prefix}{str(number + 1).zfill(3)}"

        workcentre.workcentre_id = new_workcentre_id

    insert_query = """
    INSERT INTO dbo.Workcentre$ (workcentre_id, workcentre_name, workcentre_description, capacity, capacity_unit, cost_rate_h, workcentre_last_updated)
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
            "workcentre_last_updated": workcentre.workcentre_last_updated
        }
    }
    return response

    # "data": workcentre.dict()

    # connection.commit()
    # response = {
    #     "message": "Workcentre updated successfully",
    #     "data": workcentre
    # }
    # return response
    
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
    
