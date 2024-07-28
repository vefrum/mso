import datetime
from dotenv import load_dotenv 
import os 
import pyodbc 
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import requests
from io import StringIO
from pydantic import BaseModel
from datetime import date, datetime

# Global counter for Workcentre ID (this part need to change to get the highest workcentre ID then +1 to it)
workcentre_counter = 5
bom_counter = 470

class WorkCentre(BaseModel):
    workcentre_name: str
    capacity_unit: str
    cost_rate_h: float
    workcentre_description: str
    capacity: int
    last_updated_date: date
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
    child_qty: int
    child_leadtime: int
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
    cursor.close()
    connection.close()

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
            #raise HTTPException(status_code=409, detail=error_messages["referenced_entry"])
            return {
                "message": "Cannot delete BOM entry because it is referenced by Routings$ table.",
                "BOM_id": BOM_id
            }

        # Delete BOM entry
        delete_query = "DELETE FROM dbo.BOM$ WHERE BOM_id = ?"
        cursor.execute(delete_query, (BOM_id,))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail=error_messages["bom_not_found"])
        
        # Commit the transaction
        connection.commit()

        # If no exceptions, return success response
        response = {
            "message": "BOM deleted successfully",
            "BOM_id": BOM_id
        }
        
        return response
       
    except pyodbc.IntegrityError:
        return error_messages["integrity_error"]
    except pyodbc.DatabaseError:
        raise HTTPException(status_code=500, detail=error_messages["database_error"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{error_messages['unexpected_error']}: {str(e)}")

    # Default response if an error occurs
    # response = {
    #     "message": error_messages["unexpected_error"],
    #     "BOM_id": BOM_id
    # }
    # return response

@app.put("/BOM/{BOM_id}")
async def update_bom(BOM_id: str, bom: BOM):
    update_query = """
    UPDATE dbo.BOM$
    SET part_id = ?, child_id = ?, child_qty = ?, child_leadtime = ?, BOM_last_updated = ?
    WHERE BOM_id = ?
    """
    cursor.execute(update_query, (
        bom.part_id,
        bom.child_id,
        bom.child_qty,
        bom.child_leadtime,
        bom.BOM_last_updated,
        BOM_id
    ))
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"BOM_id {BOM_id} not found")
    
    connection.commit()
    response = {
        "message": "BOM updated successfully",
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
    cursor.close()
    connection.close()

    response = {
        "message": "Routing created successfully",
        "data": routing
    }
    return response

@app.put("/routings/{routing_id}")
async def update_routing(routing_id: str, routing: Routing):
    update_query = """
    UPDATE dbo.Routings$
    SET BOM_id = ?, operations_sequence = ?, workcentre_id = ?, process_description = ?, setup_time = ?, runtime = ?, routings_last_update = ?
    WHERE routing_id = ?
    """
    cursor.execute(update_query, (
        routing.BOM_id,
        routing.operations_sequence,
        routing.workcentre_id,
        routing.process_description,
        routing.setup_time,
        routing.runtime,
        routing.routings_last_update,
        routing.routing_id
    ))

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Routing not found")
    
    connection.commit()
    response = {
        "message": "Routing updated successfully",
        "data": routing
    }
    return response
 
@app.delete("/routing/{routing_id}")
async def delete_routing(routing_id: str):
    delete_query = "DELETE FROM dbo.Routings$ WHERE routing_id = ?"
    cursor.execute(delete_query, routing_id)
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Routing not found")
    connection.commit()

    response = {
        "message": "Routing deleted successfully",
        "routing_id": routing_id
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

@app.put("/partmasterrecords/{part_id}")
async def update_part(part_id: str, part: Part):
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

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Part not found")
    
    connection.commit()
    response = {
        "message": "Part updated successfully",
        "data": part
    }
    return response

@app.delete("/partmasterrecords/{part_id}")
async def delete_part(part_id: str):
    delete_query = "DELETE FROM dbo.Part_Master_Records$ WHERE part_id = ?"
    cursor.execute(delete_query, part_id)
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Part not found")
    connection.commit()

    response = {
        "message": "Part deleted successfully",
        "part_id": part_id
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
    cursor.close()
    connection.close()

    response = {
        "message": "Order created successfully",
        "data": order
    }
    return response

@app.put("/orders/{order_id}")
async def update_order(order_id: str, order: Order):
    update_query = """
    UPDATE dbo.Orders$
    SET part_id = ?, part_qty = ?, order_date = ?, due_date = ?, order_last_updated = ?
    WHERE order_id = ?
    """
    cursor.execute(update_query, (
        order.part_id,
        order.part_qty,
        order.order_date,
        order.due_date,
        order.order_last_updated,
        order.order_id
    ))

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Order not found")
    
    connection.commit()
    response = {
        "message": "Order updated successfully",
        "data": order
    }
    return response

@app.delete("/orders/{order_id}")
async def delete_order(order_id: str):
    delete_query = "DELETE FROM dbo.Orders$ WHERE order_id = ?"
    cursor.execute(delete_query, order_id)
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Order not found")
    connection.commit()
    
    response = {
        "message": "Order deleted successfully",
        "order_id": order_id
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

@app.put("/workcentre/{workcentre_id}")
async def update_workcentre(workcentre_id: str, workcentre: WorkCentre):
    update_query = """
    UPDATE dbo.Workcentre$
    SET workcentre_name = ?, workcentre_description = ?, capacity = ?, capacity_unit = ?, cost_rate_h = ?, workcentre_last_updated = ?
    WHERE workcentre_id = ?
    """
    cursor.execute(update_query, (
        workcentre.workcentre_name,
        workcentre.workcentre_description,
        workcentre.capacity,
        workcentre.capacity_unit,
        workcentre.cost_rate_h,
        workcentre.last_updated_date,
        workcentre.workcentre_id
    ))

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Workcentre not found")
    
    connection.commit()
    response = {
        "message": "Workcentre updated successfully",
        "data": workcentre
    }
    return response
    
@app.delete("/workcentre/{workcentre_id}")
async def delete_workcentre(workcentre_id: str):
    delete_query = "DELETE FROM dbo.Workcentre$ WHERE workcentre_id = ?"
    cursor.execute(delete_query, workcentre_id)
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="WorkCentre not found")
    connection.commit()

    response = {
        "message": "Workcentre deleted successfully",
        "workcentre_id": workcentre_id
    }
    return response

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

##############################################
@app.post("/test_delete_bom")
async def test_delete_bom():
    # Insert sample BOM entry for testing
    sample_bom = BOM(
        BOM_id="B001",
        part_id="P001",
        child_id="P009",
        child_qty=0.5,
        child_leadtime=10,
        BOM_last_updated=datetime.now()
    )

    # Insert the sample BOM entry
    await create_bom(sample_bom)

    # Delete the BOM entry with BOM_id B001
    response = await delete_bom("B001")
    return response








    
####################################################################################################

# def validate_item(item: dict, disallowed_keys: list):
#     for key in disallowed_keys:
#         if key in item:
#             raise HTTPException(status_code=400, detail=f"Adding or updating {key} is not allowed")

# # Endpoint to add an item
# @app.post("/addItem")
# async def add_item(item: dict):
#     # List of disallowed keys
#     disallowed_keys = ['parent_id', 'other_disallowed_key']
    
#     # Validate the item
#     validate_item(item, disallowed_keys)
    
#     columns = ', '.join(item.keys())
#     values = ', '.join([f"'{v}'" for v in item.values()])
#     query = f"INSERT INTO dbo.YourTableName ({columns}) VALUES ({values})"
    
#     cursor.execute(query)
#     connection.commit()
    
#     return {"message": "Item added successfully"}

# # Endpoint to update an item
# @app.put("/updateItem/{id}")
# async def update_item(id: int, item: dict):
#     # List of disallowed keys
#     disallowed_keys = ['parent_id', 'other_disallowed_key']
    
#     # Validate the item
#     validate_item(item, disallowed_keys)
    
#     set_clause = ', '.join([f"{k} = '{v}'" for k, v in item.items()])
#     query = f"UPDATE dbo.YourTableName SET {set_clause} WHERE id = {id}"
    
#     cursor.execute(query)
#     connection.commit()
    
#     return {"message": "Item updated successfully"}

# # Endpoint to delete an item
# @app.delete("/deleteItem/{id}")
# async def delete_item(id: int):
#     query = f"DELETE FROM dbo.YourTableName WHERE id = {id}"
    
#     cursor.execute(query)
#     connection.commit()
    
#     return {"message": "Item deleted successfully"}
