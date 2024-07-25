from dotenv import load_dotenv
import os
import pyodbc, struct
from azure import identity

from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel


load_dotenv()
connection_string  = ("Driver={ODBC Driver 18 for SQL Server};Server=tcp:msodbsrv.database.windows.net,1433;Database=MSO test 1;Uid=msoadmin;Pwd=msopassword123!;")

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()
cursor.execute("SELECT child_id FROM dbo.BOM$ WHERE parent_id='P001'")
print(cursor.fetchall())

app = FastAPI()
print("Server Running")


@app.get("/items/{id}")
async def root(id: str):
    cursor.execute(f"SELECT * FROM dbo.BOM$")
    children = cursor.fetchall()
    return str(children[0])
