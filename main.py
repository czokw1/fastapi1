# Fast API's Tutorial: https://fastapi.tiangolo.com/tutorial/

from fastapi import FastAPI, Body, Query, Header, Depends, Request
from pydantic import BaseModel, Field
import pyodbc
import os
import uvicorn

SERVER = os.getenv('SERVER')
DATABASE = os.getenv('DATABASE')
USERNAMEFASTAPI = os.getenv('USERNAMEFASTAPI')
PASSWORD = os.getenv('PASSWORD')

# Connect to the database
connect_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAMEFASTAPI};PWD={PASSWORD}'

try: 
    conn = pyodbc.connect(connect_string)
except pyodbc.Error as e:
    print(e)
    exit()


# Function to return the sql results as a dict. 
# It also maps the column names and values for the dict
# Returns no results found if there are no records
def mssql_result2dict(cursor):
    try: 
        result = []
        columns = [column[0] for column in cursor.description]
        for row in  cursor.fetchall():
            result.append(dict(zip(columns,row)))

        # print(result)

        #Check for results
        if len(result) > 0:
            ret = result
        else:
            ret = {"message": "no results found"}
    except pyodbc.Error as e:
        print(e)
        ret = { "message": "Internal Database Query Error"}
    
    return ret


# CLRUD aka CRUD model to update your database
# Create - Create record in the database
# List - List all records
# Read - Read one record
# Update - Update one record
# Delete - Delete one record
class TestTableModel:
    # Database table
    table = "dbo.TestTable"

    # Database view
    view = "dbo.TestTable"
    
    def create(self, data):
        sql = f'INSERT INTO {self.table} ([name],[value],[date], [comment]) OUTPUT INSERTED.id VALUES (?,?,?,?);'
        
        try:
            cursor = conn.cursor()
            row = cursor.execute(sql, data.name, data.value, data.date, data.comment).fetchone()
            conn.commit()
            ret = {"message": "created", "id": row[0]}
        except pyodbc.Error as e:
            print(f'Insert Failed')
            print(e)
            ret = {"message": "failed to create record"}
       
        return ret

    
    def list(self, id = None):
        sql = f'SELECT * FROM {self.view}'
        
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            ret = mssql_result2dict(cursor)
            conn.commit()
        except pyodbc.Error as e:
            print(f'SQL Query Failed: {e}')
            ret = {"message": "system error"}
        
        return ret

    def info(self, id = None):
        sql = f'SELECT @@VERSION'
        
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            ret = mssql_result2dict(cursor)
            conn.commit()
        except pyodbc.Error as e:
            print(f'SQL Query Failed: {e}')
            ret = {"message": "system error"}
        
        return ret

    def read(self, id = None):
        if not id: 
            return {"message": "id not set"}

        sql = f'SELECT * FROM {self.view} WHERE id=?'
        
        try:
            cursor = conn.cursor()
            cursor.execute(sql, id)
            ret = mssql_result2dict(cursor)
            conn.commit()
        except pyodbc.Error as e:
            print(f'SQL Query Failed: {e}')
            ret = {"message": "system error"}
        
        return ret
    
    
    def update(self,id = None, data = None):
        if not id: 
            return {"message": "id not set"}

        sql = f'UPDATE {self.table} set name=?, value=?, date=?, comment=? WHERE id=?'
        
        try:
            cursor = conn.cursor()
            cursor.execute(sql, data.name, data.value, data.date, data.comment, id)
            ret = {"message": "updated"}
            conn.commit()
        except pyodbc.Error as e:
            print(f'SQL Query Failed: {e}')
            ret = {"message": "system error"}
        
        return ret

    def delete(self,id = None):
        if not id: 
            return {"message": "id not set"}

        sql = f'DELETE FROM {self.table} WHERE id=?'
        
        try:
            cursor = conn.cursor()
            cursor.execute(sql, id)
            ret = {"message": "deleted"}
            conn.commit()
        except pyodbc.Error as e:
            print(f'SQL Query Failed: {e}')
            ret = {"message": "system error"}
        
        return ret
        


# Create the FastAPI App   
app = FastAPI(
    title="Fast API SQL",
    description="Test SQL Connections",
    version="1.0"
)


# Class used to parse the body of POST and PUT Requests - pydantic
class TestTable_Body(BaseModel):
    name: str = None
    value: str = None
    date: str = None
    comment: str = None
    

# FastAPI Endpoints
# tag groups api calls in the swagger UI
# If you want to include query Paramaters: https://fastapi.tiangolo.com/tutorial/query-params/
@app.get("/", tags=['testing'])
async def testdb():
    t = TestTableModel()
    return t.info()

@app.get("/drivers", tags=['testing'])
async def testdb():
    t = TestTableModel()
    return pyodbc.drivers()

@app.post("/testtable", tags=['testing'])
async def testtable_create(data: TestTable_Body, request: Request):
    t = TestTableModel()
    return t.create(data)

@app.get("/testtable/{id}", tags=['testing'])
async def testtable_read(id: int = None):
    t = TestTableModel()
    return t.read(id)

@app.get("/testtable", tags=['testing'])
async def testtable_list():
    t = TestTableModel()
    return t.list()

@app.put("/testtable/{id}", tags=['testing'])
async def testtable_update(data: TestTable_Body, id: int = None):
    t = TestTableModel()
    return t.update(id, data)

@app.delete("/testtable/{id}", tags=['testing'])
async def testtable_delete(id: int = None):
    t = TestTableModel()
    return t.delete(id)

if __name__ == '__main__':
    uvicorn.run(app, port=8000)
