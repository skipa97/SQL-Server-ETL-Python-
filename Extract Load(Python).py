#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyodbc
import pandas as pd
import os
import urllib
from sqlalchemy import create_engine
import sqlalchemy
import sqlite3
from sqlite3 import Error


# Creating a connection to sql and executing a select statement to extract the data

# In[ ]:


def extract():
    try:
        sqlconn = pyodbc.connect(r'Driver=SQL Server;Server=.\SQLEXPRESS;Database=Test1;Trusted_Connection=yes;')
        cursor = sqlconn.cursor()
        #sqlconn.setdecoding(dbo.SQL_CHAR, encoding='latin1')
        #sqlconn.setencoding('latin1')
        cursor.execute("""SELECT t.[name] as table_name FROM sys.tables t where t.[name] in ('employee','People') """)
        
        cursor_tables = cursor.fetchall()
        
        for tbl in cursor_tables:
            df = pd.read_sql_query(f'select * from {tbl[0]}', sqlconn)
            load(df, tbl[0])
            
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        sqlconn.close()
        
            


# creating sql connection using SQLAlchemy so we can load the data to the new server

# In[ ]:


connection_string = "mssql+pyodbc://localhost/Test2?driver=SQL Server?Trusted_Connection=yes"

engine = create_engine(connection_string)
connection = engine.connect()


# we load the data to the new database/server

# In[ ]:


def load(df, tbl):
    try:
        rows_imported = 0
        load_conn = pyodbc.connect(f'Driver=SQL Server;Server=.\SQLEXPRESS;Database=Test1;Trusted_Connection=yes;')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        
        df.to_sql(f'stg_{tbl}',connection, if_exists='replace', index=False)
        rows_imported += len(df)
        load_conn.commit()
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
   
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))

