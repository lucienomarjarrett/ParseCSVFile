import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv('SERVER')
DATABASE = os.getenv('DATABASE')
USER = os.getenv('USER')
PWD = os.getenv('PWD')
DRIVER = '{ODBC Driver 17 for SQL Server}'


def connection_str(driver=None, server=None, database=None, user_id=None, password=None) -> str:
    cnxn = ""
    try:
        if driver is None and database is None and database is None and user_id is None and password is None:
            cnxn = pyodbc.connect('DRIVER=' + DRIVER +
                                  ';SERVER=' + SERVER +
                                  ';DATABASE=' + DATABASE +
                                  ';UID=' + USER +
                                  ';PWD=' + PWD)
        else:
            cnxn = pyodbc.connect('DRIVER=' + driver +
                                  ';SERVER=' + server +
                                  ';DATABASE=' + database +
                                  ';UID=' + user_id +
                                  ';PWD=' + password)

        return cnxn
    except:
        cnxn = 'Cannot connect to SQL server'
        return cnxn


def insert_row(cursor, cls, type,account_number,account_description,trans_date,transaction_description,source,je_,amount,cumulative, batch_id):
    # print(  cls, type, account_number, account_description, trans_date, transaction_description, source, je_, amount, cumulative, batch_id )

    insert_query = " INSERT INTO [dbo].[Account_DataTemp] \
       ([class] \
       ,[type] \
       ,[account_num] \
       ,[account_des] \
       ,[trans_date] \
       ,[description] \
       ,[source] \
       ,[JE#] \
       ,[Amount] \
       ,[Cumulative] \
       ,[batch_id] \
       ) VALUES (?, ?, ?, ?,?, ?,?,?,?,?,?)"

    rows_to_insert = (cls, type,account_number,account_description,trans_date,transaction_description,source,je_,amount,cumulative, batch_id)
    cursor.execute(insert_query, rows_to_insert)



