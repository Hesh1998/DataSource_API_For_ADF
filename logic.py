# Import dependencies
from dotenv import load_dotenv
from datetime import datetime
import json
import pyodbc
import pandas as pd
import os


def get_customer_data_from_sourceDB(date):
    # Load environment variables from .env
    load_dotenv()

    # Details requred to execute a SQL query in the data warehouse (Azure Dedicated SQL Pool)
    # Username and password are extracted from the .env file
    server = 'heshtestdwhserver.database.windows.net'
    database = 'hesh-sourceDB'
    username = os.getenv("SOURCE_DB_USR")
    password = os.getenv("SOURCE_DB_PWD")
    driver= '{ODBC Driver 17 for SQL Server}'
    
    # Runs the SQL query against the data warehouse and returns the reponse
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            query = f"SELECT [Name], [Country], [CreatedDate] FROM [Source_Dim].[Customer] WHERE [CreatedDate] = '{date}';"
            cursor.execute(query)

            # Gets the column names from the description attribute
            column_names = [column[0] for column in cursor.description]
            
            # Creates an empty DataFrame with column names
            df = pd.DataFrame(columns=column_names)

            # Access each row one-by-one
            row = cursor.fetchone()
            while row:
                row_data = {}
                
                # Access individual columns of the row by index
                for i in range(len(column_names)):
                    # Appends a row to row_data dictionary
                    if(column_names[i] == "CreatedDate"):
                        row_data[column_names[i]] = row[i].strftime("%Y-%m-%d")
                    else:
                        row_data[column_names[i]] = row[i]
                    
                
                # Appends each finalized row to the data frame and moves to the next row
                df = df._append(row_data, ignore_index=True)
                row_data.clear()
                row = cursor.fetchone()

            # Converts the DataFrame to JSON format and returns the response
            json_response = df.to_json(orient='records')
            print(json_response)
            return json_response
