from fastapi import FastAPI, HTTPException
from fastapi import Body
from pydantic import BaseModel
import requests
from requests.auth import HTTPBasicAuth
import os
from datetime import datetime
import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv, dotenv_values
import os
load_dotenv()

app = FastAPI()

# Replace with your actual key_id and key_secret
key_id = os.getenv('KEY_ID')
key_secret = os.getenv('KEY_SECRET')



client = clickhouse_connect.get_client(
    host= os.getenv('Clickhouse_Host'),
    user=os.getenv('Clickhouse_User'),
    password=os.getenv('Clickhouse_Password'),
    secure=True
)

# Pydantic model for user input
class QueryParams(BaseModel):
    postcode1_filter: str = ''
    postcode2_filter: str = ''
    start_date_filter: str = ''
    end_date_filter: str = datetime.today().strftime('%Y-%m-%d')  # Default to today
    property_type_filter: str = ''
    town_filter: str = ''
    county_filter: str = ''
    skip: int = 0
    limit: int = 3000
    page: int= 1

# Pydantic model for user input
class QueryParams2(BaseModel):
    postcode1_filter: str = None
    postcode2_filter: str = None
    start_date_filter: str = None
    end_date_filter: str = datetime.today().strftime('%Y-%m-%d')  # Default to today
    property_type_filter: str = None
    town_filter: str = None
    county_filter: str = None
    skip: int = 0
    limit: int = 3000
    page: int= None

@app.post("/unified")
async def run_query(params: QueryParams):
    # Prepare payload with query variables
    if params.page:
        params.skip = (params.page - 1)* params.limit
    payload = {
        "queryVariables": params.dict(exclude={'page'})
    }

    # Endpoint URL
    url = 'https://console-api.clickhouse.cloud/.api/query-endpoints/a21aabf9-37ff-43e7-8121-49eddc351222/run?format=CSVWithNames'

    try:
        # Make the POST request with basic authentication and JSON data
        response = requests.post(
            url,
            auth=HTTPBasicAuth(key_id, key_secret),
            json=payload,
            headers={"Content-Type": "application/json"}
        )

        # Check response status and return the result
        if response.status_code == 200:
            return response.content.decode('utf-8')  # Return CSV content as a string
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/prices")
async def get_prices(limit: int = 10, skip: int = 0):
    try:
        """
        GET request to fetch rows from 'uk_price_paid' table with the specified limit and skip.
        """
        # query
        q1 = f"SELECT * FROM default.uk_price_paid ORDER BY date DESC LIMIT {limit} OFFSET {skip};"
        # Executing the query using the client (you should replace this with the actual query execution)
        result = client.query(q1)
        # Extracting column names and rows
        columns = result.column_names
        rows = result.result_rows
        
        # Converting to pandas DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Returning the DataFrame in a JSON-like format
        return {
            "data": df.to_dict(orient="records"),  # Convert DataFrame rows to a list of dictionaries
            "returned_rows": len(rows),
            "summary": result.summary
        }
    except Exception as e:
        #Catch any general error, return 500 status code with error message
        raise HTTPException(status_code=500, detail=str(e))

# To run the FastAPI application, use:
# uvicorn your_filename:app --reload
