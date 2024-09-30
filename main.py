from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi import Body
from pydantic import BaseModel
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import clickhouse_connect
import pandas as pd
from io import StringIO
import psycopg2
import sqlalchemy
from psycopg2 import Error
from sqlalchemy import create_engine
from dotenv import load_dotenv, dotenv_values
from fastapi import Depends, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import hashlib
import os
from decimal import Decimal
load_dotenv()

##========

app = FastAPI()

# Replace with your actual key_id and key_secret
key_id = os.getenv('KEY_ID')
key_secret = os.getenv('KEY_SECRET')
# PostgreSQL connection details
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_DATABASE')


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

# # Pydantic model for user input
# class QueryParams2(BaseModel):
#     postcode1_filter: str = None
#     postcode2_filter: str = None
#     start_date_filter: str = None
#     end_date_filter: str = datetime.today().strftime('%Y-%m-%d')  # Default to today
#     property_type_filter: str = None
#     town_filter: str = None
#     county_filter: str = None
#     skip: int = 0
#     limit: int = 3000
#     page: int= None


# Setup database engine (SQLite example)
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

def hash_api_key(api_key : str):
    return hashlib.sha256(api_key.encode()).hexdigest()
# User authentication function
def authenticate_user(api_key: str):
    hashed_key = hash_api_key(api_key)
    query = "SELECT * FROM users WHERE apikey = :apikey"
    # if username:
    #     query += " AND username = :username"

    with engine.connect() as conn:
        user = conn.execute(text(query), {"apikey": hashed_key}).fetchone()

    if not user:
        print('false')
        return False
        raise HTTPException(status_code=401, detail="Invalid API key or user")
    
    return user[0], user[1], user[3]  # Returns user row



def deduct_credits(engine, user_id: int, row_count: int):
    points_to_deduct = Decimal(row_count /10000) # For example, 1 credit point per row returned
    with Session(engine) as session:
        # Fetch the user record and their current credit points
        user = session.execute(
            text("SELECT credit_points FROM users WHERE id = :id"), {"id": user_id}
        ).fetchone()

        # Check if the user exists and has enough credit points
        if user and user[0] >= points_to_deduct:
            new_credits = user[0] - points_to_deduct
            # Update user's credit points
            session.execute(
                text("UPDATE users SET credit_points = :new_credits WHERE id = :user_id"),
                {"new_credits": new_credits, "user_id": user_id}
            )
            # Commit the transaction to save the changes
            session.commit()
            print('Credits deducted successfully.')
        else:
            # If the user does not exist or has insufficient credits
            raise HTTPException(status_code=402, detail="Insufficient credit points")



def check_user_credits(engine, user_id: int):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT credit_points FROM users WHERE id = :user_id"), {"user_id": user_id}).fetchone()
        if result:
            print(f"User ID {user_id} has {result[0]} credit points.")
        else:
            print(f"User ID {user_id} not found.")


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
            csv_data = StringIO(response.content.decode('utf-8') )  # Return CSV content as a string
            df = pd.DataFrame(csv_data)
            row_len = len(df)
            return
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/prices")
async def get_prices( limit: int = 1000, skip: int = 0 , authorization:str = Header(None)):
    
    if authorization is None:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    api_key = authorization.split(" ")[1] if " " in authorization else authorization
    
    try:
        """
        GET request to fetch rows from 'uk_price_paid' table with the specified limit and skip.
        """
        # query
        q1 = f"SELECT * FROM default.uk_price_paid ORDER BY date DESC LIMIT {limit} OFFSET {skip};"
        # Executing the query using the client (you should replace this with the actual query execution)

        auth = authenticate_user(api_key)
        if auth:
            user_id = auth[0]

            result = client.query(q1)
            # Extracting column names and rows
            columns = result.column_names
            rows = result.result_rows
            deduct_credits(engine,user_id,len(rows))
                # Converting to pandas DataFrame
            df = pd.DataFrame(rows, columns=columns)
        else:
            return  "not authenticated"


        # Returning the DataFrame in a JSON-like format
        return {
            "data": df.to_dict(orient="records"),  # Convert DataFrame rows to a list of dictionaries
            "returned_rows": len(rows),
            "summary": result.summary
        }
    except Exception as e:
        #Catch any general error, return 500 status code with error message
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/credit_balance")
async def credit_balance(authorization:str = Header(None)):
    if authorization is None:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    api_key = authorization.split(" ")[1] if " " in authorization else authorization
    print(api_key)
    auth = authenticate_user(api_key)
    print(auth)
    if auth:
        bal = engine.connect().execute(text("SELECT credit_points, username FROM users WHERE id = :id"), {"id": auth[0]}).fetchone()
        return {'username':bal[1], 'credit_balance': bal[0]}
    return {'error':"invalid api key"}
# To run the FastAPI application, use:
# uvicorn your_filename:app --reload
