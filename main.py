import os
import csv
import auth
import logging
import pandas as pd

from io import StringIO
from datetime import date
from services import download_csv
from clickhouse_driver import Client
from typing import Optional, List, Dict
from fastapi.security.api_key import APIKey
from fastapi import FastAPI, HTTPException, Query, Depends, Body, Request
from config import Settings, URLItem, get_settings, QueryModel, AggregateQueryModel


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
app = FastAPI()

# clickhouse connection
def get_client():
    settings = get_settings()
    client = Client(
        settings.CC_HOST,
        user=settings.CC_USER,
        password=settings.CC_PWD,
        secure=settings.CC_SECURE,
        compression=settings.CC_COMPRESSION
    )
    return client

# get batch size
def get_batch_size():
    settings = get_settings()
    return settings.BATCH_SIZE

#  app logic
# sanitize column names
def sanitize_column_name(column_name: str) -> str:
    return column_name.replace(" ", "_").replace("-", "_").lower()


def create_table(client: Client):
    client.execute("""
    CREATE TABLE IF NOT EXISTS test_db2 (
        appid UInt32,
        name String,
        release_date Date,
        required_age UInt8,
        price Float32,
        dlc_count UInt32,
        about_the_game String,
        supported_languages String,
        windows Bool,
        mac Bool,
        linux Bool,
        positive UInt32,
        negative UInt32,
        score_rank UInt32,
        developers String,
        publishers String,
        categories String,
        genres String,
        tags String
    ) ENGINE = MergeTree() ORDER BY (appid)
    """)


def insert_into_clickhouse(df: pd.DataFrame, client: Client):
    headers = df.columns
    #  remove the first column if it is unnamed
    if headers[0] == "unnamed:_0":
        headers = headers[1:]
        df = df.iloc[:, 1:]

    df['appid'] = df['appid'].astype(int)
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce').fillna(date(2000, 1, 1))
    df['required_age'] = df['required_age'].fillna(0).astype(int)
    df['price'] = df['price'].fillna(0).astype(float)
    df['dlc_count'] = df['dlc_count'].fillna(0).astype(int)
    df['windows'] = df['windows'].astype(bool)
    df['mac'] = df['mac'].astype(bool)
    df['linux'] = df['linux'].astype(bool)
    df['positive'] = df['positive'].fillna(0).astype(int)
    df['negative'] = df['negative'].fillna(0).astype(int)
    df['score_rank'] = df['score_rank'].fillna(0).astype(int)

    create_table(client)
    columns_to_convert = ['name', 'about_the_game', 'supported_languages', 'developers', 'publishers', 'categories', 'genres', 'tags']
    for col in columns_to_convert:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    data_tuples = [tuple(x) for x in df.to_numpy()]

    batch_size = get_batch_size()
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i + batch_size]
        client.execute("INSERT INTO test_db2 VALUES", batch)
        logging.debug(f"Inserted batch | {i//batch_size + 1}")


@app.post("/upload")
async def upload_csv(
    url: URLItem,
    api_key: APIKey = Depends(auth.get_api_key),
    client: Client = Depends(get_client)
):
    try:
        data = await download_csv(url.url)
        df = pd.read_csv(StringIO(data))
        df.columns = [sanitize_column_name(col) for col in df.columns]
        insert_into_clickhouse(df, client)
    except Exception as e:
        logging.error(f"Error | upload service | {e}")
        raise HTTPException(status_code=500, detail=str(e))
    return {"data": data}


@app.get("/query")
async def query_data(
    client: Client = Depends(get_client), 
    api_key: APIKey = Depends(auth.get_api_key),
    app_id: Optional[int] = Query(None, description="Exact match for AppId"),
    name: Optional[str] = Query(None, description="Substring match for Name"),
    release_date: Optional[str] = Query(None, description="Exact match for Release Date (YYYY-MM-DD)"),
    release_date_gt: Optional[str] = Query(None, description="Records with Release Date greater than (YYYY-MM-DD)"),
    release_date_lt: Optional[str] = Query(None, description="Records with Release Date less than (YYYY-MM-DD)"),
    required_age: Optional[int] = Query(None, description="Exact match for Required Age"),
    price: Optional[float] = Query(None, description="Exact match for Price"),
    dlc_count: Optional[int] = Query(None, description="Exact match for DLC Count"),
    about_the_game: Optional[str] = Query(None, description="Substring match for About the Game"),
    supported_languages: Optional[str] = Query(None, description="Substring match for Supported Languages"),
    windows: Optional[bool] = Query(None, description="Exact match for Windows support"),
    mac: Optional[bool] = Query(None, description="Exact match for Mac support"),
    linux: Optional[bool] = Query(None, description="Exact match for Linux support"),
    positive: Optional[int] = Query(None, description="Exact match for Positive reviews count"),
    negative: Optional[int] = Query(None, description="Exact match for Negative reviews count"),
    score_rank: Optional[int] = Query(None, description="Exact match for Score Rank"),
    developers: Optional[str] = Query(None, description="Substring match for Developers"),
    publishers: Optional[str] = Query(None, description="Substring match for Publishers"),
    categories: Optional[str] = Query(None, description="Substring match for Categories"),
    genres: Optional[str] = Query(None, description="Substring match for Genres"),
    tags: Optional[str] = Query(None, description="Substring match for Tags"),
    aggregate: Optional[str] = Query(None, description="Aggregate operation (e.g., sum_x, max_price, min_age)")
):
    try:
        # Constructing the WHERE clause dynamically based on provided parameters
        conditions = []
        select_fields = "*"

        if aggregate:
            if aggregate.startswith("sum_"):
                column_name = aggregate.split("_", 1)[1]
                select_fields = f"SUM({column_name.lower()}) AS {aggregate}"
            elif aggregate.startswith("max_"):
                column_name = aggregate.split("_", 1)[1]
                select_fields = f"MAX({column_name.lower()}) AS {aggregate}"
            elif aggregate.startswith("min_"):
                column_name = aggregate.split("_", 1)[1]
                select_fields = f"MIN({column_name.lower()}) AS {aggregate}"
            elif aggregate.startswith("avg_"):
                column_name = aggregate.split("_", 1)[1]
                select_fields = f"AVG({column_name.lower()}) AS {aggregate}"
            else:
                raise HTTPException(status_code=400, detail="Invalid aggregate operation")
  

        if app_id is not None:
            conditions.append(f"appid = {app_id}")
        if name is not None:
            conditions.append(f"lower(name) LIKE '%{name.lower()}%'")
        if release_date is not None:
            conditions.append(f"release_date = '{release_date}'")
        if release_date_gt is not None:
            conditions.append(f"release_date > '{release_date_gt}'")
        if release_date_lt is not None:
            conditions.append(f"release_date < '{release_date_lt}'")
        if required_age is not None:
            conditions.append(f"required_age = {required_age}")
        if price is not None:
            conditions.append(f"price = {price}")
        if dlc_count is not None:
            conditions.append(f"dlc_count = {dlc_count}")
        if about_the_game is not None:
            conditions.append(f"lower(about_the_game) LIKE '%{about_the_game.lower()}%'")
        if supported_languages is not None:
            conditions.append(f"lower(supported_languages) LIKE '%{supported_languages.lower()}%'")
        if windows is not None:
            conditions.append(f"windows = {windows}")
        if mac is not None:
            conditions.append(f"mac = {mac}")
        if linux is not None:
            conditions.append(f"linux = {linux}")
        if positive is not None:
            conditions.append(f"positive = {positive}")
        if negative is not None:
            conditions.append(f"negative = {negative}")
        if score_rank is not None:
            conditions.append(f"score_rank = {score_rank}")
        if developers is not None:
            conditions.append(f"lower(developers) LIKE '%{developers.lower()}%'")
        if publishers is not None:
            conditions.append(f"lower(publishers) LIKE '%{publishers.lower()}%'")
        if categories is not None:
            conditions.append(f"lower(categories) LIKE '%{categories.lower()}%'")
        if genres is not None:
            conditions.append(f"lower(genres) LIKE '%{genres.lower()}%'")
        if tags is not None:
            conditions.append(f"lower(tags) LIKE '%{tags.lower()}%'")
        
        print("condition: ",conditions)
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
        else:
            where_clause = ""

        query = f"SELECT {select_fields} FROM test_db2 {where_clause}"
        
        logging.debug(f"Query: {query}")
        result = client.execute(query)
        return {"data": result}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# endpoint to check the server health
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "message": "Server is running."
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


# https://drive.usercontent.google.com/u/0/uc?id=1ZIMlNQ226uaFvTId8ineajrBmWdQB2Ew&export=download
