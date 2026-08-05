"""
FastAPI backend for the Fleet Telematics dashboard.

Serves two API endpoints reading directly from RDS:
    GET /api/drivers   -> DriverPerformance (UC-2)
    GET /api/vehicles  -> VehicleHealthReport (UC-1)

And the dashboard page itself at GET /.

Run:
    uvicorn dashboard.app:app --reload --port 8000
Then open http://localhost:8000
"""

import os
from decimal import Decimal

import boto3
import pymysql
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

load_dotenv()

RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER", "admin")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DATABASE = os.getenv("RDS_DATABASE", "fleet_telematics_eng2")

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
DYNAMO_TABLE_NAME = "DriverScore"

app = FastAPI(title="Fleet Telematics Dashboard")
templates = Jinja2Templates(directory="dashboard/templates")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)


def decimal_to_native(obj):
    """Recursively converts DynamoDB's Decimal types to plain
    int/float so FastAPI's JSON encoder can serialize them."""
    if isinstance(obj, list):
        return [decimal_to_native(v) for v in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def get_connection():
    return pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/api/drivers")
def get_drivers():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM DriverPerformance ORDER BY deviceID ASC")
            rows = cursor.fetchall()
    finally:
        conn.close()
    return {"drivers": rows}


@app.get("/api/drivers-dynamo")
def get_drivers_dynamo():
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    response = table.scan()
    items = response.get("Items", [])
    items = decimal_to_native(items)
    items.sort(key=lambda d: int(d["deviceID"]))
    return {"drivers": items}


@app.get("/api/health")
def health_check():
    """Quick check that the API itself and the RDS connection both work."""
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok", "rds": "connected"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}