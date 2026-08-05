"""
FastAPI backend for the Fleet Telematics dashboard.

Routes:
    GET /                     -> Home page
    GET /driver-stats         -> Driver Dashboard
    GET /vehicle-stats        -> Vehicle Dashboard

APIs:
    GET /api/drivers
    GET /api/drivers-dynamo
    GET /api/vehicles
    GET /api/vehicles-dynamo
    GET /api/health

Run:
    uvicorn dashboard.app:app --reload --port 8000
"""

import os
from decimal import Decimal

import boto3
import pymysql
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.requests import Request
from fastapi.templating import Jinja2Templates

load_dotenv()


RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER", "admin")
RDS_PASSWORD = os.getenv("RDS_PASSWORD","")
RDS_DATABASE = os.getenv("RDS_DATABASE", "fleet_telematics_eng2")

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# DynamoDB Tables
DRIVER_DYNAMO_TABLE = "DriverScore"
VEHICLE_DYNAMO_TABLE = "VehicleAlerts"

app = FastAPI(title="Fleet Telematics Dashboard")

templates = Jinja2Templates(directory="dashboard/templates")

dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION
)


def decimal_to_native(obj):
    """Convert DynamoDB Decimal values into Python int/float."""

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
def home(request: Request):
    return templates.TemplateResponse(
        request,
        "home.html"
    )


@app.get("/driver-stats", response_class=HTMLResponse)
def driver_dashboard(request: Request):
    return templates.TemplateResponse(
        request,
        "driver_dashboard.html"
    )


@app.get("/vehicle-stats", response_class=HTMLResponse)
def vehicle_dashboard(request: Request):
    return templates.TemplateResponse(
        request,
        "vehicle_dashboard.html"
    )


@app.get("/api/drivers")
def get_drivers():

    conn = get_connection()

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT *
                FROM DriverPerformance
                ORDER BY deviceID ASC
            """)

            rows = cursor.fetchall()

    finally:
        conn.close()

    return {"drivers": rows}


@app.get("/api/drivers-dynamo")
def get_drivers_dynamo():

    table = dynamodb.Table(DRIVER_DYNAMO_TABLE) # type: ignore

    response = table.scan()

    items = response.get("Items", [])

    items = decimal_to_native(items)

    items.sort(key=lambda d: int(d["deviceID"])) # type: ignore

    return {"drivers": items}



@app.get("/api/vehicles")
def get_vehicles():

    conn = get_connection()

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT *
                FROM VehicleHealthReport
                ORDER BY deviceID ASC
            """)

            rows = cursor.fetchall()

    finally:
        conn.close()

    return {"vehicles": rows}


@app.get("/api/vehicles-dynamo")
def get_vehicles_dynamo():

    table = dynamodb.Table(VEHICLE_DYNAMO_TABLE) # type: ignore

    response = table.scan()

    items = response.get("Items", [])

    items = decimal_to_native(items)

    items.sort(key=lambda d: int(d["deviceID"])) #type: ignore

    return {"vehicles": items}


@app.get("/api/health")
def health_check():

    try:

        conn = get_connection()

        conn.close()

        return {
            "status": "ok",
            "rds": "connected"
        }

    except Exception as e:

        return {
            "status": "error",
            "detail": str(e)
        }