# ═══════════════════════════════════════════════════════════════
#  VICIdial Dashboard API
#  v7.0.0 — Call Analysis Removed
# ═══════════════════════════════════════════════════════════════

from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile, File, Depends, status, APIRouter
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.encoders import jsonable_encoder
from jose import JWTError, jwt
import mysql.connector
from mysql.connector import Error
from datetime import datetime, date, timedelta, timezone
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import pymysql
import pymysql.cursors
import psycopg2
import psycopg2.extras
import time
import threading
import requests
import pandas as pd
import io
from typing import Optional, List
from psycopg2 import pool
import asyncio
import httpx
from twilio.rest import Client
import os
import re
import json
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

# ─────────────────────────────────────────────
# AUTH CONFIG
# ─────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM  = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# ─────────────────────────────────────────────
# MYSQL CONFIG (VICIdial)
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "192.168.15.165",
    "user":     "cron",
    "password": "1234",
    "database": "asterisk"
}

MYSQL_DB = {
    "host":     "192.168.15.165",
    "port":     3306,
    "user":     "cron",
    "password": "1234",
    "database": "asterisk",
}

# ─────────────────────────────────────────────
# POSTGRESQL CONFIG
# ─────────────────────────────────────────────
POSTGRES_DB = {
    "host":     "192.168.15.105",
    "port":     5432,
    "user":     "postgres",
    "password": "Soft!@7890",
    "database": "customDialer",
}

# ─────────────────────────────────────────────
# VICIDIAL API CONFIG
# ─────────────────────────────────────────────
vicidial_url     = "http://192.168.15.165:5165/vicidial/non_agent_api.php"
VICIDIAL_API_URL = "http://192.168.15.165:5165/agc/api.php"
API_USER         = "AdminR"
API_PASS         = "AdminR"
vici_user        = "AdminR"
Vici_pass        = "AdminR"
SOURCE           = "FASTAPI"

# ─────────────────────────────────────────────
# TWILIO / WHATSAPP CONFIG
# ─────────────────────────────────────────────
TWILIO_ACCOUNT_SID           = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN            = os.getenv("TWILIO_AUTH_TOKEN")
WHATSAPP_NUMBER              = os.getenv("WHATSAPP_NUMBER")
TWILIO_MESSAGING_SERVICE_SID = os.getenv("TWILIO_MESSAGING_SERVICE_SID")
WHATSAPP_TOKEN               = "your_meta_api_token"
PHONE_NUMBER_ID              = "your_office_number_id"
PREFILLED_TEXT               = quote("Hello! I'm interested in your services. Please contact me.")
WHATSAPP_LINK                = f"https://wa.me/{WHATSAPP_NUMBER}?text={PREFILLED_TEXT}"
twilio_client                = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="VICIdial Dashboard", version="7.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://192.168.15.104:5000", "http://localhost:5000","http://192.168.15.104:5500", "http://localhost:5500"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# POSTGRESQL POOL (for sessions)
# ─────────────────────────────────────────────
pgsqlPool = pool.SimpleConnectionPool(
    minconn=1, maxconn=20,
    dbname="customDialer", user="postgres",
    password="Soft!@7890", host="192.168.15.105"
)


# ═══════════════════════════════════════════════════════════════
#  PYDANTIC MODELS
# ═══════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    username:      str
    password:      str
    campaign_id:   str
    campaign_name: str
    role:          str

class DeleteLeadRequest(BaseModel):
    phone_number: List[str]

class SMSRequest(BaseModel):
    phone_number:   str
    custom_message: Optional[str] = None


# ═══════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════

def today_str() -> str:
    return date.today().isoformat()

def format_time(seconds):
    if seconds is None:
        return "00:00:00"
    return time.strftime('%H:%M:%S', time.gmtime(int(seconds)))

def seconds_to_hhmmss(seconds):
    seconds = int(seconds or 0)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:02}"

def resolve_date_range(sd=None, ed=None):
    if sd and ed:
        start_date = datetime.strptime(sd, "%Y-%m-%d")
        end_date   = datetime.strptime(ed, "%Y-%m-%d") + timedelta(days=1)
    elif sd:
        start_date = datetime.strptime(sd, "%Y-%m-%d")
        end_date   = start_date + timedelta(days=1)
    else:
        start_date = datetime.combine(date.today(), datetime.min.time())
        end_date   = datetime.now()
    return start_date, end_date

class RefreshRequest(BaseModel):
    refresh_token: str 
REFRESH_TOKEN_EXPIRE_DAYS = 7

def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(minutes=1440)})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS), "type": "refresh"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload       = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username      = payload.get("sub")
        is_admin      = payload.get("isAdmin", False)
        user_level    = payload.get("user_level", 1)
        campaign_name = payload.get("campaign_name")
        campaign_id   = payload.get("campaign_id")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"username": username, "isAdmin": is_admin,"user_level":user_level,"campaign_name": campaign_name, "campaign_id": campaign_id}
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired or invalid")


# ─────────────────────────────────────────────
# DB CONNECTION HELPERS
# ─────────────────────────────────────────────

def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_DB["host"], port=MYSQL_DB["port"],
        user=MYSQL_DB["user"], password=MYSQL_DB["password"],
        database=MYSQL_DB["database"],
        cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4"
    )

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_DB["host"], port=POSTGRES_DB["port"],
        user=POSTGRES_DB["user"], password=POSTGRES_DB["password"],
        dbname=POSTGRES_DB["database"]
    )


# ─────────────────────────────────────────────
# LEAD HELPERS
# ─────────────────────────────────────────────

def normalize_phone(phone):
    return str(phone).strip()[-10:]

def clean_phone(value):
    if value is None or value == "":
        return ""
    value = str(value).strip()
    if "E" in value.upper():
        try:
            value = "{:.0f}".format(float(value))
        except:
            return ""
    if isinstance(value, (int, float)):
        value = str(int(value))
    value = str(value).strip()
    if value.endswith(".0"):
        value = value[:-2]
    value = value.replace(" ", "")
    return value

def load_existing_phones():
    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT distinct phone_number FROM vicidial_list WHERE phone_number IS NOT NULL")
    phones = set()
    for (phone,) in cursor.fetchall():
        cleaned = clean_phone(phone)
        if cleaned:
            phones.add(cleaned)
    cursor.close()
    conn.close()
    return phones

def validate_list_campaign(list_id, campaign_id):
    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM vicidial_lists
        WHERE list_id = %s AND campaign_id = %s AND active = 'Y' LIMIT 1
    """, (list_id, campaign_id))
    valid = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return valid


# ─────────────────────────────────────────────
# VICIDIAL AGENT HELPERS
# ─────────────────────────────────────────────

def pauseUser(current_user):
    print("Pausing agent:", current_user)
    try:
        res = requests.get(
            VICIDIAL_API_URL,
            params={
                "source": "ctestrm", "user": API_USER, "pass": API_PASS,
                "agent_user": current_user["username"],
                "function": "external_pause", "value": "PAUSE"
            },
            timeout=10
        )
        print("Pause Response:", res.content)
        return res.text
    except Exception as e:
        print("Failed to pause:", str(e))
        return ""

def get_agent_status(username):
    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT status FROM vicidial_live_agents WHERE user = %s", (username,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row["status"] if row else None

# Refresh Token 
@app.post("/refresh")
def refresh_token(data: RefreshRequest):
    try:
        payload = jwt.decode(data.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])

        # Ensure it's actually a refresh token
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")

        username      = payload.get("sub")
        is_admin      = payload.get("isAdmin", False)
        campaign_id   = payload.get("campaign_id")
        campaign_name = payload.get("campaign_name")

        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")

        # Issue a new access token
        new_access_token = create_access_token(data={
            "sub": username,
            "isAdmin": is_admin,
            "campaign_id": campaign_id,
            "campaign_name": campaign_name
        })

        return {
            "access_token": new_access_token,
            "token_type": "bearer"
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Refresh token expired or invalid")
# ═══════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup():
    print("[Startup] Application started.")


# ═══════════════════════════════════════════════════════════════
#  AUTH ROUTES
# ═══════════════════════════════════════════════════════════════
@app.post("/login")
def login(data: LoginRequest):
    try:
        db     = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()

        if data.role == 'Agent':
            cursor.execute("""
                SELECT vu.user, full_name, vu.active, user_level, vca.campaign_id, vc.campaign_name
                FROM vicidial_users vu
                JOIN vicidial_campaign_agents vca ON vu.user = vca.user
                LEFT JOIN vicidial_campaigns vc ON vc.campaign_id = vca.campaign_id
                WHERE vu.user=%s AND pass=%s AND vca.campaign_id=%s AND vc.campaign_name=%s
                  AND vc.Active='Y' AND vu.active='Y' AND vu.user_level <> 9 LIMIT 1
            """, (data.username, data.password, data.campaign_id, data.campaign_name))
        else:
            cursor.execute("""
                SELECT vu.user, full_name, vu.active, user_level
                FROM vicidial_users vu
                WHERE vu.user=%s AND pass=%s AND vu.active='Y' AND vu.user_level <= 9 LIMIT 1
            """, (data.username, data.password))

        user = cursor.fetchone()

        cursor.close()
        db.close()

        if not user:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        if user[2] != "Y":
            raise HTTPException(status_code=403, detail="User is inactive")

        access_token = create_access_token(data={
            "sub": user[0],
            "isAdmin": user[3] in (8, 9),   # ✅ FIXED HERE
            "user_level": int(user[3]),
            "campaign_id": data.campaign_id,
            "campaign_name": data.campaign_name
        })
        refresh_token = create_refresh_token(data={
            "sub": user[0],
            "isAdmin": user[3] in (8, 9),
            "user_level": int(user[3]),
            "campaign_id": data.campaign_id,
            "campaign_name": data.campaign_name
        })

        return {
            "status": "success",
            "user": user[0],
            "full_name": user[1],
            "access_token": access_token,
            "refresh_token": refresh_token,
            "isAdmin": user[3] in (8, 9),   # ✅ FIXED HERE
            "campaign_id": data.campaign_id,
            "campaign_name": data.campaign_name,
            "token_type": "bearer"
        }

    except Exception as e:
        print("ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))
# ═══════════════════════════════════════════════════════════════
#  DASHBOARD ROUTES
# ═══════════════════════════════════════════════════════════════

# @app.get('/getcallswaiting')
# def get_waitingcalls():
#     try:
#         conn   = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor(dictionary=True)
#         cursor.execute("""
#             SELECT u.user AS agent_id, u.full_name AS agent_name,
#                    vla.campaign_id, vla.status, vla.calls_today AS calls_handled
#             FROM vicidial_live_agents vla
#             LEFT JOIN vicidial_users u ON vla.user = u.user
#             ORDER BY vla.status, vla.calls_today DESC
#         """)
#         result = cursor.fetchall()
#         cursor.close()
#         conn.close()
#         return {"count": len(result), "data": result}
#     except Error as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get('/getagnetstimeoncall')
# def get_agents_time_on_call():
#     try:
#         conn   = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor(dictionary=True)
#         cursor.execute("""
#             SELECT extension AS STATION, user AS USER, status AS STATUS, calls_today AS CALLS,
#                    (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_state_change)) AS TALK_TIME_SECONDS
#             FROM vicidial_live_agents ORDER BY status
#         """)
#         result = cursor.fetchall()
#         for row in result:
#             row["TALK_TIME_HH_MM_SS"] = format_time(row.get("TALK_TIME_SECONDS", 0))
#         cursor.close()
#         conn.close()
#         return {"count": len(result), "data": result}
#     except Error as e:
#         raise HTTPException(status_code=500, detail=str(e))


# @app.get('/get_all_data')
# def get_all_data():
#     try:
#         conn        = mysql.connector.connect(**DB_CONFIG)
#         cursor      = conn.cursor(dictionary=True)
#         today_start = date.today()

#         cursor.execute("SELECT avg(auto_dial_level) AS dialer_level FROM vicidial_campaigns")
#         dialer_level_result = cursor.fetchall()
#         cursor.execute("""
#             SELECT COUNT(*) AS dialable_leads FROM vicidial_list vl
#             JOIN vicidial_lists vli ON vl.list_id = vli.list_id
#             WHERE vli.active = 'Y' AND vl.called_since_last_reset = 'N'
#         """)
#         dialable_leads_result = cursor.fetchall()
#         cursor.execute("SELECT sum(hopper_level) AS min_hopper FROM vicidial_campaigns")
#         hopper_min_max_result = cursor.fetchall()
#         cursor.execute("SELECT COUNT(*) AS leads_in_hopper FROM vicidial_hopper WHERE status = 'READY'")
#         leads_in_hopper_result = cursor.fetchall()
#         cursor.execute("""
#             SELECT GREATEST(COUNT(vh.lead_id) - (vc.auto_dial_level * COUNT(DISTINCT vla.user)),0) AS trunk_fill,
#                    GREATEST((vc.auto_dial_level * COUNT(DISTINCT vla.user)) - COUNT(vh.lead_id),0) AS trunk_short
#             FROM vicidial_campaigns vc
#             LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
#             LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id WHERE vc.active = 'Y'
#         """)
#         trunk_short_fill_result = cursor.fetchall()
#         cursor.execute("SELECT COUNT(*) AS calls_today FROM vicidial_log WHERE date(call_date) = %s", (today_start,))
#         calls_today_result = cursor.fetchall()
#         cursor.execute("""
#             SELECT ROUND((SUM(IF(status IN ('DROP','DC'),1,0)) / COUNT(*)) * 100, 2) AS dropped_percent
#             FROM vicidial_log WHERE date(call_date) = %s
#         """, (today_start,))
#         drop_percent_result = cursor.fetchall()
#         cursor.execute("SELECT COUNT(*) AS agents FROM vicidial_live_agents")
#         avg_agent_result = cursor.fetchall()
#         cursor.execute("""
#             SELECT ROUND(vc.auto_dial_level - (COUNT(vh.lead_id) / COUNT(vla.user)),2) AS dl_diff
#             FROM vicidial_campaigns vc
#             LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id
#             LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
#         """)
#         dl_diff_result = cursor.fetchall()
#         cursor.execute("""
#             SELECT ROUND(((vc.auto_dial_level - (COUNT(vh.lead_id) / NULLIF(COUNT(vla.user), 0))) / vc.auto_dial_level) * 100, 2) AS diff_percent
#             FROM vicidial_campaigns vc
#             LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id
#             LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
#         """)
#         diff_result = cursor.fetchall()
#         cursor.execute("SELECT dial_method FROM vicidial_campaigns ORDER BY dial_method ASC LIMIT 1")
#         dial_method_result = cursor.fetchall()
#         cursor.execute("SELECT status FROM vicidial_campaign_statuses")
#         status_result = cursor.fetchall()
#         cursor.execute("SELECT DISTINCT lead_order FROM vicidial_campaigns")
#         order_result = cursor.fetchall()
#         cursor.close()
#         conn.close()

#         return {
#             "dialer_level":     dialer_level_result[0]["dialer_level"] if dialer_level_result else None,
#             "dialable_leads":   dialable_leads_result[0]["dialable_leads"] if dialable_leads_result else 0,
#             "hopper_min_max":   hopper_min_max_result[0]["min_hopper"] if hopper_min_max_result else None,
#             "trunk_short_fill": trunk_short_fill_result[0] if trunk_short_fill_result else {"trunk_fill": 0, "trunk_short": 0},
#             "calls_today":      calls_today_result[0]["calls_today"] if calls_today_result else 0,
#             "avg_agent":        avg_agent_result[0]["agents"] if avg_agent_result else 0,
#             "dl_diff":          dl_diff_result[0]["dl_diff"] if dl_diff_result else 0,
#             "diff_percent":     diff_result[0]["diff_percent"] if diff_result else 0,
#             "dial_method":      dial_method_result[0]["dial_method"] if dial_method_result else None,
#             "order":            order_result[0]["lead_order"] if order_result else None
#         }
#     except Error as e:
#         raise HTTPException(status_code=500, detail=str(e))
@app.get('/getcallbystatus')
def get_calls_by_status(request: Request, current_user: str = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        campaign_id = request.query_params.get("campaign_id")
        user_id     = request.query_params.get("user_id")
        sd          = request.query_params.get("sd")
        ed          = request.query_params.get("ed")
        admin_user  = current_user["username"]

        date_filter = "DATE(event_time) BETWEEN %s AND %s AND status IS NOT NULL" if sd and ed else "DATE(event_time) = %s AND status IS NOT NULL"
        date_params = (sd, ed) if sd and ed else (date.today(),)

        filters       = []
        filter_params = []

        # ✅ ROLE BASED ACCESS CONTROL (FIXED)
        filters.append("""
            (
                (SELECT user_level FROM vicidial_users WHERE user = %s) = 9
                OR (
                    (SELECT user_level FROM vicidial_users WHERE user = %s) > 1
                    AND (
                        SELECT vu2.user_group 
                        FROM vicidial_users vu2 
                        WHERE vu2.user = vicidial_live_agents.user
                        LIMIT 1
                    ) = (SELECT user_group FROM vicidial_users WHERE user = %s LIMIT 1)
                )
                OR (
                    (SELECT user_level FROM vicidial_users WHERE user = %s) = 1
                    AND vicidial_live_agents.user = %s
                )
            )
        """)

        filter_params.extend([
            admin_user,
            admin_user,
            admin_user,
            admin_user,
            admin_user
        ])

        # ✅ EXISTING FILTER
        filters.append("""
            campaign_id IN (
                SELECT campaign_id FROM vicidial_campaign_agents WHERE user = %s
            )
        """)
        filter_params.append(admin_user)

        filters.append("user != 'VDAD'")

        if campaign_id:
            filters.append("campaign_id = %s")
            filter_params.append(campaign_id)

        if user_id:
            filters.append("user = %s")
            filter_params.append(user_id)

        extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        query = f"""
            SELECT
                COUNT(CASE WHEN status='INCALL' THEN 1 END) AS Incall,
                COUNT(CASE WHEN status='PAUSED' THEN 1 END) AS Paused,
                COUNT(CASE WHEN status='READY'  THEN 1 END) AS Ready,
                (
                    SELECT COUNT(*) FROM vicidial_agent_log
                    WHERE {date_filter}
                    {extra_filter}
                ) AS Totalcall
            FROM vicidial_live_agents
            WHERE 1=1 {extra_filter}
        """

        params = (
            *date_params,
            *filter_params,
            *filter_params
        )

        cursor.execute(query, params)
        result = cursor.fetchall()

        cursor.close()
        conn.close()
        return {"count": len(result), "data": result}

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/totaldialstoday')
def get_totaldials(request: Request, current_user: dict = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        sd          = request.query_params.get("sd")
        ed          = request.query_params.get("ed")
        is_admin    = current_user["isAdmin"]
        admin_user  = current_user["username"]

        if not is_admin:
            user_id     = current_user["username"]
            campaign_id = current_user["campaign_id"]
            extra_filter = f"AND val.user = '{user_id}' AND val.campaign_id = '{campaign_id}'"
        else:
            campaign_id = request.query_params.get("campaign_id")
            user_id     = request.query_params.get("user_id")

            filters = []
            if campaign_id:
                filters.append(f"val.campaign_id = '{campaign_id}'")
            if user_id:
                filters.append(f"val.user = '{user_id}'")

            extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        query = f"""
            SELECT
                call_date,
                SUM(dialed_calls)       AS total_dials,
                SUM(connected_calls)    AS connected_calls,
                ROUND(SUM(connected_calls) / NULLIF(SUM(dialed_calls), 0) * 100, 2) AS connection_rate_pct,
                SUM(login_duration)     AS login_duration,
                SUM(pause_sec)          AS pause_sec,
                SUM(wait_sec)           AS wait_sec,
                SUM(TALK_TIME_SECONDS)  AS total_talk_time,
                ROUND(SUM(TALK_TIME_SECONDS) / NULLIF(SUM(connected_calls), 0), 2) AS avg_talk_time_sec
            FROM (
                SELECT
                    DATE(val.event_time) AS call_date,
                    val.user,
                    COUNT(*) AS dialed_calls,
                    SUM(IF(val.status NOT IN (
                        'N', 'B', 'AB', 'D', 'DROP', 
                        'INVN', 'NA', 'DNC','ADC','FUC','NA'
                    ), 1, 0)) AS connected_calls,
                    SUM(val.pause_sec + val.wait_sec +
                        val.talk_sec  + val.dispo_sec) AS login_duration,
                    SUM(val.pause_sec) AS pause_sec,
                    SUM(val.wait_sec) AS wait_sec,
                    SUM(val.talk_sec) AS TALK_TIME_SECONDS
                FROM vicidial_agent_log val
                LEFT JOIN vicidial_users vu ON val.user = vu.user
                WHERE DATE(val.event_time) BETWEEN %s AND %s
                    AND val.status IS NOT NULL

                    -- ✅ ROLE BASED ACCESS CONTROL
                    AND (
                        (SELECT user_level FROM vicidial_users WHERE user = %s) = 9
                        OR (
                            (SELECT user_level FROM vicidial_users WHERE user = %s) > 1
                            AND vu.user_group = (SELECT user_group FROM vicidial_users WHERE user = %s)
                        )
                        OR (
                            (SELECT user_level FROM vicidial_users WHERE user = %s) = 1
                            AND val.user = %s
                        )
                    )

                    -- ✅ EXISTING CAMPAIGN FILTER (UNCHANGED)
                    AND val.campaign_id IN (
                        SELECT campaign_id FROM vicidial_campaign_agents WHERE user = %s
                    )
                    AND val.user IN (
                        SELECT DISTINCT vca.user
                        FROM vicidial_campaign_agents vca
                        WHERE vca.campaign_id IN (
                            SELECT campaign_id
                            FROM vicidial_campaign_agents
                            WHERE user = %s
                        )
                    )

                    {extra_filter}
                GROUP BY val.user
            ) a
        """

        cursor.execute(query, (
            sd, ed,

            # ✅ role-based params (5)
            admin_user,
            admin_user,
            admin_user,
            admin_user,
            admin_user,

            # ✅ campaign params (2)
            admin_user,
            admin_user
        ))

        result = cursor.fetchall()

        for row in result:
            row["total_talk_time"]  = seconds_to_hhmmss(row.get("total_talk_time") or 0)
            row["login_duration"]   = seconds_to_hhmmss(
                int(row["login_duration"].total_seconds())
                if hasattr(row.get("login_duration"), "total_seconds")
                else (row.get("login_duration") or 0)
            )
            row["avg_talk_time_sec"] = seconds_to_hhmmss(int(row.get("avg_talk_time_sec") or 0))

        cursor.close()
        conn.close()
        return {"data": result}

    except Error as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/metalead-stats')
def get_lead_stats(request: Request, current_user: dict = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        sd          = request.query_params.get("sd")
        ed          = request.query_params.get("ed")
        list_id     = request.query_params.get("list_id", "7022026")
        is_admin    = current_user["isAdmin"]
        admin_user  = current_user["username"]

        if not is_admin:
            user_id     = current_user["username"]
            campaign_id = current_user["campaign_id"]
            extra_filter = f"""
                AND vl.user = '{user_id}'
                AND vl.list_id IN (
                    SELECT list_id FROM vicidial_lists WHERE campaign_id = '{campaign_id}'
                )
            """
        else:
            campaign_id = request.query_params.get("campaign_id")
            user_id     = request.query_params.get("user_id")
            filters = []
            if campaign_id:
                filters.append(f"""
                    vl.list_id IN (
                        SELECT list_id FROM vicidial_lists WHERE campaign_id = '{campaign_id}'
                    )
                """)
            if user_id:
                filters.append(f"vl.user = '{user_id}'")
            extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        date_filter = ""
        date_params = []
        if sd and ed:
            date_filter = "AND DATE(vl.entry_date) BETWEEN %s AND %s"
            date_params = [sd, ed]

        scope_filter = """
            AND vl.user IN (
                SELECT DISTINCT vca.user
                FROM vicidial_campaign_agents vca
                WHERE vca.campaign_id IN (
                    SELECT campaign_id
                    FROM vicidial_campaign_agents
                    WHERE user = %s
                )
            )
        """

        # ── Query 1: Total Leads (only list_id) ──────────────────────────
        q1 = """
            SELECT COUNT(*) AS total_leads
            FROM vicidial_list vl
            WHERE vl.list_id = %s
        """
        cursor.execute(q1, [list_id])
        total_leads = cursor.fetchone()["total_leads"] or 0

        # ── Query 2: Called Leads ─────────────────────────────────────────
        date_filter_called = ""
        # if sd and ed:
        #     date_filter_called = "AND DATE(vl.last_local_call_time) BETWEEN %s AND %s"

        q2 = f"""
            SELECT COUNT(*) AS called_leads
            FROM vicidial_list vl
            WHERE vl.list_id = %s
              AND vl.status != 'NEW'
             
        """
        cursor.execute(q2, [list_id] )
        called_leads = cursor.fetchone()["called_leads"] or 0

        # ── Query 3: Pending Leads (list_id + status = NEW + all filters) ─
        # ── Query 3: Pending Leads ────────────────────────────────────────
        # entry_date <= ed means "leads that existed by that date and still not called"
        date_filter_pending = ""
        date_params_pending = []
        if ed:
            date_filter_pending = "AND DATE(vl.entry_date) <= %s"
            date_params_pending = [ed]

        q3 = f"""
            SELECT COUNT(*) AS pending_leads
            FROM vicidial_list vl
            WHERE vl.list_id = %s
              AND vl.status = 'NEW'
              {date_filter_pending}
              {scope_filter}
              {extra_filter}
        """
        cursor.execute(q3, [list_id] + date_params_pending + [admin_user])
        pending_leads = cursor.fetchone()["pending_leads"] or 0

        cursor.close()
        conn.close()

        return {
            "list_id":       list_id,
            "sd":            sd,
            "ed":            ed,
            "total_leads":   total_leads,
            "called_leads":  called_leads,
            "pending_leads": pending_leads,
        }

    except Error as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/dialerperformance')
def get_dialerperformance(request: Request, current_user: dict = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        today_start = date.today()
        is_admin    = current_user["isAdmin"]

        if not is_admin:
            # Agent: filter by own user and campaign from token
            user_id     = current_user["username"]
            campaign_id = current_user["campaign_id"]
            user_filter     = f"AND vl.user = '{user_id}'"
            campaign_filter = f"AND campaign_id = '{campaign_id}'"

        else:
            # Admin: optional filters from query params
            campaign_id = request.query_params.get("campaign_id")
            user_id     = request.query_params.get("user_id")
            admin_user  = current_user["username"]  # ← NEW

            filters = []

            # ── NEW: restrict admin to only their assigned campaigns ──
            filters.append(f"""
                campaign_id IN (
                    SELECT campaign_id FROM vicidial_campaign_agents WHERE user = '{admin_user}'
                )
            """)
            filters.append("vl.user != 'VDAD'")
            # ─────────────────────────────────────────────────────────

            if campaign_id:
                filters.append(f"campaign_id = '{campaign_id}'")
            if user_id:
                filters.append(f"vl.user = '{user_id}'")

            campaign_filter = (" AND " + " AND ".join(filters)) if filters else ""
            user_filter     = ""

        query = f"""
            SELECT
                ROUND(COUNT(*) / NULLIF((SELECT COUNT(DISTINCT user) FROM vicidial_live_agents WHERE 1=1 {campaign_filter}), 0), 2) AS dial_level,
                ROUND((COUNT(*) / NULLIF((SELECT COUNT(DISTINCT user) FROM vicidial_live_agents WHERE 1=1 {campaign_filter}), 0)) / 24, 2) AS calls_per_agent_per_hour,
                (SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) FROM call_log WHERE DATE(start_time) = %s) AS avg_answer_speed_sec,
                ROUND(SUM(vl.status IN ('DROP','ABANDON')) * 100.0 / NULLIF((SELECT COUNT(*) FROM vicidial_dial_log WHERE DATE(call_date) = %s {campaign_filter}), 0), 2) AS drop_rate_percent,
                ROUND(AVG(vl.length_in_sec), 2) AS avg_call_length,
                ROUND((SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_percent
            FROM vicidial_log vl
            WHERE DATE(vl.call_date) = %s {campaign_filter} {user_filter}
        """

        cursor.execute(query, (today_start, today_start, today_start))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"data": result}
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------Campaign filter above -----------------------
def seconds_to_hhmmss(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))  # guard against negatives/None
    hours   = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

@app.get('/agentsproductivity')
def get_agentsproductivity(request: Request, current_user: str = Depends(get_current_user)):
    try:
        sd          = request.query_params.get("sd")
        ed          = request.query_params.get("ed")
        is_admin    = current_user["isAdmin"]

        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if not is_admin:
            campaign_id  = current_user["campaign_id"]
            user_id      = current_user["username"]
            extra_filter = f"AND val.campaign_id = '{campaign_id}' AND val.user = '{user_id}'"
        else:
            campaign_id = request.query_params.get("campaign_id")
            user_id     = request.query_params.get("user_id")

            filters = []
            if campaign_id:
                filters.append(f"val.campaign_id = '{campaign_id}'")
            if user_id:
                filters.append(f"val.user = '{user_id}'")

            extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        cursor.execute(f"""
            SELECT
                CONCAT('SIP/', val.user)                        AS STATION,
                val.user                                        AS USER_ID,
                MAX(vu.full_name)                               AS USER_NAME,
                MAX(vla.status)                                 AS STATUS,
                MAX(vu.user_group)                              AS user_group,
                COUNT(*)                                        AS CALLS,
                SUM(IF(val.status NOT IN (
                    'N', 'B', 'AB', 'D', 'DROP', 
                    'INVN', 'NA', 'DNC','ADC','FUC','NA'
                ), 1, 0))                                                       AS connected_calls,
                SUM(val.pause_sec + val.wait_sec +
                    val.talk_sec  + val.dispo_sec)              AS login_duration,
                SUM(val.pause_sec)                              AS pause_sec,
                SUM(val.wait_sec)                               AS wait_sec,
                SUM(val.talk_sec)                               AS TALK_TIME_SECONDS,
                SUM(val.dispo_sec)                              AS dispo_sec,
                SUM(val.dead_sec)                               AS dead_sec,
                SUM(val.talk_sec - val.dead_sec)                AS customer_sec,
                SUM(IF(val.status = 'B',    1, 0))             AS dispo_B,
                SUM(IF(val.status = 'C',    1, 0))             AS dispo_C,
                SUM(IF(val.status = 'D',    1, 0))             AS dispo_D,
                SUM(IF(val.status = 'EC',   1, 0))             AS dispo_EC,
                SUM(IF(val.status = 'FUC',  1, 0))             AS dispo_FUC,
                SUM(IF(val.status = 'IN',   1, 0))             AS dispo_IN,
                SUM(IF(val.status = 'INVN', 1, 0))             AS dispo_INVN,
                SUM(IF(val.status = 'N',    1, 0))             AS dispo_N,
                SUM(IF(val.status = 'NI',   1, 0))             AS dispo_NI,
                SUM(IF(val.status = 'WN',   1, 0))             AS dispo_WN
            FROM vicidial_agent_log val
            LEFT JOIN vicidial_users vu ON val.user = vu.user
            LEFT JOIN vicidial_live_agents vla on val.user = vla.user           
            WHERE DATE(val.event_time) BETWEEN %s AND %s
                AND val.status IS NOT NULL
                 AND (
                    (SELECT user_level FROM vicidial_users WHERE user = %s) = 9
                    OR (
                        (SELECT user_level FROM vicidial_users WHERE user = %s) > 1
                        AND vu.user_group = (SELECT user_group FROM vicidial_users WHERE user = %s)
                    )
                    OR (
                        (SELECT user_level FROM vicidial_users WHERE user = %s) = 1
                        AND val.user = %s
                    )
                )
                {extra_filter}
            GROUP BY val.user
            ORDER BY MAX(vu.full_name) ASC
        """, (sd, ed,
                current_user["username"],  # level check
                current_user["username"],  # level >1
                current_user["username"],  # group match
                current_user["username"],  # level =1
                current_user["username"],  # self
                ))

        result = cursor.fetchall()
        for row in result:
            row["TALK_TIME_HH_MM_SS"] = format_time(row.get("TALK_TIME_SECONDS", 0))

            login_raw = row.get("login_duration", 0)
            if hasattr(login_raw, "total_seconds"):
                total_seconds = int(login_raw.total_seconds())
            elif login_raw is not None:
                total_seconds = int(login_raw)
            else:
                total_seconds = 0

            row["login_duration"] = seconds_to_hhmmss(total_seconds)

            paused_sec = row.get("pause_sec", 0)
            if hasattr(login_raw, "total_seconds"):
                pause_seconds = int(paused_sec.total_seconds())
            elif login_raw is not None:
                pause_seconds = int(paused_sec)
            else:
                pause_seconds = 0
            row["pause_sec"] = seconds_to_hhmmss(pause_seconds)
            

        cursor.close()
        conn.close()

        pg_conn = get_pg_conn()
        try:
            pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            pg_cur.execute("""
                SELECT
                    agent_user,
                    ROUND(AVG(overall_rating)::numeric, 2) AS avg_rating,
                    ROUND(AVG(stars)::numeric, 1)          AS avg_stars,
                    COUNT(*)                                AS total_analyzed,
                    SUM(CASE WHEN call_outcome = 'Successful Sale' THEN 1 ELSE 0 END) AS successful_sales,
                    SUM(CASE WHEN call_outcome = 'Not Converted'   THEN 1 ELSE 0 END) AS not_converted,
                    SUM(CASE WHEN call_outcome = 'Lead Generated'  THEN 1 ELSE 0 END) AS leads_generated
                FROM call_analysis
                WHERE status IN ('success', 'successful')
                  AND DATE(start_time) BETWEEN %s AND %s
                GROUP BY agent_user
            """, (sd, ed))
            pg_rows = pg_cur.fetchall()
            pg_cur.close()
            rating_map = {
                r["agent_user"]: {
                    "avg_rating":       float(r["avg_rating"])  if r["avg_rating"]  else None,
                    "avg_stars":        float(r["avg_stars"])   if r["avg_stars"]   else None,
                    "total_analyzed":   r["total_analyzed"],
                    "successful_sales": r["successful_sales"],
                    "not_converted":    r["not_converted"],
                    "leads_generated":  r["leads_generated"],
                }
                for r in pg_rows
            }
        finally:
            pg_conn.close()

        for row in result:
            ratings = rating_map.get(row.get("USER_ID"), {})
            row["avg_rating"]       = ratings.get("avg_rating",       None)
            row["avg_stars"]        = ratings.get("avg_stars",         None)
            row["total_analyzed"]   = ratings.get("total_analyzed",    0)
            row["successful_sales"] = ratings.get("successful_sales",  0)
            row["not_converted"]    = ratings.get("not_converted",     0)
            row["leads_generated"]  = ratings.get("leads_generated",   0)

        return {"data": result}

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/campaignperformance')
def get_campaignperformance(request: Request, current_user: str = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        sd          = request.query_params.get("sd")
        ed          = request.query_params.get("ed")
        campaign_id = request.query_params.get("campaign_id")
        user_id     = request.query_params.get("user_id")
        admin_user  = current_user["username"]
        is_admin    = current_user["isAdmin"]

        if not is_admin:
            user_id     = current_user["username"]
            campaign_id = current_user["campaign_id"]
            extra_filter = f"AND val.user = '{user_id}' AND val.campaign_id = '{campaign_id}'"
        else:
            filters = []
            if campaign_id:
                filters.append(f"val.campaign_id = '{campaign_id}'")
            if user_id:
                filters.append(f"val.user = '{user_id}'")
            extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        cursor.execute(f"""
            SELECT
                val.campaign_id AS campaign_id,
                COUNT(*) AS total_dials,
                SUM(IF(val.status NOT IN (
                    'N', 'B', 'AB', 'D', 'DROP', 
                    'INVN', 'NA', 'DNC','ADC','FUC','NA'
                ), 1, 0)) AS connected_calls,
                ROUND((SUM(IF(val.talk_sec > 0, 1, 0)) / COUNT(*)) * 100, 2) AS connection_rate_pct,
                SUM(val.pause_sec + val.wait_sec +
                    val.talk_sec  + val.dispo_sec) AS login_duration,
                SUM(val.pause_sec) AS pause_sec,
                SUM(val.wait_sec) AS wait_sec,
                SUM(val.talk_sec) AS total_talk_time,
                ROUND(SUM(val.talk_sec) / NULLIF(SUM(IF(val.talk_sec > 0, 1, 0)), 0), 2) AS avg_talk_time_sec,
                ROUND((SUM(val.status = 'DROP') / COUNT(*)) * 100, 2) AS drop_rate_pct,
                SUM(val.status IN ('SALE','SUCCESS','CONVERTED')) AS conversions,
                SUM(val.dispo_sec) AS dispo_sec,
                SUM(val.dead_sec) AS dead_sec,
                SUM(val.talk_sec - val.dead_sec) AS customer_sec,
                SUM(IF(val.status = 'B',    1, 0)) AS dispo_B,
                SUM(IF(val.status = 'C',    1, 0)) AS dispo_C,
                SUM(IF(val.status = 'D',    1, 0)) AS dispo_D,
                SUM(IF(val.status = 'EC',   1, 0)) AS dispo_EC,
                SUM(IF(val.status = 'FUC',  1, 0)) AS dispo_FUC,
                SUM(IF(val.status = 'IN',   1, 0)) AS dispo_IN,
                SUM(IF(val.status = 'INVN', 1, 0)) AS dispo_INVN,
                SUM(IF(val.status = 'N',    1, 0)) AS dispo_N,
                SUM(IF(val.status = 'NI',   1, 0)) AS dispo_NI,
                SUM(IF(val.status = 'WN',   1, 0)) AS dispo_WN
            FROM vicidial_agent_log val
            LEFT JOIN vicidial_users vu ON val.user = vu.user
            WHERE DATE(val.event_time) BETWEEN %s AND %s
                AND val.status IS NOT NULL

                -- ✅ ROLE BASED ACCESS CONTROL
                AND (
                    (SELECT user_level FROM vicidial_users WHERE user = %s) = 9
                    OR (
                        (SELECT user_level FROM vicidial_users WHERE user = %s) > 1
                        AND vu.user_group = (SELECT user_group FROM vicidial_users WHERE user = %s)
                    )
                    OR (
                        (SELECT user_level FROM vicidial_users WHERE user = %s) = 1
                        AND val.user = %s
                    )
                )

                -- ✅ EXISTING CAMPAIGN FILTER (UNCHANGED)
                AND val.campaign_id IN (
                    SELECT campaign_id FROM vicidial_campaign_agents WHERE user = %s
                )
                AND val.user IN (
                    SELECT DISTINCT vca.user
                    FROM vicidial_campaign_agents vca
                    WHERE vca.campaign_id IN (
                        SELECT campaign_id
                        FROM vicidial_campaign_agents
                        WHERE user = %s
                    )
                )

                {extra_filter}
            GROUP BY val.campaign_id
            ORDER BY total_dials DESC
        """, (
            sd, ed,

            # ✅ role-based params (5)
            admin_user,
            admin_user,
            admin_user,
            admin_user,
            admin_user,

            # ✅ campaign params (2)
            admin_user,
            admin_user
        ))

        result = cursor.fetchall()

        for row in result:
            talk = row.get("total_talk_time") or 0
            row["total_talk_time"] = seconds_to_hhmmss(
                int(talk.total_seconds()) if hasattr(talk, "total_seconds") else int(talk)
            )

            login = row.get("login_duration") or 0
            row["login_duration"] = seconds_to_hhmmss(
                int(login.total_seconds()) if hasattr(login, "total_seconds") else int(login)
            )

        cursor.close()
        conn.close()
        return {"data": result}

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/compliancereview')
def get_compliancereview(current_user: str = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        today_start = date.today()
        cursor.execute("""
            SELECT DISTINCT
                (SELECT COUNT(*) FROM vicidial_dnc_log dnc) AS dnd_violations,
                (SELECT COUNT(*) FROM vicidial_callbacks cb WHERE cb.callback_time >= %s) AS callback_sla_raised,
                (SELECT dial_method FROM vicidial_campaigns ORDER BY dial_method ASC LIMIT 1) AS dial_method,
                (SELECT sum(hopper_level) FROM vicidial_campaigns) AS hooper_level,
                (SELECT COUNT(*) FROM vicidial_callbacks cb WHERE cb.callback_time < %s AND cb.status != 'COMPLETE') AS callback_sla_missed,
                (SELECT CASE WHEN COUNT(*) > 10 THEN 'HIGH' WHEN COUNT(*) BETWEEN 5 AND 10 THEN 'MEDIUM' ELSE 'LOW' END FROM vicidial_dnc_log dnc) AS risk_level
            FROM vicidial_log vl WHERE vl.call_date >= %s
        """, (today_start, today_start, today_start))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"data": result}
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/leadfunnel')
def get_LeadFunnel(request: Request, current_user: str = Depends(get_current_user)):
    try:
        sd          = request.query_params.get("sd")
        ed          = request.query_params.get("ed")
        campaign_id = request.query_params.get("campaign_id")
        user_id     = request.query_params.get("user_id")
        admin_user  = current_user["username"]

        filters       = []
        filter_params = []

        # ✅ ROLE BASED ACCESS CONTROL (NEW)
        filters.append("""
            (
                (SELECT user_level FROM vicidial_users WHERE user = %s) = 9
                OR (
                    (SELECT user_level FROM vicidial_users WHERE user = %s) > 1
                    AND vu.user_group = (SELECT user_group FROM vicidial_users WHERE user = %s)
                )
                OR (
                    (SELECT user_level FROM vicidial_users WHERE user = %s) = 1
                    AND val.user = %s
                )
            )
        """)

        # add 5 params for role logic
        filter_params.extend([
            admin_user,
            admin_user,
            admin_user,
            admin_user,
            admin_user
        ])

        # ✅ EXISTING FILTERS (UNCHANGED BUT PARAM SAFE)
        filters.append("""
            val.user IN (
                SELECT DISTINCT vca.user
                FROM vicidial_campaign_agents vca
                WHERE vca.campaign_id IN (
                    SELECT campaign_id FROM vicidial_campaign_agents WHERE user = %s
                )
            )
        """)
        filter_params.append(admin_user)

        filters.append("val.status IS NOT NULL")

        filters.append("""
            val.campaign_id IN (
                SELECT campaign_id FROM vicidial_campaign_agents WHERE user = %s
            )
        """)
        filter_params.append(admin_user)

        # optional filters
        if campaign_id:
            filters.append("val.campaign_id = %s")
            filter_params.append(campaign_id)

        if user_id:
            filters.append("val.user = %s")
            filter_params.append(user_id)

        extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        base_select = """
            SELECT
                COUNT(*)                                            AS dialed,
                SUM(val.status IN ('IN','CBR','INTEREST','I'))      AS Interested,
                SUM(IF(val.status NOT IN (
                    'N', 'B', 'AB', 'D', 'DROP', 
                    'INVN', 'NA', 'DNC','ADC','FUC','NA'
                ), 1, 0))                                          AS connected,
                SUM(val.status IN ('CON','SUCCESS','CONVERTED'))    AS converted,
                SUM(val.status IN ('EC'))                           AS existing_clients
            FROM vicidial_agent_log val
            LEFT JOIN vicidial_users vu ON val.user = vu.user
        """

        if sd and ed:
            cursor.execute(f"""
                {base_select}
                WHERE DATE(val.event_time) BETWEEN %s AND %s
                {extra_filter}
            """, (sd, ed, *filter_params))
        else:
            cursor.execute(f"""
                {base_select}
                WHERE DATE(val.event_time) = %s
                {extra_filter}
            """, (date.today(), *filter_params))

        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/hourlyperformance')
def get_hourlyperformance(request: Request, current_user: str = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        today_start = date.today()
        campaign_id = request.query_params.get("campaign_id")
        user_id     = request.query_params.get("user_id")
        admin_user  = current_user["username"]

        filters = []

        # Always restrict admin to only their assigned campaigns
        filters.append(f"""
            campaign_id IN (
                SELECT campaign_id FROM vicidial_campaign_agents WHERE user = '{admin_user}'
            )
        """)

        # Exclude system/dialer user
        filters.append("user != 'VDAD'")

        if campaign_id:
            filters.append(f"campaign_id = '{campaign_id}'")
        if user_id:
            filters.append(f"user = '{user_id}'")

        extra_filter = ("AND " + " AND ".join(filters)) if filters else ""

        cursor.execute(f"""
            SELECT HOUR(event_time) AS hour, COUNT(*) AS total_calls,SUM(IF(status NOT IN (
                    'N', 'B', 'AB', 'D', 'DROP', 
                    'INVN', 'NA', 'DNC','ADC','FUC','NA'
                ), 1, 0))  AS connected_calls
            FROM vicidial_agent_log
            WHERE DATE(event_time) = %s and status is not NULL
            {extra_filter}
            GROUP BY HOUR(event_time) ORDER BY hour
        """, (today_start,))

        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            "hours":           [r["hour"] for r in result],
            "total_calls":     [r["total_calls"] for r in result],
            "connected_calls": [r["connected_calls"] for r in result]
        }
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/graphdata')
def get_GraphData(current_user: str = Depends(get_current_user)):
    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT DATE_FORMAT(vl.call_date, '%H:%i') AS time_slot,
                   ROUND((SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_percentage,
                   ROUND(SUM(vl.status IN ('DROP','ABANDON')) * 100.0 / NULLIF(
                       (SELECT COUNT(*) FROM vicidial_dial_log vdl WHERE vdl.call_date BETWEEN NOW() - INTERVAL 2 HOUR AND NOW()), 0
                   ), 2) AS drop_rate_percentage
            FROM vicidial_log vl WHERE vl.call_date BETWEEN NOW() - INTERVAL 2 HOUR AND NOW()
            GROUP BY FLOOR(UNIX_TIMESTAMP(vl.call_date) / 720)
            ORDER BY time_slot DESC LIMIT 10
        """)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {
            "time_slot":                  [r["time_slot"] for r in result],
            "connection_rate_percentage": [r["connection_rate_percentage"] for r in result],
            "drop_rate_percentage":       [r["drop_rate_percentage"] for r in result]
        }
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


# @app.get('/leadfunnelwithdate')
# def get_LeadFunnelWithDate(request: Request, current_user: str = Depends(get_current_user)):
#     try:
#         sd     = request.query_params.get("sd")
#         ed     = request.query_params.get("ed")
#         conn   = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor(dictionary=True)
#         if sd and ed:
#             cursor.execute("""
#                 SELECT COUNT(*) AS dialed, SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
#                        SUM(length_in_sec > 0) AS connected, SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
#                        SUM(status IN ('EC')) AS existing_clients
#                 FROM vicidial_log WHERE date(call_date) BETWEEN %s AND %s
#             """, (sd, ed))
#         else:
#             cursor.execute("""
#                 SELECT COUNT(*) AS dialed, SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
#                        SUM(length_in_sec > 0) AS connected, SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
#                        SUM(status IN ('EC')) AS existing_clients
#                 FROM vicidial_log WHERE date(call_date) = %s
#             """, (date.today(),))
#         result = cursor.fetchone()
#         cursor.close()
#         conn.close()
#         return {"data": result}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
#  LEADS ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/leads")
def get_leads(request: Request, current_user: str = Depends(get_current_user)):
    try:
        sd    = request.query_params.get("sd")
        ed    = request.query_params.get("ed")
        limit = int(request.query_params.get("limit", 50))

        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if sd and ed:
            cursor.execute("""
                SELECT row_number() over (order by vl.entry_date desc,lead_id) rn,DATE(vl.entry_date) AS entry_date, vl.lead_id, vl.phone_number,
                       vl.first_name, vl.last_name, vl.status, vl.list_id, vls.campaign_id, vl.user
                FROM vicidial_list vl JOIN vicidial_lists vls ON vl.list_id = vls.list_id
                WHERE date(vl.entry_date) BETWEEN %s AND %s
                ORDER BY vl.entry_date DESC LIMIT %s
            """, (sd, ed, limit))
        else:
            cursor.execute("""
                SELECT row_number() over (order by vl.entry_date desc,lead_id) rn,DATE(vl.entry_date) AS entry_date, vl.lead_id, vl.phone_number,
                       vl.first_name, vl.last_name, vl.status, vl.list_id, vls.campaign_id, vl.user
                FROM vicidial_list vl JOIN vicidial_lists vls ON vl.list_id = vls.list_id
                WHERE date(vl.entry_date) = %s
                ORDER BY vl.entry_date DESC LIMIT %s
            """, (date.today(), limit))

        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"limit": limit, "count": len(data), "leads": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_excel_leads")
def upload_excel_leads(
    campaign_id:   str = Form(...),
    campaign_name: str = Form(...),
    file: UploadFile = File(...),
    current_user: str = Depends(get_current_user)
):
    if not (file.filename.endswith(".xlsx") or file.filename.endswith(".csv")):
        raise HTTPException(status_code=400, detail="Only .xlsx or .csv files allowed")
    try:
        contents = file.file.read()
        df = pd.read_csv(io.BytesIO(contents), dtype=str, encoding="utf-8") if file.filename.endswith(".csv") else pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid file: {e}")

    df.columns = df.columns.str.strip().str.lower()
    df.dropna(how="all", inplace=True)
    df.fillna("", inplace=True)

    if not {"phone_number", "list_id"}.issubset(df.columns):
        raise HTTPException(status_code=400, detail="File must contain: phone_number, list_id")

    existing_phones = load_existing_phones()
    success   = 0
    failed    = []
    skipped   = []
    not_valid = []

    # ✅ Get DB connection once for the entire upload
    db     = get_mysql_conn()
    cursor = db.cursor()

    try:
        for index, row in df.iterrows():
            excel_row = index + 2
            phone     = clean_phone(row.get("phone_number"))
            list_id   = str(row.get("list_id")).strip()
            agent_id  = str(row.get("agent_id", "")).strip()

            if not validate_list_campaign(list_id, campaign_id):
                not_valid.append({"row": excel_row, "list_id": list_id, "reason": f"List {list_id} not in campaign {campaign_id}"})
                continue
            if not phone or not list_id:
                skipped.append({"row": excel_row, "reason": "Missing phone or list_id"})
                continue
            if not phone.isdigit():
                skipped.append({"row": excel_row, "phone": phone, "reason": "Invalid phone number"})
                continue

            try:
                response = requests.get(vicidial_url, params={
                    "source": SOURCE, "user": vici_user, "pass": Vici_pass,
                    "function": "add_lead", "phone_number": phone, "phone_code": "1",
                    "list_id": list_id,
                    "first_name": str(row.get("first_name", "")).strip(),
                    "last_name":  str(row.get("last_name", "")).strip(),
                    **({"agent_only": agent_id} if agent_id else {}),
                }, timeout=10)

                if "SUCCESS" in response.text.upper():
                    success += 1
                    existing_phones.add(phone)

                    # ✅ Update user column only if agent_id provided
                    if agent_id:
                        cursor.execute("""
                            UPDATE vicidial_list 
                            SET user = %s 
                            WHERE phone_number = %s AND list_id = %s 
                            ORDER BY lead_id DESC 
                            LIMIT 1
                        """, (agent_id, phone, list_id))
                        db.commit()
                else:
                    failed.append({"row": excel_row, "phone": phone, "error": response.text})

            except Exception as e:
                failed.append({"row": excel_row, "phone": phone, "error": str(e)})

    finally:
        # ✅ Always close DB connection when done
        cursor.close()
        db.close()

    return {
        "campaign_id": campaign_id, "campaign_name": campaign_name,
        "total_rows": len(df), "success": success,
        "failed": len(failed), "skipped": len(skipped),
        "failed_details": failed, "skipped_details": skipped, "list_and_campaign": not_valid
    }

@app.post("/delete_lead")
def delet_lead(data: DeleteLeadRequest, current_user: str = Depends(get_current_user)):
    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    try:
        phone_list   = [p.strip() for p in data.phone_number]
        placeholders = ",".join(["%s"] * len(phone_list))
        cursor.execute(f"DELETE FROM vicidial_list WHERE phone_number IN ({placeholders})", tuple(phone_list))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": f"Deleted records for: {data.phone_number}"}
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Error deleting records: {e}")


@app.post("/clients_for_agent")
def clients_for_agent(callbackOnly: Optional[bool] = False, current_user: str = Depends(get_current_user)):
    conn        = mysql.connector.connect(**DB_CONFIG)
    cursor      = conn.cursor(dictionary=True)
    user_id     = current_user["username"]
    campaign_id = current_user["campaign_id"]

    if callbackOnly:
        cursor.execute("""
            SELECT DISTINCT vl.phone_number, vl.title, vl.first_name, vl.last_name, vl.city,
                   vl.country_code, date(vl.entry_date) entry_date, vl.date_of_birth, vl.list_id,
                   vc.callback_time, vc.comments, vl.lead_id, vl.status, vls.campaign_id
            FROM vicidial_callbacks vc
            INNER JOIN vicidial_list vl ON vc.lead_id = vl.lead_id
            INNER JOIN vicidial_lists vls ON vl.list_id = vls.list_id
            LEFT JOIN vicidial_live_agents vla ON vl.lead_id = vla.lead_id
            WHERE vc.status IN ('ACTIVE','LIVE') AND vc.user = %s AND vc.callback_time >= now()
              AND vla.lead_id IS NULL AND vls.campaign_id = %s AND vl.status IN ('CBR','CBHOLD')
        """, (user_id, campaign_id))
    else:
        cursor.execute("""
            SELECT DISTINCT vl.title, vl.first_name, vl.last_name, vl.city, vl.country_code,
                   date(vl.entry_date) entry_date, vl.date_of_birth, vl.list_id,
                   NULL callback_time, NULL comments, vl.lead_id, vl.status, vls.campaign_id
            FROM vicidial_list vl INNER JOIN vicidial_lists vls ON vl.list_id = vls.list_id
            WHERE vl.lead_id NOT IN (SELECT DISTINCT lead_id FROM vicidial_log)
              AND vl.status IN ('NEW') AND vls.campaign_id = %s and vl.user in (%s,'AdminR')
            ORDER BY vl.lead_id desc
        """, (campaign_id,user_id,))

    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"status": "success", "total_records": len(data), "data": data}


# ═══════════════════════════════════════════════════════════════
#  CALLING ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/call")
def call_number(phone: Optional[str] = None, current_user: str = Depends(get_current_user)):
    userDetails = None
    lead_id     = None
    agent_user  = current_user["username"]
    campaign_id = current_user["campaign_id"]

    pauseUser(current_user)
    paused = False
    for i in range(10):
        s = get_agent_status(agent_user)
        print(f"[AGENT STATUS {i+1}s]: {s}")
        if s in ("PAUSED", "PAUSE"):
            paused = True
            break
        time.sleep(1)

    if not paused:
        raise HTTPException(500, "Agent could not be paused.")

    if not phone:
        try:
            conn       = mysql.connector.connect(**DB_CONFIG)
            cursor     = conn.cursor(dictionary=True)
            lock_token = f"{agent_user}_{int(time.time()*1000)}"
            cursor.execute("""
                UPDATE vicidial_list vl
                INNER JOIN vicidial_lists vls ON vl.list_id = vls.list_id
                SET vl.status = 'INCALL', vl.user = %s
                WHERE vl.status = 'NEW' AND vls.campaign_id = %s
                  AND vl.lead_id NOT IN (SELECT lead_id FROM vicidial_log)
                ORDER BY vl.lead_id ASC LIMIT 1
            """, (lock_token, campaign_id))
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(404, "No callable leads found")
            cursor.execute("""
                SELECT vl.lead_id, vl.phone_number, vl.first_name, vl.last_name, vl.comments
                FROM vicidial_list vl
                WHERE vl.user = %s AND vl.status = 'INCALL'
                ORDER BY vl.lead_id ASC LIMIT 1
            """, (lock_token,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(404, "No callable leads found")
            lead_id     = row["lead_id"]
            phone       = row["phone_number"]
            userDetails = row
            cursor.execute("UPDATE vicidial_list SET user = %s WHERE lead_id = %s", (agent_user, lead_id))
            conn.commit()
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"DB error: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    dial_params = {
        "source": "crm", "user": API_USER, "pass": API_PASS,
        "agent_user": agent_user, "function": "external_dial",
        "phone_code": "1", "value": phone, "preview": "NO",
        "search": "YES", "focus": "YES", "lead_id": lead_id,
    }
    try:
        response = requests.get(VICIDIAL_API_URL, params=dial_params, timeout=10)
    except requests.exceptions.RequestException as e:
        raise HTTPException(500, str(e))

    if "ERROR" in response.text.upper():
        try:
            conn   = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("UPDATE vicidial_list SET status = 'NEW', user = '' WHERE lead_id = %s", (lead_id,))
            conn.commit()
        except:
            pass
        finally:
            cursor.close()
            conn.close()
        raise HTTPException(500, f"VICIdial dial error: {response.text}")

    return {"status": "success", "dialed_phone": phone, "lead_id": lead_id, "vicidial_response": response.text, "details": jsonable_encoder(userDetails)}


@app.post("/hangup")
def hangup_call(current_user: str = Depends(get_current_user)):
    user_id = current_user["username"]
    res = requests.get(VICIDIAL_API_URL, params={
        "source": "crm", "user": API_USER, "pass": API_PASS,
        "function": "external_hangup", "agent_user": user_id, "value": 1
    }, timeout=10)
    if "SUCCESS" not in res.text:
        raise HTTPException(status_code=400, detail=res.text)
    return {"status": "success", "agent_user": user_id, "vicidial_response": res.text}


@app.post("/logdata")
def logdata(request: Request, current_user: str = Depends(get_current_user)):
    try:
        user   = current_user["username"]
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT count(*) as inCall FROM vicidial_live_agents
            WHERE user = %s AND lead_id IN (SELECT DISTINCT lead_id FROM vicidial_auto_calls)
        """, (user,))
        data = cursor.fetchone()
        cursor.close()
        conn.close()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/submit_status")
def vicidial_agent_action(
    status: str,
    callback_datetime: str = None,
    callback_comments: str = None,
    current_user: str = Depends(get_current_user)
):
    responses  = {}
    agent_user = current_user["username"]

    status_params = {
        "source": "crm", "user": API_USER, "pass": API_PASS,
        "agent_user": agent_user, "function": "external_status", "value": status
    }
    if status == "CBR":
        if not callback_datetime:
            raise HTTPException(400, "callback_datetime required for CBR")
        status_params.update({
            "callback_datetime": callback_datetime,
            "callback_type":     "USERONLY",
            "callback_comments": callback_comments
        })

    status_resp = requests.get(VICIDIAL_API_URL, params=status_params, timeout=10)
    responses["status"] = status_resp.text
    if "SUCCESS" not in status_resp.text:
        raise HTTPException(400, status_resp.text)

    hangup_resp = requests.get(VICIDIAL_API_URL, params={
        "source": "crm", "user": API_USER, "pass": API_PASS,
        "agent_user": agent_user, "function": "external_hangup", "value": 1
    }, timeout=10)
    responses["hangup"] = hangup_resp.text

    time.sleep(5)
    responses["pause"] = pauseUser(current_user)

    return {
        "success":            True,
        "agent":              agent_user,
        "vicidial_responses": responses,
    }


# ═══════════════════════════════════════════════════════════════
#  STATUS ROUTE
# ═══════════════════════════════════════════════════════════════

@app.get("/status_data")
def get_status(current_user: str = Depends(get_current_user)):
    conn    = mysql.connector.connect(**DB_CONFIG)
    cursor  = conn.cursor(dictionary=True)
    user_id = current_user["username"]

    try:
        response = requests.get(vicidial_url, params={
            "source": "fastapi", "user": API_USER, "pass": API_PASS,
            "function": "agent_status", "agent_user": user_id
        })

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="VICIdial API not reachable")

        data = response.text

        if "INCALL"   in data: call_status = "IN_CALL"
        elif "QUEUE"  in data or "RINGING" in data: call_status = "RINGING"
        elif "DISPO"  in data: call_status = "DISPOSITION_PENDING"
        elif "PAUSED" in data: call_status = "PAUSED"
        elif "READY"  in data: call_status = "READY"
        else:                  call_status = "DISCONNECTED"

        return {"status": "success", "data": {"agent": user_id, "call_status": call_status}}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {e}")
    finally:
        cursor.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  SESSIONS / ONLINE TRACKING
# ═══════════════════════════════════════════════════════════════

BUFFER_SECONDS = 30

@app.get("/ping")
def ping(current_user: str = Depends(get_current_user)):
    user_id = current_user["username"]
    now     = datetime.now()
    conn    = pgsqlPool.getconn()
    cur     = conn.cursor()
    cur.execute("SELECT id, first_tick, last_tick FROM user_online_sessions WHERE user_id = %s ORDER BY last_tick DESC LIMIT 1", (user_id,))
    row = cur.fetchone()
    if row:
        session_id, first_tick, last_tick = row
        if now.date() == last_tick.date() and now - last_tick <= timedelta(seconds=BUFFER_SECONDS):
            cur.execute("UPDATE user_online_sessions SET last_tick = %s WHERE id = %s", (now, session_id))
        else:
            cur.execute("INSERT INTO user_online_sessions (user_id, first_tick, last_tick) VALUES (%s,%s,%s)", (user_id, now, now))
    else:
        cur.execute("INSERT INTO user_online_sessions (user_id, first_tick, last_tick) VALUES (%s,%s,%s)", (user_id, now, now))
    conn.commit()
    cur.close()
    pgsqlPool.putconn(conn)
    return {"status": "ok"}


@app.get("/usertimeline")
def usertimeline(current_user: str = Depends(get_current_user)):
    try:
        user_id = current_user["username"]
        conn    = pgsqlPool.getconn()
        cursor  = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT * FROM public.user_online_sessions
            WHERE user_id = %s AND date(first_tick) = CURRENT_DATE AND date(last_tick) = CURRENT_DATE
        """, (user_id,))
        data = cursor.fetchall()
        cursor.close()
        pgsqlPool.putconn(conn)
        return {"count": len(data), "leads": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health/db")
def check_db():
    conn = None
    try:
        conn = pgsqlPool.getconn()
        cur  = conn.cursor()
        cur.execute("SELECT 1")
        return {"db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="DB not connected")
    finally:
        if conn:
            pgsqlPool.putconn(conn)

@app.get("/campaigns")
def get_active_campaigns(request: Request):
    conn   = None
    cursor = None
    try:
        conn       = mysql.connector.connect(**DB_CONFIG)
        cursor     = conn.cursor(dictionary=True)
        admin_user = request.query_params.get("username")

        if admin_user:
            campaign_filter = f"""
                AND campaign_id IN (
                    SELECT campaign_id FROM vicidial_campaign_agents WHERE user = '{admin_user}'
                )
            """
        else:
            campaign_filter = ""

        cursor.execute(f"""
            SELECT campaign_id, campaign_name, active 
            FROM vicidial_campaigns 
            WHERE active = 'Y'
            {campaign_filter}
        """)
        campaigns = cursor.fetchall()
        return {"status": "success", "count": len(campaigns), "data": campaigns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()

# ═══════════════════════════════════════════════════════════════
#  MESSAGING ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/callusingzoho")
def call_using_zoho(phone: Optional[str] = None):
    pauseUser({"username": 8999})
    time.sleep(5)
    try:
        response = requests.get(VICIDIAL_API_URL, params={
            "source": "crm", "user": API_USER, "pass": API_PASS,
            "agent_user": '8999', "function": "external_dial",
            "phone_code": "1", "value": phone, "preview": "NO", "search": "YES", "focus": "YES"
        }, timeout=10)
    except requests.exceptions.RequestException as e:
        raise HTTPException(500, str(e))
    return {"status": "success", "dialed_phone": phone, "vicidial_response": response.text}


@app.post("/send-whatsapp")
async def send_whatsapp(client_number: str, message: str):
    url     = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": client_number, "type": "text", "text": {"body": message}}
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload, headers=headers)
    return response.json()


@app.post("/send-sms")
def send_sms(data: SMSRequest):
    phone_number = data.phone_number
    message_body = data.custom_message or f"Hello! Connect with us on WhatsApp: {WHATSAPP_LINK}"
    try:
        message = twilio_client.messages.create(
            body=message_body, messaging_service_sid=TWILIO_MESSAGING_SERVICE_SID, to=phone_number
        )
        return {"success": True, "message_sid": message.sid, "status": message.status, "to_phone_number": phone_number}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



# ═══════════════════════════════════════════════════════════════
#  META / FACEBOOK LEAD WEBHOOK
#  Add these env vars to your .env:
#    VERIFY_TOKEN=your_verify_token
#    PAGE_ACCESS_TOKEN=your_page_access_token
#    META_DEFAULT_LIST_ID=your_vicidial_list_id
#    META_DEFAULT_CAMPAIGN_ID=your_vicidial_campaign_id
# ═══════════════════════════════════════════════════════════════

VERIFY_TOKEN         = os.getenv("VERIFY_TOKEN")
PAGE_ACCESS_TOKEN    = os.getenv("PAGE_ACCESS_TOKEN")
META_DEFAULT_LIST_ID = os.getenv("META_DEFAULT_LIST_ID", "")
META_DEFAULT_CAMP_ID = os.getenv("META_DEFAULT_CAMPAIGN_ID", "")


# ─────────────────────────────────────────────
# HELPER: fetch full lead from Graph API
# ─────────────────────────────────────────────
async def fetch_meta_lead(leadgen_id: str) -> dict:
    url = f"https://graph.facebook.com/v19.0/{leadgen_id}"
    async with httpx.AsyncClient() as client:
        res = await client.get(url, params={"access_token": PAGE_ACCESS_TOKEN})
        res.raise_for_status()
        return res.json()


# ─────────────────────────────────────────────
# HELPER: parse field_data list → flat dict
# ─────────────────────────────────────────────
def parse_lead_fields(field_data: list) -> dict:
    """
    Facebook returns: [{"name": "phone_number", "values": ["0501234567"]}, ...]
    We flatten it to: {"phone_number": "0501234567", ...}
    """
    result = {}
    for field in field_data:
        key   = field.get("name", "").lower().replace(" ", "_")
        vals  = field.get("values", [])
        result[key] = vals[0] if vals else ""
    return result


# ─────────────────────────────────────────────
# HELPER: push one lead into VICIdial
# ─────────────────────────────────────────────
def push_lead_to_vicidial(phone: str, first_name: str, last_name: str,
                           email: str, list_id: str) -> dict:
    params = {
        "source":       SOURCE,
        "user":         vici_user,
        "pass":         Vici_pass,
        "function":     "add_lead",
        "phone_number": phone,
        "phone_code":   "1",
        "list_id":      7022026,
        "first_name":   first_name,
        "last_name":    last_name,
        "email":        email,
    }
    try:
        res = requests.get(vicidial_url, params=params, timeout=10)
        success = "SUCCESS" in res.text.upper()
        return {"success": success, "response": res.text}
    except Exception as e:
        return {"success": False, "response": str(e)}


# ═══════════════════════════════════════════════════════════════
#  WEBHOOK ROUTES
# ═══════════════════════════════════════════════════════════════

from fastapi.responses import PlainTextResponse, HTMLResponse

# ── Webhook verification (Facebook handshake) ─────────────────
@app.get("/webhook/facebook")
def verify_facebook_webhook(
    hub_mode:         str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge:    str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        print("[Meta Webhook] Verified successfully")
        return PlainTextResponse(content=hub_challenge, status_code=200)
    print("[Meta Webhook] Verification FAILED")
    return PlainTextResponse(content="Forbidden", status_code=403)


# ── Receive lead events ────────────────────────────────────────
@app.post("/webhook/facebook")
async def receive_facebook_webhook(request: Request):
    body = await request.json()
    print("[Meta Webhook] Incoming:", json.dumps(body, indent=2))

    results = []

    try:
        for entry in body.get("entry", []):
            for change in entry.get("changes", []):
                if change.get("field") != "leadgen":
                    continue

                leadgen_id  = change["value"].get("leadgen_id")
                form_id     = change["value"].get("form_id", "")
                page_id     = change["value"].get("page_id", "")

                if not leadgen_id:
                    continue

                print(f"[Meta Webhook] New lead: leadgen_id={leadgen_id}")

                # 1) Fetch full lead data from Graph API
                try:
                    lead_data  = await fetch_meta_lead(leadgen_id)
                except Exception as e:
                    print(f"[Meta Webhook] Failed to fetch lead {leadgen_id}: {e}")
                    results.append({"leadgen_id": leadgen_id, "error": str(e)})
                    continue

                print(f"[Meta Webhook] Lead data: {json.dumps(lead_data, indent=2)}")

                # 2) Parse fields
                field_data  = lead_data.get("field_data", [])
                fields      = parse_lead_fields(field_data)

                phone       = clean_phone(
                    fields.get("phone_number")
                    or fields.get("phone")
                    or fields.get("mobile_number")
                    or ""
                )
                first_name  = fields.get("first_name", "").strip()
                last_name   = fields.get("last_name",  "").strip()
                full_name   = fields.get("full_name",  "").strip()
                email       = fields.get("email",      "").strip()

                # Handle full_name if split names not provided
                if not first_name and full_name:
                    parts      = full_name.split(" ", 1)
                    first_name = parts[0]
                    last_name  = parts[1] if len(parts) > 1 else ""

                list_id     = META_DEFAULT_LIST_ID
                campaign_id = META_DEFAULT_CAMP_ID

                # 3) Validate phone
                if not phone or not phone.isdigit():
                    print(f"[Meta Webhook] Invalid phone for lead {leadgen_id}: '{phone}'")
                    results.append({
                        "leadgen_id": leadgen_id,
                        "error":      "Invalid or missing phone number",
                        "raw_fields": fields
                    })
                    continue

                # 4) Validate list belongs to campaign
                if list_id and campaign_id:
                    if not validate_list_campaign(list_id, campaign_id):
                        print(f"[Meta Webhook] list_id {list_id} not valid for campaign {campaign_id}")
                        results.append({
                            "leadgen_id": leadgen_id,
                            "error":      f"list_id {list_id} not in campaign {campaign_id}"
                        })
                        continue

                # 5) Push to VICIdial
                vici_result = push_lead_to_vicidial(
                    phone=phone, first_name=first_name,
                    last_name=last_name, email=email,
                    list_id=list_id
                )

                print(f"[Meta Webhook] VICIdial result for {phone}: {vici_result}")
                results.append({
                    "leadgen_id":  leadgen_id,
                    "phone":       phone,
                    "first_name":  first_name,
                    "last_name":   last_name,
                    "email":       email,
                    "list_id":     list_id,
                    "vici_result": vici_result
                })

    except Exception as e:
        print(f"[Meta Webhook] Unhandled error: {e}")
        # Always return 200 to Facebook or it will keep retrying
        return PlainTextResponse(content="OK", status_code=200)

    return PlainTextResponse(content="OK", status_code=200)



# ── Privacy / Terms / Delete (required by Meta app review) ────
@app.get("/privacy", response_class=HTMLResponse)
def privacy_policy():
    return f"""
    <h1>Privacy Policy</h1>
    <p>Last updated: {date.today().strftime("%B %d, %Y")}</p>
    <p>Spectra Global Ltd. collects and processes user data obtained from Facebook Lead Ads.</p>
    <h2>Information We Collect</h2>
    <p>Name, email, phone number, and other details submitted through Facebook Lead Ads forms.</p>
    <h2>How We Use Information</h2>
    <p>Data is used solely for business communication, lead management, and customer support.</p>
    <h2>Data Sharing</h2>
    <p>We do not sell or share your personal data with third parties.</p>
    <h2>Data Security</h2>
    <p>We take reasonable steps to protect your data from unauthorized access.</p>
    <h2>User Rights</h2>
    <p>You can request access or deletion of your data at any time.</p>
    <h2>Contact Us</h2>
    <p>Email: sgfxglobal@gmail.com</p>
    """

@app.get("/terms", response_class=HTMLResponse)
def terms_of_service():
    return f"""
    <h1>Terms of Service</h1>
    <p>Last updated: {date.today().strftime("%B %d, %Y")}</p>
    <p>By using our services, you agree to the following terms.</p>
    <h2>Use of Service</h2>
    <p>Our service collects and manages leads from Facebook Lead Ads.</p>
    <h2>User Responsibility</h2>
    <p>You agree to provide accurate information when submitting forms.</p>
    <h2>Data Usage</h2>
    <p>Submitted data may be used for communication and business purposes.</p>
    <h2>Limitation of Liability</h2>
    <p>We are not liable for any damages arising from the use of this service.</p>
    <h2>Contact</h2>
    <p>Email: sgfxglobal@gmail.com</p>
    """

@app.get("/delete-data")
def delete_data_info():
    return PlainTextResponse("Send your data deletion request to sgfxglobal@gmail.com")



