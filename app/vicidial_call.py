# ═══════════════════════════════════════════════════════════════
#  COMBINED API — VICIdial Dashboard + Call Quality Analyzer
#  v7.0.0 — All bugs fixed
# ═══════════════════════════════════════════════════════════════

from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile, File, Depends, status, APIRouter
from fastapi.responses import JSONResponse
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
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import quote
from openai import OpenAI

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
vicidial_url     = "http://192.168.15.165/vicidial/non_agent_api.php"
VICIDIAL_API_URL = "http://192.168.15.165/agc/api.php"
API_USER         = "AdminR"
API_PASS         = "AdminR"
vici_user        = "AdminR"
Vici_pass        = "AdminR"
SOURCE           = "FASTAPI"

# ─────────────────────────────────────────────
# OPENAI CONFIG
# ─────────────────────────────────────────────
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────
# POLLER STATE
# ─────────────────────────────────────────────
POLL_INTERVAL_SECONDS   = 10
AUTO_SYNC_INTERVAL_SECS = 30 * 60   # 30 minutes

_agent_last_status: dict = {}
_poller_lock             = threading.Lock()

# ─────────────────────────────────────────────
# NO-ANSWER DISPOSITIONS
# ─────────────────────────────────────────────
NO_ANSWER_STATUSES = {"B", "N", "D", "INVN", "WN"}

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
app = FastAPI(title="VICIdial Dashboard + Call Quality Analyzer", version="7.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://192.168.15.104:5000", "http://localhost:5000"],
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

# ─────────────────────────────────────────────
# GPT SYSTEM PROMPT
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are an expert call center quality analyst with 15+ years of experience.
Evaluate the agent's performance on the call and respond ONLY with valid JSON — no markdown, no extra text.

IMPORTANT — BEFORE evaluating, check if the call was actually connected and had a real conversation.
If the transcript is empty, very short (under 10 words), or clearly indicates:
  - No answer / unanswered call
  - Busy tone / line busy
  - Wrong number / number not in service
  - Voicemail only (no agent conversation)
  - Automated message only (IVR, bot, or system message)
  - Call dropped immediately with no dialogue

Then respond with EXACTLY this JSON and nothing else:
{
  "overall_rating": 0,
  "stars": 0,
  "summary": "Call not connected — no conversation to evaluate.",
  "call_outcome": "Not Connected",
  "agent_sentiment": "N/A",
  "client_sentiment": "N/A",
  "categories": {
    "greeting_professionalism":  { "score": 0, "comment": "Not applicable — call not connected." },
    "product_knowledge":         { "score": 0, "comment": "Not applicable — call not connected." },
    "convincing_ability":        { "score": 0, "comment": "Not applicable — call not connected." },
    "objection_handling":        { "score": 0, "comment": "Not applicable — call not connected." },
    "communication_clarity":     { "score": 0, "comment": "Not applicable — call not connected." },
    "empathy_patience":          { "score": 0, "comment": "Not applicable — call not connected." },
    "closing_technique":         { "score": 0, "comment": "Not applicable — call not connected." }
  },
  "strengths":    [],
  "improvements": []
}

Only if the call WAS connected and had a real conversation, use this format:
{
  "overall_rating": <float 1.0-5.0>,
  "stars": <integer 1-5>,
  "summary": "<2-3 sentence overall summary>",
  "call_outcome": "<Successful Sale | Lead Generated | Not Converted | Support Resolved | Escalated>",
  "agent_sentiment": "<Positive | Neutral | Negative>",
  "client_sentiment": "<Positive | Neutral | Negative | Frustrated | Interested>",
  "categories": {
    "greeting_professionalism":  { "score": <1-5>, "comment": "<feedback>" },
    "product_knowledge":         { "score": <1-5>, "comment": "<feedback>" },
    "convincing_ability":        { "score": <1-5>, "comment": "<feedback>" },
    "objection_handling":        { "score": <1-5>, "comment": "<feedback>" },
    "communication_clarity":     { "score": <1-5>, "comment": "<feedback>" },
    "empathy_patience":          { "score": <1-5>, "comment": "<feedback>" },
    "closing_technique":         { "score": <1-5>, "comment": "<feedback>" }
  },
  "strengths":    ["<strength 1>", "<strength 2>", "<strength 3>"],
  "improvements": ["<area 1>",     "<area 2>",     "<area 3>"]
}

Rating scale: 1=Very Poor  2=Below Average  3=Average  4=Good  5=Excellent
"""


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

def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(minutes=1440)})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload       = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username      = payload.get("sub")
        is_admin      = payload.get("isAdmin", False)
        campaign_name = payload.get("campaign_name")
        campaign_id   = payload.get("campaign_id")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"username": username, "isAdmin": is_admin, "campaign_name": campaign_name, "campaign_id": campaign_id}
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

def filename_exists_in_pg(filename: str) -> bool:
    if not filename:
        return False
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM call_analysis WHERE filename = %s LIMIT 1", (filename,))
        exists = cur.fetchone() is not None
        cur.close()
        return exists
    finally:
        conn.close()


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


# ═══════════════════════════════════════════════════════════════
#  CALL QUALITY ANALYZER — CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def create_call_analysis_table():
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS call_analysis (
                id                          SERIAL PRIMARY KEY,
                recording_id                VARCHAR(100),
                channel                     VARCHAR(255),
                server_ip                   VARCHAR(50),
                extension                   VARCHAR(50),
                start_time                  TIMESTAMP,
                start_epoch                 BIGINT,
                end_time                    TIMESTAMP,
                end_epoch                   BIGINT,
                length_in_sec               INTEGER,
                length_in_min               FLOAT,
                filename                    VARCHAR(255) UNIQUE,
                location                    TEXT,
                phone_number                VARCHAR(50),
                lead_id                     VARCHAR(50),
                agent_user                  VARCHAR(100),
                vicidial_id                 VARCHAR(100),
                synced_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                transcript                  TEXT,
                overall_rating              FLOAT,
                stars                       INTEGER,
                summary                     TEXT,
                call_outcome                VARCHAR(100),
                agent_sentiment             VARCHAR(50),
                client_sentiment            VARCHAR(50),
                greeting_score              INTEGER,
                greeting_comment            TEXT,
                product_knowledge_score     INTEGER,
                product_knowledge_comment   TEXT,
                convincing_score            INTEGER,
                convincing_comment          TEXT,
                objection_score             INTEGER,
                objection_comment           TEXT,
                clarity_score               INTEGER,
                clarity_comment             TEXT,
                empathy_score               INTEGER,
                empathy_comment             TEXT,
                closing_score               INTEGER,
                closing_comment             TEXT,
                strengths                   TEXT,
                improvements                TEXT,
                analyzed_at                 TIMESTAMP,
                status                      VARCHAR(20) DEFAULT 'pending'
            )
        """)
        conn.commit()
        cur.close()
        print("[DB] Table 'call_analysis' ready.")
    finally:
        conn.close()

def extract_phone_number(location: str):
    if not location:
        return None
    match = re.search(r'\d{8}-\d{6}_(\d+)_', location)
    return match.group(1) if match else None


# ─────────────────────────────────────────────
# DISPOSITION LOOKUP
# ─────────────────────────────────────────────

def get_vici_disposition_for_recording(mysql_cur, lead_id: str, start_time) -> str:
    if not lead_id:
        return ""
    try:
        mysql_cur.execute("""
            SELECT status FROM vicidial_log
            WHERE lead_id = %s
              AND call_date BETWEEN DATE_SUB(%s, INTERVAL 5 MINUTE)
                                AND DATE_ADD(%s, INTERVAL 5 MINUTE)
            ORDER BY call_date DESC LIMIT 1
        """, (lead_id, start_time, start_time))
        row = mysql_cur.fetchone()
        return (row.get("status") or "").upper() if row else ""
    except Exception as e:
        print(f"[Sync] Warning: could not look up disposition for lead {lead_id}: {e}")
        return ""


# ─────────────────────────────────────────────
# SYNC — MySQL recording_log → PostgreSQL call_analysis
# ─────────────────────────────────────────────

def sync_recording_log(target_date: str = None) -> dict:
    """
    Sync MySQL recording_log → PostgreSQL call_analysis.
    - Only fetches rows where location IS NOT NULL (VICIdial writes location after file is saved)
    - Skips no-answer dispositions
    - Skips rows already in PG (any status)
    - Uses RETURNING id to get new row id without extra SELECT
    - Backfills location for any PG rows previously inserted with NULL location
    """
    if not target_date:
        target_date = today_str()

    mysql_conn        = get_mysql_conn()
    inserted          = 0
    skipped           = 0
    skipped_no_answer = 0
    location_fixed    = 0
    errors            = []
    inserted_rows     = []

    try:
        # ── Step 1: Fetch from MySQL — only rows with location populated ──
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    recording_id, channel, server_ip, extension,
                    start_time, start_epoch, end_time, end_epoch,
                    length_in_sec, length_in_min, filename, location,
                    lead_id, user AS agent_user, vicidial_id
                FROM recording_log
                WHERE DATE(start_time) = %s
                  AND location IS NOT NULL
                  AND location != ''
                ORDER BY start_time DESC
            """, (target_date,))
            rows = cur.fetchall()

        print(f"[Sync] Fetched {len(rows)} rows from MySQL for {target_date}")

        # ── Step 2: Insert each row into PG ──
        with mysql_conn.cursor() as dispo_cur:
            for row in rows:
                filename = (row.get("filename") or "").strip()
                rec_id   = str(row.get("recording_id")) if row.get("recording_id") is not None else None

                # skip empty filename
                if not filename:
                    errors.append("Row with empty filename skipped")
                    skipped += 1
                    continue

                # skip if already in PG (any status)
                if filename_exists_in_pg(filename):
                    skipped += 1
                    print(f"[Sync] ~ Already in PG: {filename}")
                    continue

                # skip no-answer dispositions
                lead_id     = row.get("lead_id")
                start_time  = row.get("start_time")
                disposition = get_vici_disposition_for_recording(dispo_cur, lead_id, start_time)
                if disposition in NO_ANSWER_STATUSES:
                    skipped_no_answer += 1
                    print(f"[Sync] ✗ No-answer ({disposition}): {filename}")
                    continue

                phone = extract_phone_number(row.get("location") or "")

                # fresh PG connection per insert — no shared cursor state issues
                try:
                    pg = get_pg_conn()
                    try:
                        cur_pg = pg.cursor()
                        cur_pg.execute("""
                            INSERT INTO call_analysis (
                                recording_id, channel, server_ip, extension,
                                start_time, start_epoch, end_time, end_epoch,
                                length_in_sec, length_in_min, filename, location,
                                phone_number, lead_id, agent_user, vicidial_id
                            ) VALUES (
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                            )
                            ON CONFLICT (filename) DO NOTHING
                            RETURNING id
                        """, (
                            rec_id,
                            row.get("channel"),
                            row.get("server_ip"),
                            row.get("extension"),
                            row.get("start_time"),
                            row.get("start_epoch"),
                            row.get("end_time"),
                            row.get("end_epoch"),
                            row.get("length_in_sec"),
                            row.get("length_in_min"),
                            filename,
                            row.get("location"),
                            phone,
                            lead_id,
                            row.get("agent_user"),
                            row.get("vicidial_id"),
                        ))
                        result_row = cur_pg.fetchone()
                        pg.commit()
                        cur_pg.close()

                        if result_row:
                            new_id = result_row[0]
                            inserted += 1
                            print(f"[Sync] ✓ Inserted id={new_id}: {filename}")
                            inserted_rows.append({
                                "id":            new_id,
                                "filename":      filename,
                                "location":      row.get("location"),
                                "lead_id":       lead_id,
                                "agent_user":    row.get("agent_user"),
                                "length_in_sec": row.get("length_in_sec"),
                            })
                        else:
                            skipped += 1
                            print(f"[Sync] ~ Conflict skipped: {filename}")
                    except Exception:
                        pg.rollback()
                        raise
                    finally:
                        pg.close()

                except Exception as e:
                    err_msg = f"{filename} → {str(e)}"
                    errors.append(err_msg)
                    print(f"[Sync ERROR] {err_msg}")

        # ── Step 3: Backfill location for PG rows that had NULL location ──
        try:
            pg = get_pg_conn()
            cur_pg = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur_pg.execute("""
                SELECT id, filename FROM call_analysis
                WHERE DATE(start_time) = %s
                  AND status = 'pending'
                  AND (location IS NULL OR location = '')
            """, (target_date,))
            null_rows = cur_pg.fetchall()

            if null_rows:
                print(f"[Sync] Found {len(null_rows)} PG row(s) with NULL location — backfilling...")
                with mysql_conn.cursor() as fix_cur:
                    for pg_row in null_rows:
                        fix_cur.execute("""
                            SELECT location, length_in_sec, length_in_min
                            FROM recording_log
                            WHERE filename = %s
                              AND location IS NOT NULL AND location != ''
                            LIMIT 1
                        """, (pg_row["filename"],))
                        mysql_row = fix_cur.fetchone()
                        if mysql_row and mysql_row.get("location"):
                            cur_pg.execute("""
                                UPDATE call_analysis
                                SET location      = %s,
                                    length_in_sec = %s,
                                    length_in_min = %s
                                WHERE id = %s
                            """, (
                                mysql_row["location"],
                                mysql_row.get("length_in_sec"),
                                mysql_row.get("length_in_min"),
                                pg_row["id"],
                            ))
                            pg.commit()
                            location_fixed += 1
                            print(f"[Sync] ✓ Backfilled id={pg_row['id']}: {pg_row['filename']}")

            cur_pg.close()
            pg.close()
        except Exception as e:
            print(f"[Sync] Warning: backfill step failed: {e}")

        print(
            f"[Sync] Done — Inserted: {inserted} | Skipped: {skipped} | "
            f"No-answer: {skipped_no_answer} | Backfilled: {location_fixed} | Errors: {len(errors)}"
        )

        return {
            "date":               target_date,
            "inserted":           inserted,
            "skipped":            skipped,
            "skipped_no_answer":  skipped_no_answer,
            "location_fixed":     location_fixed,
            "error_count":        len(errors),
            "errors":             errors,
            "total_fetched":      len(rows),
            "inserted_rows":      inserted_rows,
        }

    finally:
        mysql_conn.close()


def get_unanalyzed(target_date: str = None) -> list:
    """
    Return all pending rows for the given date that are ready to analyze:
    - status = 'pending'
    - location is set (not null/empty)
    - length > 10 seconds
    """
    if not target_date:
        target_date = today_str()

    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, recording_id, filename, location, lead_id, agent_user, length_in_sec
            FROM call_analysis
            WHERE status = 'pending'
              AND location IS NOT NULL
              AND location != ''
              AND DATE(start_time) = %s
              AND length_in_sec > 10
            ORDER BY start_time ASC
        """, (target_date,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


# ─────────────────────────────────────────────
# AUDIO PIPELINE
# ─────────────────────────────────────────────

SUPPORTED_AUDIO_EXTENSIONS = {".mp3"}  # only mp3 on this server


class RecordingNotFoundError(Exception):
    """Raised when the audio file URL returns 404."""
    pass


def normalize_audio_url(url: str) -> str:
    """
    Always return a .mp3 URL.
    VICIdial recordings are stored as .mp3 only.
    Handles: already .mp3 | .wav/.gsm/.ogg/.m4a (replace) | no extension (append)
    """
    clean  = url.split("?")[0].rstrip("/")
    suffix = Path(clean).suffix.lower()

    if suffix == ".mp3":
        return url

    if suffix in {".wav", ".gsm", ".ogg", ".m4a"}:
        return url[:url.lower().rfind(suffix)] + ".mp3"

    # no extension — append .mp3
    return url + ".mp3"


def download_audio(url: str) -> str:
    url = normalize_audio_url(url)
    print(f"[Download] Trying: {url}")

    r = requests.get(url, timeout=60)

    if r.status_code == 404:
        raise RecordingNotFoundError(f"Recording not found (404): {url}")

    if r.status_code != 200:
        raise Exception(f"HTTP {r.status_code} downloading audio: {url}")

    tmp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
    tmp.write(r.content)
    tmp.flush()
    tmp.close()
    print(f"[Download] ✓ Saved → {tmp.name}")
    return tmp.name


def transcribe_audio(file_path: str) -> str:
    with open(file_path, "rb") as f:
        response = openai_client.audio.transcriptions.create(
            model="whisper-1", file=f, response_format="text"
        )
    return response


def rate_agent(transcript: str) -> dict:
    response = openai_client.chat.completions.create(
        model="gpt-4o", temperature=0.3, max_tokens=1500,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": (
                "Analyze this call transcript and rate the agent's performance.\n"
                "Focus on convincing ability and objection handling.\n\n"
                f"--- TRANSCRIPT ---\n{transcript}\n--- END ---"
            )}
        ]
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def update_analysis(row_id: int, transcript: str, rating: dict):
    conn = get_pg_conn()
    try:
        cats = rating.get("categories", {})
        cur  = conn.cursor()
        cur.execute("""
            UPDATE call_analysis SET
                transcript                  = %s,
                overall_rating              = %s,
                stars                       = %s,
                summary                     = %s,
                call_outcome                = %s,
                agent_sentiment             = %s,
                client_sentiment            = %s,
                greeting_score              = %s,
                greeting_comment            = %s,
                product_knowledge_score     = %s,
                product_knowledge_comment   = %s,
                convincing_score            = %s,
                convincing_comment          = %s,
                objection_score             = %s,
                objection_comment           = %s,
                clarity_score               = %s,
                clarity_comment             = %s,
                empathy_score               = %s,
                empathy_comment             = %s,
                closing_score               = %s,
                closing_comment             = %s,
                strengths                   = %s,
                improvements                = %s,
                analyzed_at                 = CURRENT_TIMESTAMP,
                status                      = 'success'
            WHERE id = %s
        """, (
            transcript,
            rating.get("overall_rating"),
            rating.get("stars"),
            rating.get("summary"),
            rating.get("call_outcome"),
            rating.get("agent_sentiment"),
            rating.get("client_sentiment"),
            cats.get("greeting_professionalism", {}).get("score"),
            cats.get("greeting_professionalism", {}).get("comment"),
            cats.get("product_knowledge", {}).get("score"),
            cats.get("product_knowledge", {}).get("comment"),
            cats.get("convincing_ability", {}).get("score"),
            cats.get("convincing_ability", {}).get("comment"),
            cats.get("objection_handling", {}).get("score"),
            cats.get("objection_handling", {}).get("comment"),
            cats.get("communication_clarity", {}).get("score"),
            cats.get("communication_clarity", {}).get("comment"),
            cats.get("empathy_patience", {}).get("score"),
            cats.get("empathy_patience", {}).get("comment"),
            cats.get("closing_technique", {}).get("score"),
            cats.get("closing_technique", {}).get("comment"),
            json.dumps(rating.get("strengths", [])),
            json.dumps(rating.get("improvements", [])),
            row_id
        ))
        conn.commit()
        cur.close()
        print(f"[DB] ✓ Updated row id={row_id}")
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] update_analysis failed for id={row_id}: {e}")
        raise
    finally:
        conn.close()


def mark_status(row_id: int, status: str):
    """Helper to mark a row with a given status."""
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE call_analysis SET status = %s, analyzed_at = CURRENT_TIMESTAMP WHERE id = %s",
            (status, row_id)
        )
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[DB] Warning: could not mark status={status} for id={row_id}: {e}")
    finally:
        conn.close()


def process_recording(row: dict) -> dict:
    location = (row.get("location") or "").strip()
    filename = row.get("filename", "")
    row_id   = row.get("id")

    if not location:
        raise Exception(f"No location for filename={filename}")

    tmp_path = None
    try:
        # ── Download audio ──
        try:
            tmp_path = download_audio(location)
        except RecordingNotFoundError as e:
            print(f"[NoFile]  {filename} → {e}")
            mark_status(row_id, "no_recording")
            return {"id": row_id, "filename": filename, "status": "no_recording", "reason": str(e)}

        # ── Transcribe ──
        transcript       = transcribe_audio(tmp_path)
        transcript_clean = transcript.strip()
        print(f"[Whisper] {filename} → {len(transcript_clean)} chars")

        if len(transcript_clean) < 20:
            print(f"[Skip]    {filename} → transcript too short, marking skipped")
            mark_status(row_id, "skipped")
            return {"id": row_id, "filename": filename, "status": "skipped", "reason": "transcript too short"}

        # ── Rate ──
        rating  = rate_agent(transcript_clean)
        stars   = rating.get("stars", 0)
        outcome = rating.get("call_outcome", "")

        if stars == 0 or outcome == "Not Connected":
            print(f"[Skip]    {filename} → Not connected / no conversation — marking skipped")
            mark_status(row_id, "skipped")
            return {
                "id":           row_id,
                "filename":     filename,
                "stars":        0,
                "call_outcome": "Not Connected",
                "summary":      rating.get("summary", "Call not connected — no conversation to evaluate."),
                "status":       "skipped",
            }

        print(f"[GPT-4o]  {filename} → {stars}/5 stars")
        update_analysis(row_id, transcript_clean, rating)

        return {
            "id":               row_id,
            "filename":         filename,
            "stars":            stars,
            "overall_rating":   rating.get("overall_rating"),
            "call_outcome":     outcome,
            "summary":          rating.get("summary"),
            "agent_sentiment":  rating.get("agent_sentiment"),
            "client_sentiment": rating.get("client_sentiment"),
            "categories":       rating.get("categories"),
            "strengths":        rating.get("strengths"),
            "improvements":     rating.get("improvements"),
            "transcript":       transcript_clean,
            "status":           "success",
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ═══════════════════════════════════════════════════════════════
#  AGENT STATUS POLLER (BACKGROUND THREAD)
# ═══════════════════════════════════════════════════════════════

def get_vicidial_agent_status_api(agent_user: str) -> str:
    try:
        response = requests.get(vicidial_url, params={
            "source": "fastapi", "user": API_USER, "pass": API_PASS,
            "function": "agent_status", "agent_user": agent_user,
        }, timeout=10)
        if response.status_code != 200:
            return "DISCONNECTED"
        data = response.text
        if "INCALL"  in data: return "IN_CALL"
        if "QUEUE"   in data or "RINGING" in data: return "RINGING"
        if "DISPO"   in data: return "DISPOSITION_PENDING"
        if "PAUSED"  in data: return "PAUSED"
        if "READY"   in data: return "READY"
        return "DISCONNECTED"
    except Exception as e:
        print(f"[Poller] Error fetching status for {agent_user}: {e}")
        return "DISCONNECTED"


def get_all_active_agents() -> list:
    try:
        conn = get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT user FROM vicidial_live_agents WHERE user != ''")
            agents = [row["user"] for row in cur.fetchall()]
        conn.close()
        return agents
    except Exception as e:
        print(f"[Poller] Error fetching agent list: {e}")
        return []


def trigger_sync_and_analyze_for_agent(agent_user: str, disposition: str = ""):
    # fast-path: skip no-answer dispositions entirely
    if disposition.upper() in NO_ANSWER_STATUSES:
        print(f"[Trigger] Agent '{agent_user}' disposed '{disposition}' — no-answer, skipping")
        return

    print(f"[Trigger] Agent '{agent_user}' → syncing+analyzing (disposition='{disposition}')")
    today = today_str()

    try:
        sync_result = sync_recording_log(target_date=today)
        print(f"[Trigger] Sync: inserted={sync_result['inserted']} skipped={sync_result['skipped']}")
    except Exception as e:
        print(f"[Trigger] Sync failed for {agent_user}: {e}")
        return

    # fetch the most recent pending call for this agent
    try:
        conn = get_pg_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, recording_id, filename, location, lead_id, agent_user, length_in_sec
            FROM call_analysis
            WHERE status = 'pending'
              AND location IS NOT NULL
              AND location != ''
              AND agent_user = %s
              AND DATE(start_time) = %s
              AND length_in_sec > 10
            ORDER BY start_time DESC
            LIMIT 1
        """, (agent_user, today))
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Trigger] DB query failed for {agent_user}: {e}")
        return

    if not row:
        print(f"[Trigger] No pending call found for agent '{agent_user}' today.")
        return

    try:
        result = process_recording(dict(row))
        print(f"[Trigger] ✓ {agent_user} | {result['filename']} | {result.get('stars', 0)}/5 stars")
    except Exception as e:
        print(f"[Trigger] Analysis failed for {agent_user}: {e}")
        mark_status(row["id"], "failed")


def poll_agent_statuses():
    print(f"[Poller] Started — checking every {POLL_INTERVAL_SECONDS}s")
    while True:
        try:
            agents = get_all_active_agents()
            for agent_user in agents:
                current_status = get_vicidial_agent_status_api(agent_user)
                with _poller_lock:
                    prev_status = _agent_last_status.get(agent_user)
                    if current_status == "DISPOSITION_PENDING" and prev_status != "DISPOSITION_PENDING":
                        print(f"[Poller] {agent_user}: {prev_status} → DISPOSITION_PENDING ← TRIGGER!")
                        _agent_last_status[agent_user] = current_status
                        threading.Thread(
                            target=trigger_sync_and_analyze_for_agent,
                            args=(agent_user, ""),
                            daemon=True
                        ).start()
                    else:
                        if prev_status != current_status:
                            print(f"[Poller] {agent_user}: {prev_status} → {current_status}")
                        _agent_last_status[agent_user] = current_status
        except Exception as e:
            print(f"[Poller] Unexpected error: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


def auto_sync_and_analyze_loop():
    print(f"[AutoLoop] Started — will sync+analyze every {AUTO_SYNC_INTERVAL_SECS // 60} minutes")
    while True:
        try:
            today = today_str()
            print(f"[AutoLoop] ─── Running sync+analyze for {today} ───")

            # Step 1: Sync MySQL → PG
            try:
                sync_result = sync_recording_log(target_date=today)
                print(
                    f"[AutoLoop] Sync done — inserted={sync_result['inserted']} | "
                    f"skipped={sync_result['skipped']} | no-answer={sync_result['skipped_no_answer']} | "
                    f"backfilled={sync_result.get('location_fixed', 0)} | errors={sync_result['error_count']}"
                )
            except Exception as e:
                print(f"[AutoLoop] Sync failed: {e}")

            # Step 2: Analyze ALL pending rows for today
            try:
                all_pending = get_unanalyzed(target_date=today)
                print(f"[AutoLoop] Found {len(all_pending)} pending recording(s) to analyze...")

                processed = 0
                failed    = 0

                for row in all_pending:
                    try:
                        result = process_recording(row)
                        print(
                            f"[AutoLoop] ✓ {result['filename']} | "
                            f"{result.get('stars', 0)}/5 stars | {result.get('call_outcome', '')}"
                        )
                        processed += 1
                    except Exception as e:
                        failed += 1
                        print(f"[AutoLoop] ✗ {row.get('filename')}: {e}")
                        mark_status(row["id"], "failed")

                print(f"[AutoLoop] ─── Done — processed={processed} | failed={failed} ───")

            except Exception as e:
                print(f"[AutoLoop] Analyze step failed: {e}")

        except Exception as e:
            print(f"[AutoLoop] Unexpected error: {e}")

        print(f"[AutoLoop] Sleeping {AUTO_SYNC_INTERVAL_SECS // 60} minutes...")
        time.sleep(AUTO_SYNC_INTERVAL_SECS)


# ═══════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup():
    create_call_analysis_table()
    threading.Thread(target=poll_agent_statuses, daemon=True).start()
    print("[Startup] Agent status poller started.")
    threading.Thread(target=auto_sync_and_analyze_loop, daemon=True).start()
    print("[Startup] Auto sync+analyze loop started (every 30 minutes).")


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
            "sub": user[0], "isAdmin": user[3] == 9,
            "campaign_id": data.campaign_id, "campaign_name": data.campaign_name
        })
        return {
            "status": "success", "user": user[0], "full_name": user[1],
            "access_token": access_token, "isAdmin": user[3] == 9,
            "campaign_id": data.campaign_id, "campaign_name": data.campaign_name,
            "token_type": "bearer"
        }
    except Exception as e:
        print("ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
#  DASHBOARD ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get('/getcallswaiting')
def get_waitingcalls():
    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT u.user AS agent_id, u.full_name AS agent_name,
                   vla.campaign_id, vla.status, vla.calls_today AS calls_handled
            FROM vicidial_live_agents vla
            LEFT JOIN vicidial_users u ON vla.user = u.user
            ORDER BY vla.status, vla.calls_today DESC
        """)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"count": len(result), "data": result}
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/getagnetstimeoncall')
def get_agents_time_on_call():
    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT extension AS STATION, user AS USER, status AS STATUS, calls_today AS CALLS,
                   (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_state_change)) AS TALK_TIME_SECONDS
            FROM vicidial_live_agents ORDER BY status
        """)
        result = cursor.fetchall()
        for row in result:
            row["TALK_TIME_HH_MM_SS"] = format_time(row.get("TALK_TIME_SECONDS", 0))
        cursor.close()
        conn.close()
        return {"count": len(result), "data": result}
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/get_all_data')
def get_all_data():
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        today_start = date.today()

        cursor.execute("SELECT avg(auto_dial_level) AS dialer_level FROM vicidial_campaigns")
        dialer_level_result = cursor.fetchall()
        cursor.execute("""
            SELECT COUNT(*) AS dialable_leads FROM vicidial_list vl
            JOIN vicidial_lists vli ON vl.list_id = vli.list_id
            WHERE vli.active = 'Y' AND vl.called_since_last_reset = 'N'
        """)
        dialable_leads_result = cursor.fetchall()
        cursor.execute("SELECT sum(hopper_level) AS min_hopper FROM vicidial_campaigns")
        hopper_min_max_result = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) AS leads_in_hopper FROM vicidial_hopper WHERE status = 'READY'")
        leads_in_hopper_result = cursor.fetchall()
        cursor.execute("""
            SELECT GREATEST(COUNT(vh.lead_id) - (vc.auto_dial_level * COUNT(DISTINCT vla.user)),0) AS trunk_fill,
                   GREATEST((vc.auto_dial_level * COUNT(DISTINCT vla.user)) - COUNT(vh.lead_id),0) AS trunk_short
            FROM vicidial_campaigns vc
            LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
            LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id WHERE vc.active = 'Y'
        """)
        trunk_short_fill_result = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) AS calls_today FROM vicidial_log WHERE date(call_date) = %s", (today_start,))
        calls_today_result = cursor.fetchall()
        cursor.execute("""
            SELECT ROUND((SUM(IF(status IN ('DROP','DC'),1,0)) / COUNT(*)) * 100, 2) AS dropped_percent
            FROM vicidial_log WHERE date(call_date) = %s
        """, (today_start,))
        drop_percent_result = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) AS agents FROM vicidial_live_agents")
        avg_agent_result = cursor.fetchall()
        cursor.execute("""
            SELECT ROUND(vc.auto_dial_level - (COUNT(vh.lead_id) / COUNT(vla.user)),2) AS dl_diff
            FROM vicidial_campaigns vc
            LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id
            LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
        """)
        dl_diff_result = cursor.fetchall()
        cursor.execute("""
            SELECT ROUND(((vc.auto_dial_level - (COUNT(vh.lead_id) / NULLIF(COUNT(vla.user), 0))) / vc.auto_dial_level) * 100, 2) AS diff_percent
            FROM vicidial_campaigns vc
            LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id
            LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
        """)
        diff_result = cursor.fetchall()
        cursor.execute("SELECT dial_method FROM vicidial_campaigns ORDER BY dial_method ASC LIMIT 1")
        dial_method_result = cursor.fetchall()
        cursor.execute("SELECT status FROM vicidial_campaign_statuses")
        status_result = cursor.fetchall()
        cursor.execute("SELECT DISTINCT lead_order FROM vicidial_campaigns")
        order_result = cursor.fetchall()
        cursor.close()
        conn.close()

        return {
            "dialer_level":     dialer_level_result[0]["dialer_level"] if dialer_level_result else None,
            "dialable_leads":   dialable_leads_result[0]["dialable_leads"] if dialable_leads_result else 0,
            "hopper_min_max":   hopper_min_max_result[0]["min_hopper"] if hopper_min_max_result else None,
            "trunk_short_fill": trunk_short_fill_result[0] if trunk_short_fill_result else {"trunk_fill": 0, "trunk_short": 0},
            "calls_today":      calls_today_result[0]["calls_today"] if calls_today_result else 0,
            "avg_agent":        avg_agent_result[0]["agents"] if avg_agent_result else 0,
            "dl_diff":          dl_diff_result[0]["dl_diff"] if dl_diff_result else 0,
            "diff_percent":     diff_result[0]["diff_percent"] if diff_result else 0,
            "dial_method":      dial_method_result[0]["dial_method"] if dial_method_result else None,
            "order":            order_result[0]["lead_order"] if order_result else None
        }
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/getcallbystatus')
def get_calls_by_status(current_user: str = Depends(get_current_user)):
    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN status='INCALL' THEN 1 END) AS Incall,
                COUNT(CASE WHEN status='PAUSED' THEN 1 END) AS Paused,
                COUNT(CASE WHEN status='READY'  THEN 1 END) AS Ready,
                (SELECT COUNT(*) FROM vicidial_log WHERE DATE(call_date) = %s) AS Totalcall
            FROM vicidial_live_agents
        """, (date.today(),))
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
        user_id     = current_user["username"]
        is_admin    = current_user["isAdmin"]
        campaign_id = current_user["campaign_id"]
        userfitler  = f" and v.user='{user_id}' and v.campaign_id='{campaign_id}' " if not is_admin else ""

        query = f"""
            SELECT total_dials, connected_calls, connection_rate_pct, total_talk_time,
                   avg_talk_time_sec, leads_connected, sum(total_seconds) total_seconds
            FROM (
                SELECT
                    DATE(v.call_date) AS call_date,
                    (SELECT count(*) FROM vicidial_log v WHERE date(call_date) BETWEEN %s AND %s {userfitler}) AS total_dials,
                    (SELECT count(*) FROM vicidial_log v WHERE date(call_date) BETWEEN %s AND %s AND length_in_sec > 0 {userfitler}) AS connected_calls,
                    ROUND((SUM(v.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_pct,
                    (SELECT SUM(length_in_sec) FROM vicidial_log v WHERE DATE(call_date) BETWEEN %s AND %s {userfitler}) AS total_talk_time,
                    (SELECT AVG(length_in_sec) FROM vicidial_log v WHERE date(call_date) BETWEEN %s AND %s AND length_in_sec > 0 {userfitler}) AS avg_talk_time_sec,
                    COUNT(distinct v.lead_id) AS leads_connected,
                    SUM(length_in_sec) AS total_seconds
                FROM vicidial_log v
                WHERE date(v.call_date) BETWEEN %s AND %s {userfitler}
                GROUP BY DATE(v.call_date)
            ) a
        """
        cursor.execute(query, (sd, ed, sd, ed, sd, ed, sd, ed, sd, ed))
        result = cursor.fetchall()
        for row in result:
            row["total_talk_time"] = seconds_to_hhmmss(row["total_talk_time"])
        cursor.close()
        conn.close()
        return {"data": result}
    except Error as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/dialerperformance')
def get_dialerperformance(current_user: str = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        today_start = date.today()
        cursor.execute("""
            SELECT
                ROUND(COUNT(*) / NULLIF((SELECT COUNT(DISTINCT user) FROM vicidial_live_agents), 0), 2) AS dial_level,
                ROUND((COUNT(*) / NULLIF((SELECT COUNT(DISTINCT user) FROM vicidial_live_agents), 0)) / 24, 2) AS calls_per_agent_per_hour,
                (SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) FROM call_log WHERE DATE(start_time) = %s) AS avg_answer_speed_sec,
                ROUND(SUM(vl.status IN ('DROP','ABANDON')) * 100.0 / NULLIF((SELECT COUNT(*) FROM vicidial_dial_log WHERE DATE(call_date) = %s), 0), 2) AS drop_rate_percent,
                ROUND(AVG(vl.length_in_sec), 2) AS avg_call_length,
                ROUND((SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_percent
            FROM vicidial_log vl WHERE DATE(vl.call_date) = %s
        """, (today_start, today_start, today_start))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"data": result}
    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/agentsproductivity')
def get_agentsproductivity(request: Request, current_user: str = Depends(get_current_user)):
    try:
        sd = request.query_params.get("sd")
        ed = request.query_params.get("ed")

        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT CONCAT('SIP/',vl.user) AS STATION, vl.user AS USER_ID,
                   vu.full_name AS USER_NAME, vla.status AS STATUS,
                   COUNT(*) AS CALLS, SUM(vl.length_in_sec > 0) AS connected_calls,
                   MAX(vl.phone_number) AS phone_number,
                   SEC_TO_TIME(IFNULL(al.login_seconds,0)) AS login_duration,
                   (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(vla.last_state_change)) AS TALK_TIME_SECONDS
            FROM vicidial_log vl
            LEFT JOIN vicidial_users vu ON vl.user = vu.user
            LEFT JOIN vicidial_live_agents vla ON vl.user = vla.user
            LEFT JOIN (
                SELECT user, SUM(pause_sec + wait_sec + talk_sec + dispo_sec) AS login_seconds
                FROM vicidial_agent_log WHERE DATE(event_time) BETWEEN %s AND %s GROUP BY user
            ) al ON vl.user = al.user
            WHERE DATE(vl.call_date) BETWEEN %s AND %s
            GROUP BY vl.user, vl.campaign_id ORDER BY vl.user
        """, (sd, ed, sd, ed))
        result = cursor.fetchall()
        for row in result:
            row["TALK_TIME_HH_MM_SS"] = format_time(row.get("TALK_TIME_SECONDS", 0))
            if hasattr(row["login_duration"], "total_seconds"):
                row["login_duration"] = seconds_to_hhmmss(row["login_duration"].total_seconds())
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
                WHERE status = 'success'
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
            row["avg_rating"]       = ratings.get("avg_rating",      None)
            row["avg_stars"]        = ratings.get("avg_stars",        None)
            row["total_analyzed"]   = ratings.get("total_analyzed",   0)
            row["successful_sales"] = ratings.get("successful_sales", 0)
            row["not_converted"]    = ratings.get("not_converted",    0)
            row["leads_generated"]  = ratings.get("leads_generated",  0)

        return {"data": result}

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/campaignperformance')
def get_campaignperformance(request: Request, current_user: str = Depends(get_current_user)):
    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        sd     = request.query_params.get("sd")
        ed     = request.query_params.get("ed")
        cursor.execute("""
            SELECT vl.campaign_id, COUNT(*) AS total_dials, SUM(vl.length_in_sec > 0) AS connected_calls,
                   ROUND((SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_pct,
                   SEC_TO_TIME(SUM(vl.length_in_sec)) AS total_talk_time,
                   AVG(vl.length_in_sec) AS avg_talk_time,
                   ROUND((SUM(vl.status = 'DROP') / COUNT(*)) * 100, 2) AS drop_rate_pct,
                   SUM(vl.status IN ('SALE','SUCCESS','CONVERTED')) AS conversions
            FROM vicidial_log vl WHERE DATE(vl.call_date) BETWEEN %s AND %s
            GROUP BY vl.campaign_id ORDER BY total_dials DESC
        """, (sd, ed))
        result = cursor.fetchall()
        for row in result:
            if hasattr(row["total_talk_time"], "total_seconds"):
                row["total_talk_time"] = seconds_to_hhmmss(row["total_talk_time"].total_seconds())
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
        sd     = request.query_params.get("sd")
        ed     = request.query_params.get("ed")
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        if sd and ed:
            cursor.execute("""
                SELECT COUNT(*) AS dialed, SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                       SUM(length_in_sec > 0) AS connected, SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                       SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log WHERE date(call_date) BETWEEN %s AND %s
            """, (sd, ed))
        else:
            cursor.execute("""
                SELECT COUNT(*) AS dialed, SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                       SUM(length_in_sec > 0) AS connected, SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                       SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log WHERE date(call_date) = %s
            """, (date.today(),))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/hourlyperformance')
def get_hourlyperformance(current_user: str = Depends(get_current_user)):
    try:
        conn        = mysql.connector.connect(**DB_CONFIG)
        cursor      = conn.cursor(dictionary=True)
        today_start = date.today()
        cursor.execute("""
            SELECT HOUR(call_date) AS hour, COUNT(*) AS total_calls, SUM(length_in_sec > 0) AS connected_calls
            FROM vicidial_log WHERE DATE(call_date) = %s
            GROUP BY HOUR(call_date) ORDER BY hour
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


@app.get('/leadfunnelwithdate')
def get_LeadFunnelWithDate(request: Request, current_user: str = Depends(get_current_user)):
    try:
        sd     = request.query_params.get("sd")
        ed     = request.query_params.get("ed")
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        if sd and ed:
            cursor.execute("""
                SELECT COUNT(*) AS dialed, SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                       SUM(length_in_sec > 0) AS connected, SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                       SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log WHERE date(call_date) BETWEEN %s AND %s
            """, (sd, ed))
        else:
            cursor.execute("""
                SELECT COUNT(*) AS dialed, SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                       SUM(length_in_sec > 0) AS connected, SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                       SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log WHERE date(call_date) = %s
            """, (date.today(),))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
                SELECT DATE(vl.entry_date) AS entry_date, vl.lead_id, vl.phone_number,
                       vl.first_name, vl.last_name, vl.status, vl.list_id, vls.campaign_id, vl.user
                FROM vicidial_list vl JOIN vicidial_lists vls ON vl.list_id = vls.list_id
                WHERE date(vl.entry_date) BETWEEN %s AND %s
                ORDER BY vl.entry_date DESC LIMIT %s
            """, (sd, ed, limit))
        else:
            cursor.execute("""
                SELECT DATE(vl.entry_date) AS entry_date, vl.lead_id, vl.phone_number,
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

    for index, row in df.iterrows():
        excel_row = index + 2
        phone     = clean_phone(row.get("phone_number"))
        list_id   = str(row.get("list_id")).strip()

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
            }, timeout=10)
            if "SUCCESS" in response.text.upper():
                success += 1
                existing_phones.add(phone)
            else:
                failed.append({"row": excel_row, "phone": phone, "error": response.text})
        except Exception as e:
            failed.append({"row": excel_row, "phone": phone, "error": str(e)})

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
              AND vl.status IN ('NEW') AND vls.campaign_id = %s
            ORDER BY vl.lead_id
        """, (campaign_id,))

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

    # skip sync+analyze for no-answer dispositions
    if status.upper() in NO_ANSWER_STATUSES:
        print(f"[Submit] Agent '{agent_user}' disposed '{status}' (no-answer) — skipping analysis")
        return {
            "success":            True,
            "agent":              agent_user,
            "vicidial_responses": responses,
            "analysis":           f"skipped — '{status}' is a no-answer disposition"
        }

    print(f"[Submit] Agent '{agent_user}' disposed '{status}' — triggering sync+analyze")
    threading.Thread(
        target=trigger_sync_and_analyze_for_agent,
        args=(agent_user, status),
        daemon=True
    ).start()

    return {
        "success":            True,
        "agent":              agent_user,
        "vicidial_responses": responses,
        "analysis":           "triggered"
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

        with _poller_lock:
            prev_status = _agent_last_status.get(user_id)
            if call_status == "DISPOSITION_PENDING" and prev_status != "DISPOSITION_PENDING":
                print(f"[Status API] {user_id}: {prev_status} → DISPOSITION_PENDING ← TRIGGER!")
                _agent_last_status[user_id] = call_status
                threading.Thread(
                    target=trigger_sync_and_analyze_for_agent,
                    args=(user_id, ""),
                    daemon=True
                ).start()
            else:
                _agent_last_status[user_id] = call_status

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
def get_active_campaigns():
    conn   = None
    cursor = None
    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT campaign_id, campaign_name, active FROM vicidial_campaigns WHERE active = 'Y'")
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
#  CALL QUALITY ANALYZER ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {
        "status":             "ok",
        "version":            "7.0.0",
        "table":              "call_analysis",
        "model":              "gpt-4o",
        "today":              today_str(),
        "auto_loop_interval": f"{AUTO_SYNC_INTERVAL_SECS // 60} minutes",
        "supported_audio":    list(SUPPORTED_AUDIO_EXTENSIONS),
        "no_answer_statuses": list(NO_ANSWER_STATUSES),
    }


@app.get("/poller-status")
def poller_status():
    with _poller_lock:
        snapshot = dict(_agent_last_status)
    return {
        "poll_interval_seconds":  POLL_INTERVAL_SECONDS,
        "auto_loop_interval_min": AUTO_SYNC_INTERVAL_SECS // 60,
        "agents_tracked":         len(snapshot),
        "agent_statuses":         snapshot,
    }


@app.post("/trigger-agent/{agent_user}")
def trigger_agent(agent_user: str):
    threading.Thread(target=trigger_sync_and_analyze_for_agent, args=(agent_user, ""), daemon=True).start()
    return {"message": f"Triggered for agent '{agent_user}'", "date": today_str()}


@app.get("/debug-sync")
def debug_sync(date: str = None):
    target_date = date or today_str()
    result      = {"date": target_date}
    try:
        mc = get_mysql_conn()
        with mc.cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM recording_log WHERE DATE(start_time) = %s", (target_date,))
            result["mysql_total"] = cur.fetchone().get("total", 0)
            cur.execute("""
                SELECT recording_id, filename, start_time, location
                FROM recording_log WHERE DATE(start_time) = %s
                ORDER BY start_time DESC LIMIT 3
            """, (target_date,))
            result["mysql_sample"] = [
                {"recording_id": str(r.get("recording_id")), "filename": r.get("filename"),
                 "start_time": str(r.get("start_time")), "location": r.get("location")}
                for r in cur.fetchall()
            ]
        mc.close()
        result["mysql_status"] = "ok"
    except Exception as e:
        result["mysql_status"] = f"ERROR: {str(e)}"
    try:
        pg  = get_pg_conn()
        cur = pg.cursor()
        cur.execute("SELECT COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s", (target_date,))
        result["pg_total_for_date"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM call_analysis")
        result["pg_total_all"] = cur.fetchone()[0]
        cur.execute("SELECT status, COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s GROUP BY status", (target_date,))
        result["pg_status_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        pg.close()
        result["pg_status"] = "ok"
    except Exception as e:
        result["pg_status"] = f"ERROR: {str(e)}"
    return result


@app.post("/sync-recordings")
def sync_recordings(date: str = None):
    try:
        result = sync_recording_log(target_date=date)
        return {"message": "Sync complete", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze-all")
def analyze_all(date: str = None):
    target_date = date or today_str()
    rows        = get_unanalyzed(target_date=target_date)
    if not rows:
        return {
            "message":   f"No pending recordings for {target_date}. Run /sync-recordings first.",
            "date":      target_date,
            "processed": 0
        }

    results = []
    errors  = []
    for row in rows:
        try:
            results.append(process_recording(row))
        except Exception as e:
            mark_status(row["id"], "failed")
            errors.append({"filename": row.get("filename"), "error": str(e)})

    return {
        "date":      target_date,
        "processed": len(results),
        "failed":    len(errors),
        "results":   results,
        "errors":    errors
    }


@app.post("/analyze-one/{filename}")
def analyze_one(filename: str):
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, recording_id, filename, location, lead_id, agent_user, length_in_sec, status
            FROM call_analysis WHERE filename = %s LIMIT 1
        """, (filename,))
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"'{filename}' not found. Run /sync-recordings first.")
    row = dict(row)
    if row.get("status") == "success":
        raise HTTPException(status_code=409, detail=f"'{filename}' already analyzed.")
    try:
        return JSONResponse(content=process_recording(row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/results")
def get_results(limit: int = 50, offset: int = 0, date: str = None):
    target_date = date or today_str()
    conn        = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, recording_id, channel, server_ip, extension, start_time, start_epoch,
                   end_time, end_epoch, length_in_sec, length_in_min, filename, location,
                   phone_number, lead_id, agent_user, vicidial_id, synced_at,
                   overall_rating, stars, call_outcome, summary, agent_sentiment, client_sentiment,
                   greeting_score, product_knowledge_score, convincing_score, objection_score,
                   clarity_score, empathy_score, closing_score, strengths, improvements, analyzed_at, status
            FROM call_analysis WHERE DATE(start_time) = %s
            ORDER BY start_time DESC LIMIT %s OFFSET %s
        """, (target_date, limit, offset))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        for row in rows:
            for f in ["start_time", "end_time", "synced_at", "analyzed_at"]:
                if row.get(f): row[f] = str(row[f])
        return {"date": target_date, "total": len(rows), "data": rows}
    finally:
        conn.close()


@app.get("/results/{result_id}")
def get_result(result_id: int):
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM call_analysis WHERE id = %s", (result_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            raise HTTPException(status_code=404, detail=f"ID {result_id} not found.")
        row = dict(row)
        for f in ["start_time", "end_time", "synced_at", "analyzed_at"]:
            if row.get(f): row[f] = str(row[f])
        for f in ["strengths", "improvements"]:
            if row.get(f):
                try:    row[f] = json.loads(row[f])
                except: pass
        return row
    finally:
        conn.close()