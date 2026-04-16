import os
import re
import json
import math
import time
import tempfile
import threading
import requests
import pymysql
import pymysql.cursors
import psycopg2
import psycopg2.extras
import pandas as pd
from pathlib import Path
from datetime import date, datetime

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from openai import OpenAI
from urllib.parse import urlparse, urlunparse

# ── Optional pydub (needs ffmpeg installed + audioop-lts on Python 3.13) ──
try:
    from pydub import AudioSegment
    from pydub.effects import normalize
    PYDUB_AVAILABLE = True
    print("[Startup] pydub loaded — ringing detection & volume boost ENABLED.")
except ImportError:
    PYDUB_AVAILABLE = False
    print("[Warning] pydub not available — volume boost & ringing detection DISABLED.")

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ── Call Analyzer DB (MySQL + PostgreSQL) ──
MYSQL_DB = {
    "host":     "192.168.15.165",
    "port":     3306,
    "user":     "cron",
    "password": "1234",
    "database": "asterisk",
}

POSTGRES_DB = {
    "host":     "192.168.15.105",
    "port":     5432,
    "user":     "postgres",
    "password": "Soft!@7890",
    "database": "customDialer",
}

# ── Lead Uploader (Vicidial API + Vicidial MySQL) ──
VICIDIAL_DB_CONFIG = {
    "host":     "192.168.15.165",   # same MySQL — update if different
    "user":     "cron",
    "password": "1234",
    "database": "asterisk",
    "port":     3306,
}

VICIDIAL_API_USER = "AdminR"
VICIDIAL_API_PASS = "AdminR"
VICIDIAL_API_URL  = "http://192.168.15.165:5165/vicidial/non_agent_api.php"
VICIDIAL_LIST_ID  = 7022026

# Google Sheets to upload leads from
LEAD_SHEETS = [
    "https://docs.google.com/spreadsheets/d/1zoRdvvdkgwdhqubhna_FsS1N8zbcK7UE9uNKBFqTeTM/edit?gid=0",
    "https://docs.google.com/spreadsheets/d/1kaWWrEYy_00YJrlKH5H04iC0g-_to9CCdTKmnHPYDGY/edit?gid=0",
]

# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════
AUTO_LOOP_INTERVAL_SECONDS        = 5 * 60   # call analyzer loop: every 5 min
LEAD_UPLOAD_INTERVAL_SECONDS      = 10 * 60  # lead uploader loop: every 10 min

LOW_VOLUME_THRESHOLD_DBFS = -30.0
BOOST_TARGET_DBFS         = -18.0

# ═══════════════════════════════════════════════════════════════
#  LOOP STATE — Call Analyzer
# ═══════════════════════════════════════════════════════════════
_loop_state = {
    "running":        False,
    "iteration":      0,
    "last_run_at":    None,
    "next_run_at":    None,
    "last_sync":      {},
    "last_analyze":   {},
    "total_inserted": 0,
    "total_analyzed": 0,
    "total_failed":   0,
    "errors":         [],
}
_loop_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════
#  LOOP STATE — Lead Uploader
# ═══════════════════════════════════════════════════════════════
_lead_loop_state = {
    "running":        False,
    "iteration":      0,
    "last_run_at":    None,
    "next_run_at":    None,
    "total_success":  0,
    "total_failed":   0,
    "total_skipped":  0,
    "last_cycle":     {},
    "errors":         [],
}
_lead_loop_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════════
app = FastAPI(
    title="VICIdial All-in-One — Call Analyzer + Lead Uploader",
    description="Auto sync + analyze calls every 5 min | Auto upload leads every 10 min",
    version="6.0.0",
)

# ═══════════════════════════════════════════════════════════════
#  SYSTEM PROMPT  (Call Analyzer)
# ═══════════════════════════════════════════════════════════════
SYSTEM_PROMPT = """You are an expert call center quality analyst with 15+ years of experience.
Evaluate the agent's performance and respond ONLY with valid JSON — no markdown, no extra text.

─────────────────────────────────────────
STEP 1 — DETERMINE CALL STATUS
─────────────────────────────────────────
First, classify what actually happened on this call:

  "Connected"       → A real live human answered and a two-way conversation took place
  "Not Connected"   → Phone rang but nobody answered / pure silence
  "Voicemail"       → Went to voicemail / answering machine (no live person)
  "Busy"            → Line was busy / engaged tone
  "Wrong Number"    → Person confirmed they are not the intended contact
  "Dropped"         → Call connected but was cut off before any real exchange
  "Callback"        → Person answered but asked to be called back later (no real conversation)
  "IVR / Auto"      → Reached an automated system / IVR menu only

─────────────────────────────────────────
STEP 2 — CHOOSE THE RIGHT JSON FORMAT
─────────────────────────────────────────

If status is ANYTHING other than "Connected", return this format:
{
  "overall_rating": 0,
  "stars": 0,
  "summary": "<One sentence describing what happened, e.g. 'Line was busy, no connection made.'>",
  "call_status": "<Not Connected | Voicemail | Busy | Wrong Number | Dropped | Callback | IVR / Auto>",
  "call_outcome": "N/A",
  "agent_sentiment": "N/A",
  "client_sentiment": "N/A",
  "categories": {
    "greeting_professionalism":  { "score": 0, "comment": "Not applicable." },
    "product_knowledge":         { "score": 0, "comment": "Not applicable." },
    "convincing_ability":        { "score": 0, "comment": "Not applicable." },
    "objection_handling":        { "score": 0, "comment": "Not applicable." },
    "communication_clarity":     { "score": 0, "comment": "Not applicable." },
    "empathy_patience":          { "score": 0, "comment": "Not applicable." },
    "closing_technique":         { "score": 0, "comment": "Not applicable." }
  },
  "strengths": [],
  "improvements": []
}

If status is "Connected" (real two-way human conversation), return this format:
{
  "overall_rating": <float 1.0–5.0>,
  "stars": <integer 1–5>,
  "summary": "<2–3 sentence overall summary>",
  "call_status": "Connected",
  "call_outcome": "<Successful Sale | Lead Generated | Not Converted | Support Resolved | Escalated | Callback Scheduled>",
  "agent_sentiment": "<Positive | Neutral | Negative>",
  "client_sentiment": "<Positive | Neutral | Negative | Frustrated | Interested>",
  "categories": {
    "greeting_professionalism":  { "score": <1–5>, "comment": "<feedback>" },
    "product_knowledge":         { "score": <1–5>, "comment": "<feedback>" },
    "convincing_ability":        { "score": <1–5>, "comment": "<feedback>" },
    "objection_handling":        { "score": <1–5>, "comment": "<feedback>" },
    "communication_clarity":     { "score": <1–5>, "comment": "<feedback>" },
    "empathy_patience":          { "score": <1–5>, "comment": "<feedback>" },
    "closing_technique":         { "score": <1–5>, "comment": "<feedback>" }
  },
  "strengths":    ["<strength 1>", "<strength 2>", "<strength 3>"],
  "improvements": ["<area 1>", "<area 2>", "<area 3>"]
}

Rating scale: 1=Very Poor  2=Below Average  3=Average  4=Good  5=Excellent
"""


# ═══════════════════════════════════════════════════════════════
#  BASIC HELPERS
# ═══════════════════════════════════════════════════════════════

def today_str() -> str:
    return date.today().isoformat()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def extract_phone_number(location: str):
    if not location:
        return None
    match = re.search(r'\d{8}-\d{6}_(\d+)_', location)
    return match.group(1) if match else None


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_DB["host"], port=MYSQL_DB["port"],
        user=MYSQL_DB["user"], password=MYSQL_DB["password"],
        database=MYSQL_DB["database"],
        cursorclass=pymysql.cursors.DictCursor, charset="utf8mb4",
    )


def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_DB["host"], port=POSTGRES_DB["port"],
        user=POSTGRES_DB["user"], password=POSTGRES_DB["password"],
        dbname=POSTGRES_DB["database"],
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


def mark_status(row_id: int, status: str):
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE call_analysis SET status = %s, analyzed_at = CURRENT_TIMESTAMP WHERE id = %s",
            (status, row_id),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[DB] Warning: could not mark status={status} for id={row_id}: {e}")
    finally:
        conn.close()


def set_not_picked(row_id: int, summary: str = None, avg_dbfs: float = None, volume_boosted: bool = False):
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE call_analysis
            SET status         = 'not_picked',
                summary        = COALESCE(%s, summary),
                avg_dbfs       = COALESCE(%s, avg_dbfs),
                volume_boosted = %s,
                analyzed_at    = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (
            summary,
            round(avg_dbfs, 2) if avg_dbfs is not None and not math.isinf(avg_dbfs) else None,
            volume_boosted,
            row_id,
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"[DB] Warning: set_not_picked failed for id={row_id}: {e}")
    finally:
        conn.close()


def _log_error(msg: str):
    with _loop_lock:
        _loop_state["errors"].append(f"{now_str()} — {msg}")
        if len(_loop_state["errors"]) > 20:
            _loop_state["errors"].pop(0)


def _log_lead_error(msg: str):
    with _lead_loop_lock:
        _lead_loop_state["errors"].append(f"{now_str()} — {msg}")
        if len(_lead_loop_state["errors"]) > 20:
            _lead_loop_state["errors"].pop(0)


# ═══════════════════════════════════════════════════════════════
#  AUDIO HELPERS  (pydub — optional)
# ═══════════════════════════════════════════════════════════════

def _detect_ringing_pattern(chunk_levels: list) -> bool:
    if len(chunk_levels) < 6:
        return False
    THRESHOLD    = -40.0
    active       = [1 if (not math.isinf(lvl) and lvl > THRESHOLD) else 0 for lvl in chunk_levels]
    transitions  = sum(1 for i in range(1, len(active)) if active[i] != active[i - 1])
    active_ratio = sum(active) / len(active)
    if transitions < 4 or not (0.20 <= active_ratio <= 0.70):
        return False
    run_lengths, current_val, current_len = [], active[0], 1
    for val in active[1:]:
        if val == current_val:
            current_len += 1
        else:
            run_lengths.append(current_len)
            current_val, current_len = val, 1
    run_lengths.append(current_len)
    if len(run_lengths) < 4:
        return False
    avg_run  = sum(run_lengths) / len(run_lengths)
    variance = sum((r - avg_run) ** 2 for r in run_lengths) / len(run_lengths)
    return variance < avg_run * 2.0


def analyze_audio(file_path: str) -> dict:
    default = {"avg_dbfs": -999, "is_ringing": False, "is_low_volume": False, "duration_ms": 0}
    if not PYDUB_AVAILABLE:
        return default
    try:
        audio        = AudioSegment.from_file(file_path)
        avg_dbfs     = audio.dBFS
        chunk_ms     = 500
        chunks       = [audio[i: i + chunk_ms] for i in range(0, len(audio), chunk_ms)]
        chunk_levels = [c.dBFS for c in chunks if len(c) > 0]
        return {
            "avg_dbfs":      avg_dbfs,
            "is_ringing":    _detect_ringing_pattern(chunk_levels),
            "is_low_volume": (not math.isinf(avg_dbfs) and avg_dbfs < LOW_VOLUME_THRESHOLD_DBFS),
            "duration_ms":   len(audio),
        }
    except Exception as e:
        print(f"[Audio] analyze_audio error ({file_path}): {e}")
        return default


def boost_audio(file_path: str) -> str:
    audio = AudioSegment.from_file(file_path)
    if math.isinf(audio.dBFS):
        print("[Audio] Boost skipped — completely silent file.")
        return file_path
    audio_norm  = normalize(audio)
    gain_needed = BOOST_TARGET_DBFS - audio_norm.dBFS
    boosted     = audio_norm + gain_needed
    print(f"[Audio] Boosted: {audio.dBFS:.1f} dBFS → {boosted.dBFS:.1f} dBFS (gain {gain_needed:+.1f} dB)")
    suffix = Path(file_path).suffix or ".mp3"
    tmp    = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    boosted.export(tmp.name, format=suffix.lstrip(".") or "mp3")
    tmp.close()
    return tmp.name


# ═══════════════════════════════════════════════════════════════
#  CREATE / MIGRATE TABLE  (Call Analyzer)
# ═══════════════════════════════════════════════════════════════

def create_table():
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
                avg_dbfs                    FLOAT,
                volume_boosted              BOOLEAN DEFAULT FALSE,
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
        for col, definition in [
            ("avg_dbfs",       "FLOAT"),
            ("volume_boosted", "BOOLEAN DEFAULT FALSE"),
        ]:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'call_analysis' AND column_name = %s
            """, (col,))
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE call_analysis ADD COLUMN {col} {definition}")
                print(f"[DB] Migrated: added column {col}")
        conn.commit()
        cur.close()
        print("[DB] Table 'call_analysis' ready (v6.0).")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  PHONE CLEANING  (Lead Uploader — all countries)
# ═══════════════════════════════════════════════════════════════

def clean_phone(phone):
    """
    Handles phone numbers from ANY country/region.
    Strips p:/ph: prefix, detects and removes country code,
    returns the local subscriber number.
    """
    if phone is None:
        return None

    phone = str(phone).strip().lower()

    if phone in ["", "nan", "none", "phone number", "phone_number", "phone", "phonenumber"]:
        return None

    # Remove 'p:' / 'ph:' prefix
    phone = re.sub(r'^(ph?)\s*:\s*', '', phone.strip())

    # Remove leading + or 00
    phone = re.sub(r'^\+', '', phone)
    phone = re.sub(r'^00', '', phone)

    digits = re.findall(r'\d+', phone)
    if not digits:
        return None

    phone = "".join(digits)

    COUNTRY_CODES = [
        # 4-digit NANP codes first
        ("1242", 7), ("1246", 7), ("1264", 7), ("1268", 7), ("1284", 7),
        ("1340", 7), ("1345", 7), ("1441", 7), ("1473", 7), ("1649", 7),
        ("1664", 7), ("1671", 7), ("1684", 7), ("1721", 7), ("1758", 7),
        ("1767", 7), ("1784", 7), ("1787", 7), ("1809", 7), ("1868", 7),
        ("1869", 7), ("1876", 7), ("1939", 7),
        # 3-digit codes
        ("210",-1),("211",-1),("212",-1),("213",-1),("216",-1),("218",-1),
        ("220",-1),("221",-1),("222",-1),("223",-1),("224",-1),("225",-1),
        ("226",-1),("227",-1),("228",-1),("229",-1),("230",-1),("231",-1),
        ("232",-1),("233",-1),("234",-1),("235",-1),("236",-1),("237",-1),
        ("238",-1),("239",-1),("240",-1),("241",-1),("242",-1),("243",-1),
        ("244",-1),("245",-1),("246",-1),("247",-1),("248",-1),("249",-1),
        ("250",-1),("251",-1),("252",-1),("253",-1),("254",-1),("255",-1),
        ("256",-1),("257",-1),("258",-1),("260",-1),("261",-1),("262",-1),
        ("263",-1),("264",-1),("265",-1),("266",-1),("267",-1),("268",-1),
        ("269",-1),("290",-1),("291",-1),("297",-1),("298",-1),("299",-1),
        ("350",-1),("351",-1),("352",-1),("353",-1),("354",-1),("355",-1),
        ("356",-1),("357",-1),("358",-1),("359",-1),("370",-1),("371",-1),
        ("372",-1),("373",-1),("374",-1),("375",-1),("376",-1),("377",-1),
        ("378",-1),("380",-1),("381",-1),("382",-1),("383",-1),("385",-1),
        ("386",-1),("387",-1),("389",-1),("420",-1),("421",-1),("423",-1),
        ("500",-1),("501",-1),("502",-1),("503",-1),("504",-1),("505",-1),
        ("506",-1),("507",-1),("508",-1),("509",-1),("590",-1),("591",-1),
        ("592",-1),("593",-1),("594",-1),("595",-1),("596",-1),("597",-1),
        ("598",-1),("599",-1),("670",-1),("672",-1),("673",-1),("674",-1),
        ("675",-1),("676",-1),("677",-1),("678",-1),("679",-1),("680",-1),
        ("681",-1),("682",-1),("683",-1),("685",-1),("686",-1),("687",-1),
        ("688",-1),("689",-1),("690",-1),("691",-1),("692",-1),("850",-1),
        ("852",-1),("853",-1),("855",-1),("856",-1),("880",-1),("886",-1),
        ("960",-1),("961",-1),("962",-1),("963",-1),("964",-1),("965",-1),
        ("966",-1),("967",-1),("968",-1),("970",-1),("971",-1),("972",-1),
        ("973",-1),("974",-1),("975",-1),("976",-1),("977",-1),("992",-1),
        ("993",-1),("994",-1),("995",-1),("996",-1),("998",-1),
        # 2-digit codes
        ("20",-1),("27",-1),("30",-1),("31",-1),("32",-1),("33",-1),
        ("34",-1),("36",-1),("39",-1),("40",-1),("41",-1),("43",-1),
        ("44",-1),("45",-1),("46",-1),("47",-1),("48",-1),("49",-1),
        ("51",-1),("52",-1),("53",-1),("54",-1),("55",-1),("56",-1),
        ("57",-1),("58",-1),("60",-1),("61",-1),("62",-1),("63",-1),
        ("64",-1),("65",-1),("66",-1),("81",-1),("82",-1),("84",-1),
        ("86",-1),("90",-1),("91",10),("92",10),("93",-1),("94",-1),
        ("95",-1),("98",-1),
        # 1-digit codes last
        ("1", 10),
        ("7", -1),
    ]

    for cc, local_len in COUNTRY_CODES:
        if phone.startswith(cc):
            local = phone[len(cc):]
            if local_len == -1:
                if 6 <= len(local) <= 12:
                    return cc + local
            else:
                if len(local) == local_len:
                    return cc + local
            break  # CC matched but wrong length — stop

    # No CC matched — accept as-is if 6–12 digits
    if 6 <= len(phone) <= 12:
        return phone

    return None


def is_header_value(value) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in [
        "phone number", "phone_number", "phone", "phonenumber",
        "name", "first name", "firstname", "lead_status", "lead status",
    ]


def convert_to_csv_url(sheet_url: str) -> str:
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not match:
        raise ValueError(f"Invalid Google Sheet URL: {sheet_url}")
    sheet_id  = match.group(1)
    gid_match = re.search(r'gid=(\d+)', sheet_url)
    gid       = gid_match.group(1) if gid_match else "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def load_existing_vicidial_numbers() -> set:
    conn   = pymysql.connect(**VICIDIAL_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT phone_number FROM vicidial_list WHERE list_id = %s",
        (VICIDIAL_LIST_ID,),
    )
    numbers = set()
    for (num,) in cursor.fetchall():
        if num:
            num = re.sub(r"\D", "", str(num))
            if len(num) >= 6:
                numbers.add(num)
    cursor.close()
    conn.close()
    print(f"[LeadUpload] 📊 Loaded {len(numbers)} existing numbers from Vicidial DB")
    return numbers


def send_to_vicidial(phone: str, first_name: str) -> str:
    params = {
        "source":       "python_script",
        "user":         VICIDIAL_API_USER,
        "pass":         VICIDIAL_API_PASS,
        "function":     "add_lead",
        "phone_number": phone,
        "phone_code":   1,
        "list_id":      VICIDIAL_LIST_ID,
        "first_name":   first_name or "",
    }
    try:
        res = requests.get(VICIDIAL_API_URL, params=params, timeout=10)
        return res.text
    except Exception as e:
        return str(e)


# ═══════════════════════════════════════════════════════════════
#  LEAD UPLOAD — one full cycle across all sheets
# ═══════════════════════════════════════════════════════════════

def run_lead_upload_cycle() -> dict:
    """
    Loads existing numbers from Vicidial DB once,
    then iterates all LEAD_SHEETS uploading new numbers.
    Returns cycle summary dict.
    """
    success = 0
    failed  = 0
    skipped = 0
    errors  = []

    try:
        existing_numbers = load_existing_vicidial_numbers()
    except Exception as e:
        msg = f"Failed to load existing numbers: {e}"
        print(f"[LeadUpload] ❌ {msg}")
        return {"success": 0, "failed": 0, "skipped": 0, "errors": [msg]}

    for sheet_url in LEAD_SHEETS:
        print(f"\n[LeadUpload] 📄 Processing: {sheet_url}")
        try:
            csv_url = convert_to_csv_url(sheet_url)
            df      = pd.read_csv(csv_url, dtype=str)
        except Exception as e:
            msg = f"Failed to read sheet {sheet_url}: {e}"
            print(f"[LeadUpload] ❌ {msg}")
            errors.append(msg)
            continue

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False)
        print(f"[LeadUpload] 📋 Columns: {list(df.columns)}")

        phone_col = None
        name_col  = None
        for col in df.columns:
            if phone_col is None and re.search(r'phone', col):
                phone_col = col
            if name_col is None and re.search(r'name', col):
                name_col = col

        if not phone_col:
            msg = f"No phone column found in sheet: {sheet_url}"
            print(f"[LeadUpload] ❌ {msg}")
            errors.append(msg)
            continue

        print(f"[LeadUpload] 📌 Phone col: '{phone_col}' | Name col: '{name_col}'")

        for i, row in df.iterrows():
            raw_phone = row.get(phone_col)
            raw_name  = row.get(name_col, "") if name_col else ""

            # Skip repeated header rows mid-sheet
            if is_header_value(raw_phone) or is_header_value(raw_name):
                skipped += 1
                continue

            phone = clean_phone(raw_phone)
            name  = str(raw_name).strip() if raw_name and str(raw_name).lower() not in ["nan", "none"] else ""

            if not phone:
                print(f"[LeadUpload] ⚠️  Row {i+2} skipped (invalid phone: '{raw_phone}')")
                skipped += 1
                continue

            if phone in existing_numbers:
                print(f"[LeadUpload] ⏩ {phone} already exists")
                skipped += 1
                continue

            response = send_to_vicidial(phone, name)

            if "SUCCESS" in response.upper():
                success += 1
                existing_numbers.add(phone)
                print(f"[LeadUpload] ✅ {phone} | {name} → Added")
            else:
                failed += 1
                msg = f"{phone} → {response}"
                print(f"[LeadUpload] ❌ {msg}")
                errors.append(msg)

            time.sleep(0.2)  # rate limit

    return {
        "success": success,
        "failed":  failed,
        "skipped": skipped,
        "errors":  errors,
    }


# ═══════════════════════════════════════════════════════════════
#  SYNC  — MySQL recording_log → PostgreSQL call_analysis
# ═══════════════════════════════════════════════════════════════

def parse_new_url(url: str) -> str:
    parsed    = urlparse(url)
    new_netloc = f"{parsed.hostname}:5165"
    return urlunparse(parsed._replace(netloc=new_netloc))


def sync_recording_log(target_date: str = None) -> dict:
    if not target_date:
        target_date = today_str()

    mysql_conn     = get_mysql_conn()
    inserted       = 0
    skipped_no_loc = 0
    skipped_wav    = 0
    skipped_exists = 0
    location_fixed = 0
    errors         = []
    inserted_rows  = []

    try:
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT recording_id, channel, server_ip, extension,
                       start_time, start_epoch, end_time, end_epoch,
                       length_in_sec, length_in_min, filename, location,
                       lead_id, user AS agent_user, vicidial_id
                FROM recording_log
                WHERE DATE(start_time) = %s
                ORDER BY start_time DESC
            """, (target_date,))
            rows = cur.fetchall()

        print(f"[Sync] Fetched {len(rows)} rows from MySQL for {target_date}")

        for row in rows:
            filename = (row.get("filename") or "").strip()
            location = (row.get("location") or "").strip()
            rec_id   = str(row.get("recording_id")) if row.get("recording_id") is not None else None

            if not filename:
                skipped_no_loc += 1
                continue

            if not location:
                skipped_no_loc += 1
                print(f"[Sync] ⏭ No location yet: {filename}")
                continue

            if location.lower().endswith(".wav"):
                skipped_wav += 1
                print(f"[Sync] ⏭ WAV not yet converted: {filename}")
                continue

            if filename_exists_in_pg(filename):
                skipped_exists += 1
                continue

            phone = extract_phone_number(location)

            try:
                pg = get_pg_conn()
                try:
                    cur_pg = pg.cursor()
                    cur_pg.execute("""
                        INSERT INTO call_analysis (
                            recording_id, channel, server_ip, extension,
                            start_time, start_epoch, end_time, end_epoch,
                            length_in_sec, length_in_min, filename, location,
                            phone_number, lead_id, agent_user, vicidial_id, status
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                        ON CONFLICT (filename) DO NOTHING
                        RETURNING id
                    """, (
                        rec_id,
                        row.get("channel"),     row.get("server_ip"),
                        row.get("extension"),   row.get("start_time"),
                        row.get("start_epoch"), row.get("end_time"),
                        row.get("end_epoch"),   row.get("length_in_sec"),
                        row.get("length_in_min"), filename, location, phone,
                        row.get("lead_id"),     row.get("agent_user"),
                        row.get("vicidial_id"),
                    ))
                    result_row = cur_pg.fetchone()
                    pg.commit()
                    cur_pg.close()
                    if result_row:
                        inserted += 1
                        print(f"[Sync] ✓ Inserted id={result_row[0]}: {filename}")
                        inserted_rows.append({"id": result_row[0], "filename": filename})
                    else:
                        skipped_exists += 1
                except Exception:
                    pg.rollback()
                    raise
                finally:
                    pg.close()
            except Exception as e:
                err = f"{filename} → {e}"
                errors.append(err)
                print(f"[Sync ERROR] {err}")

        # ── Backfill NULL/WAV location rows ──
        try:
            pg     = get_pg_conn()
            cur_pg = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur_pg.execute("""
                SELECT id, filename, location FROM call_analysis
                WHERE DATE(start_time) = %s AND status = 'pending'
                  AND (location IS NULL OR location = '' OR lower(location) LIKE '%%.wav')
            """, (target_date,))
            null_rows = cur_pg.fetchall()
            if null_rows:
                print(f"[Sync] Backfilling {len(null_rows)} pending row(s)…")
                with mysql_conn.cursor(pymysql.cursors.DictCursor) as fix_cur:
                    for pg_row in null_rows:
                        fix_cur.execute("""
                            SELECT location, length_in_sec, length_in_min
                            FROM recording_log
                            WHERE filename = %s
                              AND location IS NOT NULL AND location != ''
                              AND lower(location) LIKE '%%.mp3'
                            LIMIT 1
                        """, (pg_row["filename"],))
                        mysql_row = fix_cur.fetchone()
                        if mysql_row and mysql_row.get("location"):
                            new_loc   = parse_new_url(mysql_row["location"].strip())
                            new_phone = extract_phone_number(new_loc)
                            cur_pg.execute("""
                                UPDATE call_analysis
                                SET location = %s, phone_number = %s,
                                    length_in_sec = %s, length_in_min = %s
                                WHERE id = %s
                            """, (new_loc, new_phone,
                                  mysql_row.get("length_in_sec"),
                                  mysql_row.get("length_in_min"),
                                  pg_row["id"]))
                            pg.commit()
                            location_fixed += 1
                            print(f"[Sync] ✓ Backfilled id={pg_row['id']}: {pg_row['filename']}")
                        else:
                            print(f"[Sync] ⏭ No MP3 yet for: {pg_row['filename']}")
            cur_pg.close()
            pg.close()
        except Exception as e:
            print(f"[Sync] Backfill warning: {e}")

        summary = {
            "date": target_date, "total_fetched": len(rows),
            "inserted": inserted, "skipped_no_loc": skipped_no_loc,
            "skipped_wav": skipped_wav, "skipped_exists": skipped_exists,
            "backfilled": location_fixed, "error_count": len(errors),
            "errors": errors, "inserted_rows": inserted_rows,
        }
        print(
            f"[Sync] Done — inserted={inserted} | skipped_no_loc={skipped_no_loc} | "
            f"skipped_wav={skipped_wav} | skipped_exists={skipped_exists} | "
            f"backfilled={location_fixed} | errors={len(errors)}"
        )
        return summary
    finally:
        mysql_conn.close()


# ═══════════════════════════════════════════════════════════════
#  ANALYZE  — pending rows → Whisper → GPT-4o
# ═══════════════════════════════════════════════════════════════

def get_unanalyzed(target_date: str = None) -> list:
    if not target_date:
        target_date = today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, recording_id, filename, location, lead_id, agent_user, length_in_sec
            FROM call_analysis
            WHERE status = 'pending'
              AND location IS NOT NULL AND location != ''
              AND lower(location) LIKE '%%.mp3'
              AND DATE(start_time) = %s
            ORDER BY start_time ASC
        """, (target_date,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def download_audio(url: str) -> str:
    new_url = parse_new_url(url)
    print(f"[Download] {new_url}")
    r = requests.get(new_url, timeout=60)
    if r.status_code == 404:
        raise FileNotFoundError(f"Recording not found (404): {new_url}")
    if r.status_code != 200:
        raise Exception(f"HTTP {r.status_code} downloading: {new_url}")
    suffix = Path(new_url.split("?")[0]).suffix or ".mp3"
    tmp    = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp.write(r.content)
    tmp.flush()
    tmp.close()
    return tmp.name


def transcribe_audio(file_path: str) -> str:
    with open(file_path, "rb") as f:
        response = client.audio.transcriptions.create(
            model="whisper-1", file=f, response_format="text",
        )
    return response


def rate_agent(transcript: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o", temperature=0.3, max_tokens=1500,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": (
                "Analyze this call transcript and rate the agent's performance.\n"
                "IMPORTANT: If this sounds like a voicemail, IVR menu, hold music, "
                "or automated system with no real human agent-customer conversation, "
                "return the Not Connected JSON.\n\n"
                f"--- TRANSCRIPT ---\n{transcript}\n--- END ---"
            )},
        ],
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def update_analysis(row_id, transcript, rating, volume_boosted, avg_dbfs):
    conn = get_pg_conn()
    try:
        cats = rating.get("categories", {})
        cur  = conn.cursor()
        cur.execute("""
            UPDATE call_analysis SET
                transcript = %s, overall_rating = %s, stars = %s, summary = %s,
                call_outcome = %s, agent_sentiment = %s, client_sentiment = %s,
                greeting_score = %s, greeting_comment = %s,
                product_knowledge_score = %s, product_knowledge_comment = %s,
                convincing_score = %s, convincing_comment = %s,
                objection_score = %s, objection_comment = %s,
                clarity_score = %s, clarity_comment = %s,
                empathy_score = %s, empathy_comment = %s,
                closing_score = %s, closing_comment = %s,
                strengths = %s, improvements = %s,
                avg_dbfs = %s, volume_boosted = %s,
                analyzed_at = CURRENT_TIMESTAMP, status = 'successful'
            WHERE id = %s
        """, (
            transcript,
            rating.get("overall_rating"), rating.get("stars"),
            rating.get("summary"),        rating.get("call_outcome"),
            rating.get("agent_sentiment"), rating.get("client_sentiment"),
            cats.get("greeting_professionalism", {}).get("score"),
            cats.get("greeting_professionalism", {}).get("comment"),
            cats.get("product_knowledge",        {}).get("score"),
            cats.get("product_knowledge",        {}).get("comment"),
            cats.get("convincing_ability",       {}).get("score"),
            cats.get("convincing_ability",       {}).get("comment"),
            cats.get("objection_handling",       {}).get("score"),
            cats.get("objection_handling",       {}).get("comment"),
            cats.get("communication_clarity",    {}).get("score"),
            cats.get("communication_clarity",    {}).get("comment"),
            cats.get("empathy_patience",         {}).get("score"),
            cats.get("empathy_patience",         {}).get("comment"),
            cats.get("closing_technique",        {}).get("score"),
            cats.get("closing_technique",        {}).get("comment"),
            json.dumps(rating.get("strengths",    [])),
            json.dumps(rating.get("improvements", [])),
            round(avg_dbfs, 2) if not math.isinf(avg_dbfs) else None,
            volume_boosted, row_id,
        ))
        conn.commit()
        cur.close()
        print(f"[DB] ✓ id={row_id} → successful")
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] update_analysis failed for id={row_id}: {e}")
        raise
    finally:
        conn.close()


def process_recording(row: dict) -> dict:
    location     = (row.get("location") or "").strip()
    filename     = row.get("filename", "")
    row_id       = row.get("id")
    tmp_path     = None
    boosted_path = None

    if not location:
        raise Exception(f"No location for filename={filename}")

    try:
        # 1. Download
        try:
            tmp_path = download_audio(location)
        except FileNotFoundError as e:
            print(f"[NoFile] {filename} → {e}")
            mark_status(row_id, "no_recording")
            return {"id": row_id, "filename": filename, "status": "no_recording", "reason": str(e)}

        # 2. Audio analysis
        audio_info     = analyze_audio(tmp_path)
        is_ringing     = audio_info["is_ringing"]
        is_low_volume  = audio_info["is_low_volume"]
        avg_dbfs       = audio_info["avg_dbfs"]
        volume_boosted = False

        print(f"[Audio] {filename} | dBFS={avg_dbfs:.1f} | ringing={is_ringing} | low_vol={is_low_volume}")

        if is_ringing:
            set_not_picked(row_id, "Ringing tone detected — call never connected.", avg_dbfs)
            return {"id": row_id, "filename": filename, "avg_dbfs": avg_dbfs,
                    "status": "not_picked", "reason": "Ringing tone detected."}

        audio_file = tmp_path
        if is_low_volume:
            print(f"[Audio] {filename} — low volume ({avg_dbfs:.1f} dBFS), boosting…")
            boosted_path   = boost_audio(tmp_path)
            audio_file     = boosted_path
            volume_boosted = True
            avg_dbfs       = analyze_audio(audio_file)["avg_dbfs"]

        # 3. Transcribe
        transcript       = transcribe_audio(audio_file)
        transcript_clean = transcript.strip()
        print(f"[Whisper] {filename} → {len(transcript_clean)} chars")

        if len(transcript_clean) < 20:
            mark_status(row_id, "skipped")
            return {"id": row_id, "filename": filename, "status": "skipped",
                    "reason": "transcript too short (< 20 chars)"}

        # 4. GPT-4o rating
        rating  = rate_agent(transcript_clean)
        stars   = rating.get("stars", 0)
        outcome = rating.get("call_outcome", "")

        if stars == 0 or outcome == "Not Connected":
            set_not_picked(row_id, summary=rating.get("summary"),
                           avg_dbfs=avg_dbfs, volume_boosted=volume_boosted)
            return {"id": row_id, "filename": filename, "stars": 0,
                    "call_outcome": "Not Connected", "status": "not_picked"}

        update_analysis(row_id, transcript_clean, rating, volume_boosted, avg_dbfs)
        return {
            "id": row_id, "filename": filename, "avg_dbfs": avg_dbfs,
            "volume_boosted": volume_boosted, "stars": stars,
            "overall_rating": rating.get("overall_rating"),
            "call_outcome": outcome, "summary": rating.get("summary"),
            "agent_sentiment": rating.get("agent_sentiment"),
            "client_sentiment": rating.get("client_sentiment"),
            "categories": rating.get("categories"),
            "strengths": rating.get("strengths"),
            "improvements": rating.get("improvements"),
            "transcript": transcript_clean, "status": "successful",
        }
    finally:
        for p in [tmp_path, boosted_path]:
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════
#  BACKGROUND LOOP 1 — Call Analyzer (every 5 min)
# ═══════════════════════════════════════════════════════════════

def _run_one_analyzer_cycle(target_date: str) -> dict:
    cycle_inserted, cycle_analyzed, cycle_failed, cycle_errors = 0, 0, 0, []

    try:
        sync_result    = sync_recording_log(target_date=target_date)
        cycle_inserted = sync_result.get("inserted", 0)
        print(
            f"[AutoLoop] Sync → inserted={sync_result['inserted']} | "
            f"skipped_no_loc={sync_result['skipped_no_loc']} | "
            f"skipped_wav={sync_result['skipped_wav']} | "
            f"skipped_exists={sync_result['skipped_exists']} | "
            f"backfilled={sync_result['backfilled']}"
        )
    except Exception as e:
        msg = f"Sync failed: {e}"
        print(f"[AutoLoop] {msg}")
        cycle_errors.append(msg)
        _log_error(msg)

    try:
        pending = get_unanalyzed(target_date=target_date)
        print(f"[AutoLoop] Analyze → {len(pending)} pending recording(s)")
        for row in pending:
            try:
                result = process_recording(row)
                print(f"[AutoLoop] ✓ {result['filename']} | status={result.get('status')} | stars={result.get('stars', 0)}/5")
                cycle_analyzed += 1
            except Exception as e:
                cycle_failed += 1
                mark_status(row["id"], "failed")
                msg = f"{row.get('filename')}: {e}"
                print(f"[AutoLoop] ✗ {msg}")
                cycle_errors.append(msg)
                _log_error(msg)
    except Exception as e:
        msg = f"Analyze step failed: {e}"
        print(f"[AutoLoop] {msg}")
        cycle_errors.append(msg)
        _log_error(msg)

    return {"inserted": cycle_inserted, "analyzed": cycle_analyzed,
            "failed": cycle_failed, "errors": cycle_errors}


def auto_sync_analyze_loop():
    print(f"[AutoLoop] Started — interval={AUTO_LOOP_INTERVAL_SECONDS // 60} min")
    with _loop_lock:
        _loop_state["running"] = True
    while True:
        today = today_str()
        now   = now_str()
        print(f"\n[AutoLoop] ══════ {now} | {today} ══════")
        with _loop_lock:
            _loop_state["iteration"]  += 1
            _loop_state["last_run_at"] = now
        cycle = _run_one_analyzer_cycle(target_date=today)
        next_run = datetime.fromtimestamp(
            datetime.now().timestamp() + AUTO_LOOP_INTERVAL_SECONDS
        ).strftime("%Y-%m-%d %H:%M:%S")
        with _loop_lock:
            _loop_state["next_run_at"]     = next_run
            _loop_state["last_sync"]       = {"inserted": cycle["inserted"]}
            _loop_state["last_analyze"]    = {"analyzed": cycle["analyzed"], "failed": cycle["failed"]}
            _loop_state["total_inserted"] += cycle["inserted"]
            _loop_state["total_analyzed"] += cycle["analyzed"]
            _loop_state["total_failed"]   += cycle["failed"]
        print(f"[AutoLoop] Cycle done — inserted={cycle['inserted']} | analyzed={cycle['analyzed']} | failed={cycle['failed']} | next={next_run}\n")
        time.sleep(AUTO_LOOP_INTERVAL_SECONDS)


# ═══════════════════════════════════════════════════════════════
#  BACKGROUND LOOP 2 — Lead Uploader (every 10 min)
# ═══════════════════════════════════════════════════════════════

def auto_lead_upload_loop():
    print(f"[LeadLoop] Started — interval={LEAD_UPLOAD_INTERVAL_SECONDS // 60} min")
    with _lead_loop_lock:
        _lead_loop_state["running"] = True

    while True:
        now = now_str()
        print(f"\n[LeadLoop] ══════ {now} ══════")
        with _lead_loop_lock:
            _lead_loop_state["iteration"]  += 1
            _lead_loop_state["last_run_at"] = now

        cycle = run_lead_upload_cycle()

        next_run = datetime.fromtimestamp(
            datetime.now().timestamp() + LEAD_UPLOAD_INTERVAL_SECONDS
        ).strftime("%Y-%m-%d %H:%M:%S")

        with _lead_loop_lock:
            _lead_loop_state["next_run_at"]     = next_run
            _lead_loop_state["last_cycle"]      = cycle
            _lead_loop_state["total_success"]  += cycle["success"]
            _lead_loop_state["total_failed"]   += cycle["failed"]
            _lead_loop_state["total_skipped"]  += cycle["skipped"]
            for e in cycle.get("errors", []):
                _log_lead_error(e)

        print(
            f"[LeadLoop] Cycle done — success={cycle['success']} | "
            f"failed={cycle['failed']} | skipped={cycle['skipped']} | next={next_run}\n"
        )
        time.sleep(LEAD_UPLOAD_INTERVAL_SECONDS)


# ═══════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup():
    create_table()

    try:
        conn = get_pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE call_analysis SET status = 'pending', analyzed_at = NULL
            WHERE status = 'not_picked' AND analyzed_at IS NULL
              AND avg_dbfs IS NULL AND location IS NOT NULL AND location != ''
              AND lower(location) LIKE '%%.mp3'
        """)
        cur.execute("""
            UPDATE call_analysis SET status = 'not_picked'
            WHERE status = 'skipped' AND stars = 0 AND analyzed_at IS NOT NULL
        """)
        cur.execute("DELETE FROM call_analysis WHERE status = 'pending' AND lower(location) LIKE '%%.wav'")
        cur.execute("DELETE FROM call_analysis WHERE status = 'pending' AND (location IS NULL OR location = '')")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[Startup] Migration warning: {e}")

    # Start both background loops as daemon threads
    threading.Thread(target=auto_sync_analyze_loop, daemon=True, name="AnalyzerLoop").start()
    threading.Thread(target=auto_lead_upload_loop,  daemon=True, name="LeadUploadLoop").start()
    print("[Startup] ✅ Both background loops started.")
    print(f"[Startup]   • Call Analyzer  → every {AUTO_LOOP_INTERVAL_SECONDS   // 60} min")
    print(f"[Startup]   • Lead Uploader  → every {LEAD_UPLOAD_INTERVAL_SECONDS // 60} min")


# ═══════════════════════════════════════════════════════════════
#  ROUTES — Health & Status
# ═══════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {
        "status":  "ok",
        "version": "6.0.0",
        "model":   "gpt-4o",
        "today":   today_str(),
        "loops": {
            "call_analyzer": f"every {AUTO_LOOP_INTERVAL_SECONDS   // 60} min",
            "lead_uploader": f"every {LEAD_UPLOAD_INTERVAL_SECONDS // 60} min",
        },
        "pydub_available": PYDUB_AVAILABLE,
    }


@app.get("/loop-status")
def loop_status():
    with _loop_lock:
        return dict(_loop_state)


@app.get("/lead-loop-status")
def lead_loop_status():
    with _lead_loop_lock:
        return dict(_lead_loop_state)


@app.get("/status-summary")
def status_summary(date: str = None):
    target_date = date or today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT status, COUNT(*) AS cnt FROM call_analysis
            WHERE DATE(start_time) = %s GROUP BY status ORDER BY cnt DESC
        """, (target_date,))
        rows = cur.fetchall()
        cur.close()
        return {"date": target_date, "summary": {r[0]: r[1] for r in rows}}
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  ROUTES — Manual Triggers
# ═══════════════════════════════════════════════════════════════

@app.post("/sync-recordings")
def sync_recordings_endpoint(date: str = None):
    try:
        result = sync_recording_log(target_date=date)
        return {"message": "Sync complete", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze-all")
def analyze_all_endpoint(date: str = None):
    target_date = date or today_str()
    rows = get_unanalyzed(target_date=target_date)
    if not rows:
        return {"message": f"No pending recordings for {target_date}.", "date": target_date, "processed": 0}
    results, errors = [], []
    for row in rows:
        try:
            results.append(process_recording(row))
        except Exception as e:
            mark_status(row["id"], "failed")
            errors.append({"filename": row.get("filename"), "error": str(e)})
    return {"date": target_date, "processed": len(results), "failed": len(errors),
            "results": results, "errors": errors}


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
        raise HTTPException(status_code=404, detail=f"'{filename}' not found.")
    row = dict(row)
    if row.get("status") == "successful":
        raise HTTPException(status_code=409, detail=f"'{filename}' already analyzed successfully.")
    try:
        return JSONResponse(content=process_recording(row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-leads")
def upload_leads_endpoint():
    """Manually trigger one lead upload cycle across all sheets."""
    try:
        result = run_lead_upload_cycle()
        return {"message": "Lead upload cycle complete", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
#  ROUTES — Results
# ═══════════════════════════════════════════════════════════════

@app.get("/results")
def get_results(limit: int = 50, offset: int = 0, date: str = None, status: str = None):
    target_date = date or today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        base = """
            SELECT id, recording_id, channel, server_ip, extension,
                   start_time, start_epoch, end_time, end_epoch,
                   length_in_sec, length_in_min, filename, location,
                   phone_number, lead_id, agent_user, vicidial_id, synced_at,
                   avg_dbfs, volume_boosted, overall_rating, stars, call_outcome, summary,
                   agent_sentiment, client_sentiment,
                   greeting_score, product_knowledge_score, convincing_score,
                   objection_score, clarity_score, empathy_score, closing_score,
                   strengths, improvements, analyzed_at, status
            FROM call_analysis WHERE DATE(start_time) = %s
        """
        if status:
            cur.execute(base + " AND status = %s ORDER BY start_time DESC LIMIT %s OFFSET %s",
                        (target_date, status, limit, offset))
        else:
            cur.execute(base + " ORDER BY start_time DESC LIMIT %s OFFSET %s",
                        (target_date, limit, offset))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        for row in rows:
            for f in ["start_time", "end_time", "synced_at", "analyzed_at"]:
                if row.get(f):
                    row[f] = str(row[f])
        return {"date": target_date, "status_filter": status, "total": len(rows), "data": rows}
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
            if row.get(f):
                row[f] = str(row[f])
        for f in ["strengths", "improvements"]:
            if row.get(f):
                try:
                    row[f] = json.loads(row[f])
                except Exception:
                    pass
        return row
    finally:
        conn.close()


@app.get("/debug-sync")
def debug_sync(date: str = None):
    target_date = date or today_str()
    result = {"date": target_date}
    try:
        mc = get_mysql_conn()
        with mc.cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM recording_log WHERE DATE(start_time) = %s", (target_date,))
            result["mysql_total"] = cur.fetchone().get("total", 0)
            cur.execute("SELECT COUNT(*) as cnt FROM recording_log WHERE DATE(start_time) = %s AND location IS NOT NULL AND location != '' AND lower(location) LIKE '%%.mp3'", (target_date,))
            result["mysql_mp3_count"] = cur.fetchone().get("cnt", 0)
            cur.execute("SELECT COUNT(*) as cnt FROM recording_log WHERE DATE(start_time) = %s AND location IS NOT NULL AND location != '' AND lower(location) LIKE '%%.wav'", (target_date,))
            result["mysql_wav_count"] = cur.fetchone().get("cnt", 0)
            cur.execute("SELECT COUNT(*) as cnt FROM recording_log WHERE DATE(start_time) = %s AND (location IS NULL OR location = '')", (target_date,))
            result["mysql_no_location"] = cur.fetchone().get("cnt", 0)
        mc.close()
        result["mysql_status"] = "ok"
    except Exception as e:
        result["mysql_status"] = f"ERROR: {e}"
    try:
        pg  = get_pg_conn()
        cur = pg.cursor()
        cur.execute("SELECT COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s", (target_date,))
        result["pg_total_for_date"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM call_analysis")
        result["pg_total_all"] = cur.fetchone()[0]
        cur.execute("SELECT status, COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s GROUP BY status", (target_date,))
        result["pg_status_breakdown"] = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()
        pg.close()
        result["pg_status"] = "ok"
    except Exception as e:
        result["pg_status"] = f"ERROR: {e}"
    return result