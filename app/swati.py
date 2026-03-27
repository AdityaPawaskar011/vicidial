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

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
AUTO_LOOP_INTERVAL_SECONDS = 5 * 60        # 5 minutes

# pydub volume thresholds
LOW_VOLUME_THRESHOLD_DBFS = -30.0
BOOST_TARGET_DBFS         = -18.0

# ─────────────────────────────────────────────
# STATUS VALUES  (call_analysis.status column)
# ─────────────────────────────────────────────
#  pending       — inserted with valid MP3 location, not yet processed
#  not_picked    — ringing-only audio  OR  GPT confirmed "Not Connected"
#  successful    — fully transcribed & rated by GPT-4o (stars >= 1)
#  failed        — unrecoverable processing error
#  no_recording  — audio file returned 404
#  skipped       — transcript genuinely too short (< 20 chars)

# ─────────────────────────────────────────────
# LOOP STATE
# ─────────────────────────────────────────────
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

app = FastAPI(
    title="VICIdial Call Quality Analyzer",
    description="Auto sync + analyze every 5 minutes",
    version="5.5.0",
)

# ─────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are an expert call center quality analyst with 15+ years of experience.
Evaluate the agent's performance on the call and respond ONLY with valid JSON — no markdown, no extra text.

IMPORTANT — BEFORE evaluating, carefully check if a real two-way human conversation took place.

Return the "Not Connected" JSON if ANY of these are true:
  - The transcript is empty or under 10 words
  - Only one side is speaking (e.g. only the agent, no customer response)
  - The content is purely a voicemail greeting or beep
  - The content is an IVR / automated phone menu
  - The content is hold music or silence descriptions
  - No meaningful exchange of information occurred

"Not Connected" response (use EXACTLY this):
{
  "overall_rating": 0, "stars": 0,
  "summary": "Call not connected — no conversation to evaluate.",
  "call_outcome": "Not Connected",
  "agent_sentiment": "N/A", "client_sentiment": "N/A",
  "categories": {
    "greeting_professionalism":  { "score": 0, "comment": "Not applicable." },
    "product_knowledge":         { "score": 0, "comment": "Not applicable." },
    "convincing_ability":        { "score": 0, "comment": "Not applicable." },
    "objection_handling":        { "score": 0, "comment": "Not applicable." },
    "communication_clarity":     { "score": 0, "comment": "Not applicable." },
    "empathy_patience":          { "score": 0, "comment": "Not applicable." },
    "closing_technique":         { "score": 0, "comment": "Not applicable." }
  },
  "strengths": [], "improvements": []
}

Only if a real two-way human conversation took place, use this format:
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


def is_mp3_location(location: str) -> bool:
    """Return True only if the location URL points to an MP3 file."""
    if not location:
        return False
    return location.strip().lower().endswith(".mp3")


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
    """Update a row to not_picked with optional metadata."""
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


# ═══════════════════════════════════════════════════════════════
#  AUDIO HELPERS  (pydub — optional, needs ffmpeg)
# ═══════════════════════════════════════════════════════════════

def _detect_ringing_pattern(chunk_levels: list) -> bool:
    """
    Returns True when the recording looks like an unanswered ringing call.
    Looks for periodic on/off energy with 4+ transitions and
    roughly equal run lengths (low variance).
    """
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
    """
    Analyze audio file quality.
    Returns avg_dbfs, is_ringing, is_low_volume, duration_ms.
    Falls back to safe defaults if pydub/ffmpeg unavailable.
    """
    default = {
        "avg_dbfs":      -999,
        "is_ringing":    False,
        "is_low_volume": False,
        "duration_ms":   0,
    }
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
    """
    Normalize + gain-boost audio to BOOST_TARGET_DBFS.
    Returns path to a NEW temp file (caller must clean it up).
    """
    audio = AudioSegment.from_file(file_path)
    if math.isinf(audio.dBFS):
        print("[Audio] Boost skipped — completely silent file.")
        return file_path
    audio_norm  = normalize(audio)
    gain_needed = BOOST_TARGET_DBFS - audio_norm.dBFS
    boosted     = audio_norm + gain_needed
    print(
        f"[Audio] Boosted: {audio.dBFS:.1f} dBFS → {boosted.dBFS:.1f} dBFS "
        f"(gain {gain_needed:+.1f} dB)"
    )
    suffix = Path(file_path).suffix or ".mp3"
    tmp    = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    boosted.export(tmp.name, format=suffix.lstrip(".") or "mp3")
    tmp.close()
    return tmp.name


# ═══════════════════════════════════════════════════════════════
#  CREATE / MIGRATE TABLE
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

        # Safe migration — add columns introduced in v5
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
        print("[DB] Table 'call_analysis' ready (v5.5).")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  SYNC  — MySQL recording_log → PostgreSQL call_analysis
#
#  INSERT conditions (ALL must be true):
#    1. filename is not empty
#    2. location is NOT NULL and NOT empty
#    3. location ends with .mp3  (WAV = skip, retry next cycle)
#    4. filename not already in PG
# ═══════════════════════════════════════════════════════════════

def sync_recording_log(target_date: str = None) -> dict:
    """
    Pull rows from MySQL recording_log for target_date and insert into PG.

    Skip rules (retried automatically next cycle):
      • location IS NULL or empty  — file not written yet
      • location ends with .wav    — WAV not yet converted to MP3

    All inserted rows start as status='pending'.
    Audio + GPT pipeline is the sole decider of not_picked.
    ON CONFLICT (filename) DO NOTHING — safe to run repeatedly.

    Also backfills existing pending PG rows:
      • NULL location rows  — if MySQL now has a location
      • WAV location rows   — if MySQL now has an MP3 location
    """
    if not target_date:
        target_date = today_str()

    mysql_conn     = get_mysql_conn()
    inserted       = 0
    skipped_no_loc = 0   # NULL / empty location
    skipped_wav    = 0   # location is WAV (not yet converted)
    skipped_exists = 0   # already in PG
    location_fixed = 0   # existing PG rows backfilled with MP3 location
    errors         = []
    inserted_rows  = []

    try:
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    recording_id, channel, server_ip, extension,
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

            # ── Skip: no location yet ────────────────────────────
            if not location:
                skipped_no_loc += 1
                print(f"[Sync] ⏭ No location yet (retry next cycle): {filename}")
                continue

            # ── Skip: WAV file — wait for MP3 conversion ─────────
            if location.lower().endswith(".wav"):
                skipped_wav += 1
                print(f"[Sync] ⏭ WAV not yet converted to MP3 (retry next cycle): {filename}")
                continue

            # ── Skip: already in PG ──────────────────────────────
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
                            phone_number, lead_id, agent_user, vicidial_id,
                            status
                        ) VALUES (
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            'pending'
                        )
                        ON CONFLICT (filename) DO NOTHING
                        RETURNING id
                    """, (
                        rec_id,
                        row.get("channel"),       row.get("server_ip"),
                        row.get("extension"),     row.get("start_time"),
                        row.get("start_epoch"),   row.get("end_time"),
                        row.get("end_epoch"),     row.get("length_in_sec"),
                        row.get("length_in_min"), filename,
                        location,                 phone,
                        row.get("lead_id"),       row.get("agent_user"),
                        row.get("vicidial_id"),
                    ))
                    result_row = cur_pg.fetchone()
                    pg.commit()
                    cur_pg.close()

                    if result_row:
                        inserted += 1
                        print(f"[Sync] ✓ Inserted id={result_row[0]} [MP3]: {filename}")
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

        # ── Backfill: update PG rows that have NULL or WAV location ──
        # Covers rows inserted by older code, or rows waiting for MP3
        try:
            pg     = get_pg_conn()
            cur_pg = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Find pending PG rows with no location OR a WAV location
            cur_pg.execute("""
                SELECT id, filename, location FROM call_analysis
                WHERE DATE(start_time) = %s
                  AND status = 'pending'
                  AND (
                      location IS NULL
                      OR location = ''
                      OR lower(location) LIKE '%.wav'
                  )
            """, (target_date,))
            null_rows = cur_pg.fetchall()

            if null_rows:
                print(f"[Sync] Backfilling {len(null_rows)} pending row(s) with NULL/WAV location…")
                with mysql_conn.cursor(pymysql.cursors.DictCursor) as fix_cur:
                    for pg_row in null_rows:
                        # Look for an MP3 location in MySQL
                        fix_cur.execute("""
                            SELECT location, length_in_sec, length_in_min
                            FROM recording_log
                            WHERE filename = %s
                              AND location IS NOT NULL
                              AND location != ''
                              AND lower(location) LIKE '%%.mp3'
                            LIMIT 1
                        """, (pg_row["filename"],))
                        mysql_row = fix_cur.fetchone()
                        if mysql_row and mysql_row.get("location"):
                            new_loc   = parse_new_url(mysql_row["location"].strip())
                            new_phone = extract_phone_number(new_loc)
                            cur_pg.execute("""
                                UPDATE call_analysis
                                SET location      = %s,
                                    phone_number  = %s,
                                    length_in_sec = %s,
                                    length_in_min = %s
                                WHERE id = %s
                            """, (
                                new_loc,
                                new_phone,
                                mysql_row.get("length_in_sec"),
                                mysql_row.get("length_in_min"),
                                pg_row["id"],
                            ))
                            pg.commit()
                            location_fixed += 1
                            old_loc = pg_row.get("location") or "NULL"
                            print(
                                f"[Sync] ✓ Backfilled id={pg_row['id']} "
                                f"({old_loc[-15:]} → MP3): {pg_row['filename']}"
                            )
                        else:
                            print(f"[Sync] ⏭ No MP3 yet in MySQL for: {pg_row['filename']}")

            cur_pg.close()
            pg.close()
        except Exception as e:
            print(f"[Sync] Backfill warning: {e}")

        summary = {
            "date":            target_date,
            "total_fetched":   len(rows),
            "inserted":        inserted,
            "skipped_no_loc":  skipped_no_loc,
            "skipped_wav":     skipped_wav,
            "skipped_exists":  skipped_exists,
            "backfilled":      location_fixed,
            "error_count":     len(errors),
            "errors":          errors,
            "inserted_rows":   inserted_rows,
        }
        print(
            f"[Sync] Done — inserted={inserted} | "
            f"skipped_no_loc={skipped_no_loc} | skipped_wav={skipped_wav} | "
            f"skipped_exists={skipped_exists} | backfilled={location_fixed} | "
            f"errors={len(errors)}"
        )
        return summary

    finally:
        mysql_conn.close()


# ═══════════════════════════════════════════════════════════════
#  ANALYZE  — pending rows → audio → Whisper → GPT-4o
# ═══════════════════════════════════════════════════════════════

def get_unanalyzed(target_date: str = None) -> list:
    """Return all status='pending' rows with a valid MP3 location for the given date."""
    if not target_date:
        target_date = today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, recording_id, filename, location, lead_id, agent_user, length_in_sec
            FROM call_analysis
            WHERE status   = 'pending'
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

def parse_new_url(url):
    parsed = urlparse(url)
    new_netloc = f"{parsed.hostname}:5165"
    return urlunparse(parsed._replace(netloc=new_netloc))


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


def update_analysis(
    row_id: int,
    transcript: str,
    rating: dict,
    volume_boosted: bool,
    avg_dbfs: float,
):
    """Write a successful GPT-4o analysis to PostgreSQL with status=successful."""
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
                greeting_score              = %s,   greeting_comment          = %s,
                product_knowledge_score     = %s,   product_knowledge_comment = %s,
                convincing_score            = %s,   convincing_comment        = %s,
                objection_score             = %s,   objection_comment         = %s,
                clarity_score               = %s,   clarity_comment           = %s,
                empathy_score               = %s,   empathy_comment           = %s,
                closing_score               = %s,   closing_comment           = %s,
                strengths                   = %s,
                improvements                = %s,
                avg_dbfs                    = %s,
                volume_boosted              = %s,
                analyzed_at                 = CURRENT_TIMESTAMP,
                status                      = 'successful'
            WHERE id = %s
        """, (
            transcript,
            rating.get("overall_rating"),  rating.get("stars"),
            rating.get("summary"),         rating.get("call_outcome"),
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
            volume_boosted,
            row_id,
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
    """
    Full pipeline for one pending recording:

      1. Download audio (MP3 only at this point)
      2. Audio analysis (pydub — requires ffmpeg):
           • Ringing throughout  → not_picked  (no Whisper/GPT cost)
           • Low volume          → boost audio, then continue
      3. Transcribe with Whisper
           • < 20 chars          → skipped
      4. Rate with GPT-4o
           • "Not Connected"     → not_picked
           • Real conversation   → successful
    """
    location     = (row.get("location") or "").strip()
    filename     = row.get("filename", "")
    row_id       = row.get("id")
    tmp_path     = None
    boosted_path = None

    if not location:
        raise Exception(f"No location for filename={filename}")

    try:
        # ── 1. Download ──────────────────────────────────────────
        try:
            tmp_path = download_audio(location)
        except FileNotFoundError as e:
            print(f"[NoFile] {filename} → {e}")
            mark_status(row_id, "no_recording")
            return {
                "id":       row_id,
                "filename": filename,
                "status":   "no_recording",
                "reason":   str(e),
            }

        # ── 2. Audio analysis ────────────────────────────────────
        audio_info     = analyze_audio(tmp_path)
        is_ringing     = audio_info["is_ringing"]
        is_low_volume  = audio_info["is_low_volume"]
        avg_dbfs       = audio_info["avg_dbfs"]
        volume_boosted = False

        print(
            f"[Audio] {filename} | dBFS={avg_dbfs:.1f} | "
            f"ringing={is_ringing} | low_vol={is_low_volume}"
        )

        # Ringing-only → not_picked immediately (no API cost)
        if is_ringing:
            print(f"[Audio] {filename} — ringing only → not_picked")
            set_not_picked(row_id, "Ringing tone detected — call never connected.", avg_dbfs)
            return {
                "id":       row_id,
                "filename": filename,
                "avg_dbfs": avg_dbfs,
                "status":   "not_picked",
                "reason":   "Ringing tone detected — call never connected.",
            }

        # Low volume → boost before sending to Whisper
        audio_file = tmp_path
        if is_low_volume:
            print(f"[Audio] {filename} — low volume ({avg_dbfs:.1f} dBFS), boosting…")
            boosted_path   = boost_audio(tmp_path)
            audio_file     = boosted_path
            volume_boosted = True
            avg_dbfs       = analyze_audio(audio_file)["avg_dbfs"]
            print(f"[Audio] After boost: {avg_dbfs:.1f} dBFS")

        # ── 3. Transcribe ────────────────────────────────────────
        transcript       = transcribe_audio(audio_file)
        transcript_clean = transcript.strip()
        print(f"[Whisper] {filename} → {len(transcript_clean)} chars")

        if len(transcript_clean) < 20:
            mark_status(row_id, "skipped")
            return {
                "id":       row_id,
                "filename": filename,
                "status":   "skipped",
                "reason":   "transcript too short (< 20 chars)",
            }

        # ── 4. Rate with GPT-4o ──────────────────────────────────
        rating  = rate_agent(transcript_clean)
        stars   = rating.get("stars", 0)
        outcome = rating.get("call_outcome", "")

        # GPT confirmed call was NOT a real conversation → not_picked
        if stars == 0 or outcome == "Not Connected":
            print(f"[GPT-4o] {filename} — Not Connected → not_picked")
            set_not_picked(
                row_id,
                summary        = rating.get("summary"),
                avg_dbfs       = avg_dbfs,
                volume_boosted = volume_boosted,
            )
            return {
                "id":           row_id,
                "filename":     filename,
                "stars":        0,
                "call_outcome": "Not Connected",
                "summary":      rating.get("summary"),
                "status":       "not_picked",
            }

        # Real conversation — save full analysis
        print(f"[GPT-4o] {filename} → {stars}/5 stars | boosted={volume_boosted}")
        update_analysis(row_id, transcript_clean, rating, volume_boosted, avg_dbfs)

        return {
            "id":               row_id,
            "filename":         filename,
            "avg_dbfs":         avg_dbfs,
            "volume_boosted":   volume_boosted,
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
            "status":           "successful",
        }

    finally:
        for p in [tmp_path, boosted_path]:
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════
#  BACKGROUND LOOP — every 5 minutes
# ═══════════════════════════════════════════════════════════════

def _run_one_cycle(target_date: str) -> dict:
    cycle_inserted = 0
    cycle_analyzed = 0
    cycle_failed   = 0
    cycle_errors   = []

    # Step 1 — Sync MySQL records (MP3 only, skip WAV/NULL)
    try:
        sync_result    = sync_recording_log(target_date=target_date)
        cycle_inserted = sync_result.get("inserted", 0)
        print(
            f"[AutoLoop] Sync → inserted={sync_result['inserted']} | "
            f"skipped_no_loc={sync_result['skipped_no_loc']} | "
            f"skipped_wav={sync_result['skipped_wav']} | "
            f"skipped_exists={sync_result['skipped_exists']} | "
            f"backfilled={sync_result['backfilled']} | "
            f"errors={sync_result['error_count']}"
        )
    except Exception as e:
        msg = f"Sync failed: {e}"
        print(f"[AutoLoop] {msg}")
        cycle_errors.append(msg)
        _log_error(msg)

    # Steps 2+3 — Analyze only status='pending' MP3 rows
    try:
        pending = get_unanalyzed(target_date=target_date)
        print(f"[AutoLoop] Analyze → {len(pending)} pending recording(s)")

        for row in pending:
            try:
                result = process_recording(row)
                print(
                    f"[AutoLoop] ✓ {result['filename']} | "
                    f"status={result.get('status')} | "
                    f"stars={result.get('stars', 0)}/5 | "
                    f"boosted={result.get('volume_boosted', False)}"
                )
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

    return {
        "inserted": cycle_inserted,
        "analyzed": cycle_analyzed,
        "failed":   cycle_failed,
        "errors":   cycle_errors,
    }


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

        cycle = _run_one_cycle(target_date=today)

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

        print(
            f"[AutoLoop] Cycle done — inserted={cycle['inserted']} | "
            f"analyzed={cycle['analyzed']} | failed={cycle['failed']} | next={next_run}\n"
        )
        time.sleep(AUTO_LOOP_INTERVAL_SECONDS)


# ═══════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup():
    create_table()

    try:
        conn = get_pg_conn()
        cur  = conn.cursor()

        # 1. Reset rows wrongly marked not_picked by old disposition filter
        cur.execute("""
            UPDATE call_analysis
            SET status      = 'pending',
                analyzed_at = NULL
            WHERE status    = 'not_picked'
              AND analyzed_at IS NULL
              AND avg_dbfs   IS NULL
              AND location  IS NOT NULL
              AND location  != ''
              AND lower(location) LIKE '%%.mp3'
        """)
        reset_disposition = cur.rowcount

        # 2. Fix rows wrongly saved as 'skipped' when GPT said Not Connected
        cur.execute("""
            UPDATE call_analysis
            SET status = 'not_picked'
            WHERE status     = 'skipped'
              AND stars       = 0
              AND analyzed_at IS NOT NULL
        """)
        reset_skipped = cur.rowcount

        # 3. Remove any WAV-location pending rows inserted by old code
        #    so they get cleanly re-inserted as MP3 next sync cycle
        cur.execute("""
            DELETE FROM call_analysis
            WHERE status = 'pending'
              AND lower(location) LIKE '%%.wav'
        """)
        deleted_wav = cur.rowcount

        # 4. Remove any NULL-location pending rows inserted by old code
        cur.execute("""
            DELETE FROM call_analysis
            WHERE status = 'pending'
              AND (location IS NULL OR location = '')
        """)
        deleted_null = cur.rowcount

        conn.commit()
        cur.close()
        conn.close()

        if reset_disposition > 0:
            print(f"[Startup] Migration: reset {reset_disposition} disposition-filtered row(s) → pending")
        if reset_skipped > 0:
            print(f"[Startup] Migration: fixed {reset_skipped} row(s) skipped → not_picked")
        if deleted_wav > 0:
            print(f"[Startup] Migration: deleted {deleted_wav} WAV-location row(s) — will re-sync as MP3")
        if deleted_null > 0:
            print(f"[Startup] Migration: deleted {deleted_null} NULL-location row(s) — will re-sync")

    except Exception as e:
        print(f"[Startup] Migration warning: {e}")

    threading.Thread(target=auto_sync_analyze_loop, daemon=True).start()
    print("[Startup] Background sync+analyze loop started.")


# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {
        "status":          "ok",
        "version":         "5.5.0",
        "model":           "gpt-4o",
        "today":           today_str(),
        "loop_interval":   f"{AUTO_LOOP_INTERVAL_SECONDS // 60} minutes",
        "pydub_available": PYDUB_AVAILABLE,
        "accepted_format": "MP3 only (WAV skipped until converted)",
        "statuses": {
            "pending":      "Inserted with valid MP3 location, awaiting analysis",
            "not_picked":   "Ringing-only audio OR GPT confirmed not connected/voicemail/IVR",
            "successful":   "Fully transcribed and rated by GPT-4o (stars 1-5)",
            "failed":       "Unrecoverable processing error",
            "no_recording": "Audio file returned 404",
            "skipped":      "Transcript too short (< 20 chars)",
        },
    }


@app.get("/loop-status")
def loop_status():
    with _loop_lock:
        return dict(_loop_state)


@app.get("/status-summary")
def status_summary(date: str = None):
    """Count of each status for a given date."""
    target_date = date or today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT status, COUNT(*) AS cnt
            FROM call_analysis
            WHERE DATE(start_time) = %s
            GROUP BY status ORDER BY cnt DESC
        """, (target_date,))
        rows = cur.fetchall()
        cur.close()
        return {"date": target_date, "summary": {r[0]: r[1] for r in rows}}
    finally:
        conn.close()


@app.get("/debug-sync")
def debug_sync(date: str = None):
    """Show MySQL vs PG counts broken down by file type."""
    target_date = date or today_str()
    result = {"date": target_date}

    try:
        mc = get_mysql_conn()
        with mc.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as total FROM recording_log WHERE DATE(start_time) = %s",
                (target_date,),
            )
            result["mysql_total"] = cur.fetchone().get("total", 0)
            cur.execute("""
                SELECT COUNT(*) as cnt FROM recording_log
                WHERE DATE(start_time) = %s
                  AND location IS NOT NULL AND location != ''
                  AND lower(location) LIKE '%%.mp3'
            """, (target_date,))
            result["mysql_mp3_count"] = cur.fetchone().get("cnt", 0)
            cur.execute("""
                SELECT COUNT(*) as cnt FROM recording_log
                WHERE DATE(start_time) = %s
                  AND location IS NOT NULL AND location != ''
                  AND lower(location) LIKE '%%.wav'
            """, (target_date,))
            result["mysql_wav_count"] = cur.fetchone().get("cnt", 0)
            cur.execute("""
                SELECT COUNT(*) as cnt FROM recording_log
                WHERE DATE(start_time) = %s
                  AND (location IS NULL OR location = '')
            """, (target_date,))
            result["mysql_no_location"] = cur.fetchone().get("cnt", 0)
            cur.execute("""
                SELECT recording_id, filename, start_time, location
                FROM recording_log WHERE DATE(start_time) = %s
                ORDER BY start_time DESC LIMIT 5
            """, (target_date,))
            result["mysql_sample"] = [
                {
                    "recording_id": str(r.get("recording_id")),
                    "filename":     r.get("filename"),
                    "start_time":   str(r.get("start_time")),
                    "location":     r.get("location"),
                }
                for r in cur.fetchall()
            ]
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
        cur.execute(
            "SELECT status, COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s GROUP BY status",
            (target_date,),
        )
        result["pg_status_breakdown"] = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("""
            SELECT COUNT(*) FROM call_analysis
            WHERE DATE(start_time) = %s AND (location IS NULL OR location = '')
        """, (target_date,))
        result["pg_null_location"] = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM call_analysis
            WHERE DATE(start_time) = %s AND lower(location) LIKE '%%.wav'
        """, (target_date,))
        result["pg_wav_location"] = cur.fetchone()[0]
        cur.close()
        pg.close()
        result["pg_status"] = "ok"
    except Exception as e:
        result["pg_status"] = f"ERROR: {e}"

    return result


@app.post("/sync-recordings")
def sync_recordings_endpoint(date: str = None):
    """Manually trigger a full sync for a specific date (default: today)."""
    try:
        result = sync_recording_log(target_date=date)
        return {"message": "Sync complete", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze-all")
def analyze_all_endpoint(date: str = None):
    """Manually trigger analysis of all pending rows for a date (default: today)."""
    target_date = date or today_str()
    rows = get_unanalyzed(target_date=target_date)

    if not rows:
        return {
            "message":   f"No pending recordings for {target_date}.",
            "date":      target_date,
            "processed": 0,
        }

    results, errors = [], []
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
        "errors":    errors,
    }


@app.post("/analyze-one/{filename}")
def analyze_one(filename: str):
    """Manually re-analyze a single recording by filename."""
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


@app.get("/results")
def get_results(
    limit:  int = 50,
    offset: int = 0,
    date:   str = None,
    status: str = None,
):
    """
    List results for a date.
    Optional ?status= filter: pending | not_picked | successful | failed | no_recording | skipped
    """
    target_date = date or today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        base_select = """
            SELECT id, recording_id, channel, server_ip, extension,
                   start_time, start_epoch, end_time, end_epoch,
                   length_in_sec, length_in_min, filename, location,
                   phone_number, lead_id, agent_user, vicidial_id, synced_at,
                   avg_dbfs, volume_boosted,
                   overall_rating, stars, call_outcome, summary,
                   agent_sentiment, client_sentiment,
                   greeting_score, product_knowledge_score,
                   convincing_score, objection_score, clarity_score,
                   empathy_score, closing_score,
                   strengths, improvements, analyzed_at, status
            FROM call_analysis
            WHERE DATE(start_time) = %s
        """

        if status:
            cur.execute(
                base_select + " AND status = %s ORDER BY start_time DESC LIMIT %s OFFSET %s",
                (target_date, status, limit, offset),
            )
        else:
            cur.execute(
                base_select + " ORDER BY start_time DESC LIMIT %s OFFSET %s",
                (target_date, limit, offset),
            )

        rows = [dict(r) for r in cur.fetchall()]
        cur.close()

        for row in rows:
            for f in ["start_time", "end_time", "synced_at", "analyzed_at"]:
                if row.get(f):
                    row[f] = str(row[f])

        return {
            "date":          target_date,
            "status_filter": status,
            "total":         len(rows),
            "data":          rows,
        }
    finally:
        conn.close()


@app.get("/results/{result_id}")
def get_result(result_id: int):
    """Get full detail of one result row, including transcript."""
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