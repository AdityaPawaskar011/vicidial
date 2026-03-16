import os
import re
import json
import tempfile
import requests
import pymysql
import pymysql.cursors
import psycopg2
import psycopg2.extras
from pathlib import Path
from datetime import date

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from openai import OpenAI

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


app = FastAPI(
    title="VICIdial Call Quality Analyzer",
    description="Single table: recording_log + AI analysis results",
    version="3.0.0"
)

SYSTEM_PROMPT = """You are an expert call center quality analyst with 15+ years of experience.
Evaluate the agent's performance on the call and respond ONLY with valid JSON — no markdown, no extra text.

Use this exact format:
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


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def today_str() -> str:
    return date.today().isoformat()


def extract_phone_number(location: str) -> str:
    if not location:
        return None
    match = re.search(r'\d{8}-\d{6}_(\d+)_', location)
    return match.group(1) if match else None


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_DB["host"],
        port=MYSQL_DB["port"],
        user=MYSQL_DB["user"],
        password=MYSQL_DB["password"],
        database=MYSQL_DB["database"],
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4"
    )


def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_DB["host"],
        port=POSTGRES_DB["port"],
        user=POSTGRES_DB["user"],
        password=POSTGRES_DB["password"],
        dbname=POSTGRES_DB["database"]
    )


def filename_exists_in_pg(filename: str) -> bool:
    """
    Duplicate check using filename — always unique, always present.
    recording_id is NOT used for conflict detection anymore.
    """
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
# CREATE TABLE
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# SYNC MySQL → PostgreSQL (TODAY ONLY)
# ─────────────────────────────────────────────

def sync_recording_log(target_date: str = None) -> dict:
    if not target_date:
        target_date = today_str()

    mysql_conn = get_mysql_conn()
    pg_conn    = get_pg_conn()
    inserted   = 0
    skipped    = 0
    errors     = []

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

        pg_cur = pg_conn.cursor()

        for row in rows:
            filename = (row.get("filename") or "").strip()
            rec_id   = str(row.get("recording_id")) if row.get("recording_id") is not None else None

            if not filename:
                errors.append("Row with empty filename skipped")
                skipped += 1
                continue

            # ── Duplicate check by filename ──
            if filename_exists_in_pg(filename):
                skipped += 1
                continue

            phone = extract_phone_number(row.get("location") or "")

            try:
                pg_cur.execute("""
                    INSERT INTO call_analysis (
                        recording_id, channel, server_ip, extension,
                        start_time, start_epoch, end_time, end_epoch,
                        length_in_sec, length_in_min, filename, location,
                        phone_number, lead_id, agent_user, vicidial_id
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON CONFLICT (filename) DO NOTHING
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
                    row.get("lead_id"),
                    row.get("agent_user"),
                    row.get("vicidial_id"),
                ))
                pg_conn.commit()  # commit per row so one failure doesn't block others

                if pg_cur.rowcount > 0:
                    inserted += 1
                    print(f"[Sync] ✓ Inserted: {filename}")
                else:
                    skipped += 1
                    print(f"[Sync] ~ Conflict skipped: {filename}")

            except Exception as e:
                pg_conn.rollback()
                err_msg = f"{filename} → {str(e)}"
                errors.append(err_msg)
                print(f"[Sync ERROR] {err_msg}")

        pg_cur.close()
        print(f"[Sync] Done — Inserted: {inserted} | Skipped: {skipped} | Errors: {len(errors)}")

        return {
            "date":          target_date,
            "inserted":      inserted,
            "skipped":       skipped,
            "error_count":   len(errors),
            "errors":        errors,        # ← real errors now visible!
            "total_fetched": len(rows)
        }

    finally:
        mysql_conn.close()
        pg_conn.close()


# ─────────────────────────────────────────────
# FETCH UNANALYZED ROWS (TODAY ONLY)
# ─────────────────────────────────────────────

def get_unanalyzed( target_date: str = None) -> list:
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
            ORDER BY start_time DESC
            
        """, (target_date,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


# ─────────────────────────────────────────────
# AUDIO PIPELINE
# ─────────────────────────────────────────────

def download_audio(url: str) -> str:
    print(f"[Download] {url}")
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise Exception(f"HTTP {r.status_code} downloading: {url}")
    suffix = Path(url).suffix or ".mp3"
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    tmp.write(r.content)
    tmp.flush()
    tmp.close()
    return tmp.name


def transcribe_audio(file_path: str) -> str:
    with open(file_path, "rb") as f:
        response = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="text"
        )
    return response


def rate_agent(transcript: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.3,
        max_tokens=1500,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Analyze this call transcript and rate the agent's performance.\n"
                    "Focus on convincing ability and objection handling.\n\n"
                    f"--- TRANSCRIPT ---\n{transcript}\n--- END ---"
                )
            }
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
        print(f"[DB] Updated row id={row_id}")
    finally:
        conn.close()


def process_recording(row: dict) -> dict:
    location = (row.get("location") or "").strip()
    filename = row.get("filename", "")
    row_id   = row.get("id")

    if not location:
        raise Exception("No location URL.")

    tmp_path = None  # ← Fix #1: initialize before try
    try:
        tmp_path         = download_audio(location)
        transcript       = transcribe_audio(tmp_path)
        transcript_clean = transcript.strip()

        print(f"[Whisper] {filename} → {len(transcript_clean)} chars")

        if len(transcript_clean) < 20:
            raise Exception(
                f"Transcript too short ({len(transcript_clean)} chars) — "
                "audio is likely silent, corrupt, or a missed call."
            )

        rating = rate_agent(transcript_clean)
        print(f"[GPT-4o]  {filename} → {rating.get('stars')}/5 stars")
        update_analysis(row_id, transcript_clean, rating)  # ← also use clean here

        return {
            "id":               row_id,
            "filename":         filename,
            "stars":            rating.get("stars"),
            "overall_rating":   rating.get("overall_rating"),
            "call_outcome":     rating.get("call_outcome"),
            "summary":          rating.get("summary"),
            "agent_sentiment":  rating.get("agent_sentiment"),
            "client_sentiment": rating.get("client_sentiment"),
            "categories":       rating.get("categories"),
            "strengths":        rating.get("strengths"),
            "improvements":     rating.get("improvements"),
            "transcript":       transcript_clean,  # ← Fix #2: return clean version
        }
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────

@app.on_event("startup")
def startup():
    create_table()


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "table": "call_analysis", "model": "gpt-4o", "today": today_str()}


@app.get("/debug-sync")
def debug_sync(date: str = None):
    """
    Debug: shows what MySQL returns + what PG already has for a given date.
    Run this FIRST to diagnose any sync issues.

    GET /debug-sync
    GET /debug-sync?date=2026-03-05
    """
    target_date = date or today_str()
    result = {"date": target_date}

    # MySQL check
    try:
        mysql_conn = get_mysql_conn()
        with mysql_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM recording_log WHERE DATE(start_time) = %s", (target_date,))
            result["mysql_total"] = cur.fetchone().get("total", 0)

            cur.execute("""
                SELECT recording_id, filename, start_time, location
                FROM recording_log WHERE DATE(start_time) = %s
                ORDER BY start_time DESC 
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
        mysql_conn.close()
        result["mysql_status"] = "ok"
    except Exception as e:
        result["mysql_status"] = f"ERROR: {str(e)}"

    # PostgreSQL check
    try:
        pg_conn = get_pg_conn()
        cur = pg_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s", (target_date,))
        result["pg_total_for_date"] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM call_analysis")
        result["pg_total_all"] = cur.fetchone()[0]

        cur.execute("SELECT status, COUNT(*) FROM call_analysis WHERE DATE(start_time) = %s GROUP BY status", (target_date,))
        result["pg_status_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

        cur.close()
        pg_conn.close()
        result["pg_status"] = "ok"
    except Exception as e:
        result["pg_status"] = f"ERROR: {str(e)}"

    return result


@app.post("/sync-recordings")
def sync_recordings(date: str = None):
    """
    Step 1 — Copy TODAY's recording_log from MySQL into PostgreSQL.
    Duplicate check is by filename. Errors are visible in response.

    POST /sync-recordings
    POST /sync-recordings?date=2026-03-05
    """
    try:
        result = sync_recording_log(target_date=date)
        return {"message": "Sync complete", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze-all")
def analyze_all( date: str = None):
    """
    Step 2 — Analyze TODAY's pending recordings with Whisper + GPT-4o.

    POST /analyze-all
    POST /analyze-all
    POST /analyze-all?date=2026-03-05
    """
    target_date = date or today_str()
    rows = get_unanalyzed( target_date=target_date)

    if not rows:
        return {
            "message":   f"No pending recordings found for {target_date}. Run /sync-recordings first.",
            "date":      target_date,
            "processed": 0
        }

    results = []
    errors  = []

    for row in rows:
        try:
            result = process_recording(row)
            results.append(result)
        except Exception as e:
            try:
                conn = get_pg_conn()
                cur  = conn.cursor()
                cur.execute("UPDATE call_analysis SET status='failed' WHERE id=%s", (row["id"],))
                conn.commit()
                cur.close()
                conn.close()
            except:
                pass
            errors.append({"filename": row.get("filename"), "error": str(e)})
            print(f"[ERROR] {row.get('filename')}: {e}")

    return {
        "date":      target_date,
        "processed": len(results),
        "failed":    len(errors),
        "results":   results,
        "errors":    errors
    }


@app.post("/analyze-one/{filename}")
def analyze_one(filename: str):
    """
    Analyze a single recording by filename.
    Returns 409 if already successfully analyzed.

    POST /analyze-one/20260305-095425_917415576828_8003
    """
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
        raise HTTPException(status_code=409, detail=f"'{filename}' already analyzed. No re-analysis needed.")

    try:
        result = process_recording(row)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/results")
def get_results(limit: int = 50, offset: int = 0, date: str = None):
    """
    View all rows for TODAY (or a given date).

    GET /results
    GET /results?limit=20&offset=0
    GET /results?date=2026-03-05
    """
    target_date = date or today_str()
    conn = get_pg_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                id, recording_id, channel, server_ip, extension,
                start_time, start_epoch, end_time, end_epoch,
                length_in_sec, length_in_min, filename, location,
                phone_number, lead_id, agent_user, vicidial_id, synced_at,
                overall_rating, stars, call_outcome, summary,
                agent_sentiment, client_sentiment,
                greeting_score, product_knowledge_score,
                convincing_score, objection_score, clarity_score,
                empathy_score, closing_score,
                strengths, improvements, analyzed_at, status
            FROM call_analysis
            WHERE DATE(start_time) = %s
            ORDER BY start_time DESC
            
        """, (target_date,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        for row in rows:
            for f in ["start_time", "end_time", "synced_at", "analyzed_at"]:
                if row.get(f):
                    row[f] = str(row[f])
        return {"date": target_date, "total": len(rows), "data": rows}
    finally:
        conn.close()


@app.get("/results/{result_id}")
def get_result(result_id: int):
    """
    Get full details of one row including transcript and all category comments.

    GET /results/1
    """
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
                except:
                    pass
        return row
    finally:
        conn.close()