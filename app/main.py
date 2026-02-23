from fastapi import FastAPI, Form, HTTPException,Query, Request,UploadFile,File,Depends,status,APIRouter, Depends
from jose import JWTError, jwt
import mysql.connector
from mysql.connector import Error
from datetime import datetime,date,timedelta
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from datetime import datetime, date, timezone
from pydantic import BaseModel
from psycopg2.extras import RealDictCursor
import pymysql
import time
import requests
import pandas as pd
import io 
from fastapi.encoders import jsonable_encoder
from typing import Optional
from psycopg2 import pool
import asyncio
from pydantic import BaseModel
from typing import List
import re

SECRET_KEY = "41b2ae40f9299813102265496f77665b12163f2386d4fc3ec7a8bcfa4ec56931"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


app = FastAPI()

DB_CONFIG = {
    "host": "192.168.15.165",
    "user": "cron",
    "password": "1234",
    "database": "asterisk"
}
# WHITELISTED_IPS = {
#     "192.168.15.104:5000"
# }
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://192.168.15.104:5000",  # frontend origin (WITH port)
        "http://localhost:5000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        is_admin = payload.get("isAdmin", False)
        campaign_name = payload.get("campaign_name")
        campaign_id = payload.get("campaign_id")

        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        return {
            "username": username,
            "isAdmin": is_admin,
            "campaign_name":campaign_name,
            "campaign_id":campaign_id

        }

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired or invalid"
        )


# @app.middleware("http")
# async def ip_whitelist_middleware(request, call_next):
#     client_ip = request.client.host

#     if client_ip not in WHITELISTED_IPS:
#         raise HTTPException(status_code=403, detail="IP not allowed")

#     return await call_next(request)

# @app.on_event("startup")
# def startup_event():
#     global db_connection
#     try:
#         db_connection = mysql.connector.connect(**DB_CONFIG)
            
#         print("✅ Database connected")
#     except Error as e:
#         print("❌ Database connection failed:", e)

# @app.on_event("shutdown")
# def shutdown_event():
#     global db_connection
#     if db_connection and db_connection.is_connected():
#         db_connection.close()
#         print("🛑 Database connection closed")

#Calls Waiting
@app.get('/getcallswaiting')
def get_waitingcalls():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """SELECT u.user AS agent_id, u.full_name AS agent_name, vla.campaign_id,vla.status,vla.calls_today AS calls_handled
                    FROM vicidial_live_agents vla left JOIN vicidial_users u ON vla.user = u.user ORDER BY vla.status, vla.calls_today DESC;
                    """ 
        
        
                    # CASE WHEN vla.status = 'INCALL' THEN SEC_TO_TIME(UNIX_TIMESTAMP(NOW()) )
                    #     ELSE '00:00:00' END AS talk_time,
                    # CASE WHEN vla.status = 'READY' THEN SEC_TO_TIME(UNIX_TIMESTAMP(NOW()))
                    #     ELSE '00:00:00' END AS wait_time
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close() 
        conn.close() 
        return { "count": len(result), "data": result }

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))

def format_time(seconds):
    if seconds is None:
        return "00:00:00"
    return time.strftime('%H:%M:%S', time.gmtime(int(seconds)))

#Agents Time on Call Campaign
@app.get('/getagnetstimeoncall')
def get_waitingcalls():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """
                    SELECT extension  AS STATION,user AS USER,status AS STATUS,calls_today AS CALLS,
                    (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(last_state_change))  AS TALK_TIME_SECONDS
                    FROM vicidial_live_agents
                    ORDER BY status;""" 
        cursor.execute(query)
        result = cursor.fetchall()
        for row in result:
            seconds = row.get("TALK_TIME_SECONDS", 0)
            row["TALK_TIME_HH_MM_SS"] = format_time(seconds)

        cursor.close()
        conn.close() 
        return { "count": len(result), "data": result }

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/get_all_data')
def get_all_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        # today_start = datetime.combine(date.today(), datetime.min.time())
        today_start = date.today()

        # Dialer Level
        query_dialer_level = """SELECT avg(auto_dial_level) AS dialer_level FROM vicidial_campaigns """
        cursor.execute(query_dialer_level)
        dialer_level_result = cursor.fetchall()
        #get_all_data['dialer_level_result'] = cursor.fetchall()['dialer_level_result']

        # Dialable Leads
        query_dialable_leads = """SELECT COUNT(*) AS dialable_leads 
                                  FROM vicidial_list vl 
                                  JOIN vicidial_lists vli ON vl.list_id = vli.list_id 
                                  WHERE vli.active = 'Y' AND vl.called_since_last_reset = 'N'"""
        cursor.execute(query_dialable_leads)
        dialable_leads_result = cursor.fetchall()

        # Hopper Min/Max
        query_hopper_min_max = """SELECT sum(hopper_level) AS min_hopper  FROM vicidial_campaigns """
        cursor.execute(query_hopper_min_max)
        hopper_min_max_result = cursor.fetchall()

        # Leads in Hopper
        query_leads_in_hopper = """SELECT COUNT(*) AS leads_in_hopper FROM vicidial_hopper WHERE status = 'READY'"""
        cursor.execute(query_leads_in_hopper)
        leads_in_hopper_result = cursor.fetchall()

        # Trunk Short/Fill
        query_trunk_short_fill = """SELECT GREATEST(COUNT(vh.lead_id) - (vc.auto_dial_level * COUNT(DISTINCT vla.user)),0) AS trunk_fill,
                                    GREATEST((vc.auto_dial_level * COUNT(DISTINCT vla.user)) - COUNT(vh.lead_id),0) AS trunk_short
                                    FROM vicidial_campaigns vc 
                                    LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id
                                    LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id 
                                    WHERE vc.active = 'Y'"""
        cursor.execute(query_trunk_short_fill)
        trunk_short_fill_result = cursor.fetchall()

        # Calls Today
        query_calls_today = """SELECT COUNT(*) AS calls_today FROM vicidial_log WHERE date(call_date) = %s"""
        cursor.execute(query_calls_today, (today_start,))
        calls_today_result = cursor.fetchall()

        # Dropped / Answered
        # query_drop_answer = """SELECT (SELECT COUNT(*) FROM vicidial_log WHERE status NOT IN ('DROP', 'DC', 'NA')) AS count_non_drop_dc_na,
        #                        (SELECT COUNT(*) FROM vicidial_log WHERE status IN ('DROP', 'DC')) AS count_drop_dc
        #                        WHERE call_date >= %s"""
        # cursor.execute(query_drop_answer, (today_start,))
        # drop_answer_result = cursor.fetchall()

        # Drop Percentage
        query_drop_percent = """SELECT ROUND((SUM(IF(status IN ('DROP','DC'),1,0)) / COUNT(*)) * 100, 2) AS dropped_percent
                               FROM vicidial_log WHERE date(call_date) = %s"""
        cursor.execute(query_drop_percent, (today_start,))
        drop_percent_result = cursor.fetchall()

        # Avg Agent
        query_avg_agent = """SELECT COUNT(*) AS agents FROM vicidial_live_agents"""
        cursor.execute(query_avg_agent)
        avg_agent_result = cursor.fetchall()

        # DL Diff
        query_dl_diff = """SELECT ROUND(vc.auto_dial_level - (COUNT(vh.lead_id) / COUNT(vla.user)),2) AS dl_diff 
                          FROM vicidial_campaigns vc
                          LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id
                          LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id"""
        cursor.execute(query_dl_diff)
        dl_diff_result = cursor.fetchall()

        # Diff
        query_diff = """SELECT ROUND(((vc.auto_dial_level - (COUNT(vh.lead_id) / NULLIF(COUNT(vla.user), 0))) / vc.auto_dial_level) * 100, 2) AS diff_percent
                        FROM vicidial_campaigns vc 
                        LEFT JOIN vicidial_hopper vh ON vc.campaign_id = vh.campaign_id
                        LEFT JOIN vicidial_live_agents vla ON vc.campaign_id = vla.campaign_id"""
        cursor.execute(query_diff)
        diff_result = cursor.fetchall()

        # Dial Method
        query_dial_method = """SELECT dial_method FROM vicidial_campaigns order by dial_method asc limit 1"""
        cursor.execute(query_dial_method)
        dial_method_result = cursor.fetchall()

        # Status
        query_status = """SELECT status FROM vicidial_campaign_statuses;"""
        cursor.execute(query_status)
        status_result = cursor.fetchall()

        # Order
        query_order = """SELECT Distinct lead_order FROM vicidial_campaigns"""
        cursor.execute(query_order)
        order_result = cursor.fetchall()

        # Closing DB connection
        cursor.close()
        conn.close()

        # Returning the consolidated results
        return {
            "dialer_level": dialer_level_result[0]["dialer_level"] if dialer_level_result else None,
            "dialable_leads": dialable_leads_result[0]["dialable_leads"] if dialable_leads_result else 0,

            "hopper_min_max": hopper_min_max_result[0]["min_hopper"] if hopper_min_max_result else None,

            # "leads_in_hopper": leads_in_hopper_result[0]["leads_in_hopper"] if leads_in_hopper_result else 0,

            "trunk_short_fill": trunk_short_fill_result[0] if trunk_short_fill_result else {
                "trunk_fill": 0,
                "trunk_short": 0
            },
            "calls_today": calls_today_result[0]["calls_today"] if calls_today_result else 0,

            # "drop_answer": drop_answer_result[0] if drop_answer_result else None,

            # "drop_percent": drop_percent_result[0]["dropped_percent"] if drop_percent_result else 0,

            "avg_agent": avg_agent_result[0]["agents"] if avg_agent_result else 0,
            "dl_diff": dl_diff_result[0]["dl_diff"] if dl_diff_result else 0,
            "diff_percent": diff_result[0]["diff_percent"] if diff_result else 0,
            "dial_method": dial_method_result[0]["dial_method"] if dial_method_result else None,

            # "status": status_result,

            "order": order_result[0]["lead_order"] if order_result else None
        }



    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get('/getcallbystatus')
def get_calls_by_status( current_user: str = Depends(get_current_user)):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        today_start = date.today()

        query = """
           SELECT 
                COUNT(CASE WHEN status='INCALL' THEN 1 END) AS Incall,
                COUNT(CASE WHEN status='PAUSED' THEN 1 END) AS Paused,
                COUNT(CASE WHEN status='READY' THEN 1 END) AS Ready,
                (
                    SELECT COUNT(*) 
                    FROM vicidial_log 
                    WHERE DATE(call_date) = %s
                ) AS Totalcall
            FROM vicidial_live_agents;
        """

        cursor.execute(query, (today_start,))
        result = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "count": len(result),
            "data": result
        }

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))


def seconds_to_hhmmss(seconds):
    seconds = int(seconds or 0)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:02}"


#Total Dials Today
@app.get('/totaldialstoday')
def get_totaldials(request:Request, current_user: dict = Depends(get_current_user)):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        today_start = date.today()
        sd = request.query_params.get("sd")
        ed = request.query_params.get("ed")

        print("cuerrrrr --- ", current_user)

        user_id = current_user["username"]
        is_admin = current_user["isAdmin"]
        campaign_id = current_user["campaign_id"]
        campaign_name = current_user["campaign_name"]

        userfitler = f" and v.user='{user_id}' and v.campaign_id='{campaign_id}' " if not is_admin else ""
 
        query  = f""" select total_dials,connected_calls,connection_rate_pct,total_talk_time,avg_talk_time_sec,leads_connected,sum(total_seconds)total_seconds from (
                        SELECT
                        DATE(v.call_date) AS call_date,
                        (select count(*)Total_calls from vicidial_log v where date(call_date)  BETWEEN %s AND %s {userfitler}) AS total_dials,
                        (select count(*) from vicidial_log v where date(call_date)  BETWEEN %s AND %s  and length_in_sec > 0 {userfitler}) AS connected_calls,
                        ROUND((SUM(v.length_in_sec > 0) / COUNT(*)) * 100, 2 )  AS connection_rate_pct,
                        (SELECT SUM(length_in_sec) FROM vicidial_log v  WHERE DATE(call_date) BETWEEN %s AND %s {userfitler}) AS total_talk_time,
                        (select AVG(length_in_sec) from vicidial_log v where date(call_date) BETWEEN %s AND %s and length_in_sec >0 {userfitler} ) AS avg_talk_time_sec,
                        COUNT(distinct v.lead_id) AS leads_connected,
                        SUM(length_in_sec) AS total_seconds
                    FROM vicidial_log v 
                    where date(v.call_date)  BETWEEN %s AND %s {userfitler} GROUP BY DATE(v.call_date))a ;
                    """
 
        cursor.execute(query,(f"{sd}",f"{ed}",f"{sd}",f"{ed}",f"{sd}",f"{ed}",f"{sd}",f"{ed}",f"{sd}",f"{ed}",))
     

        result = cursor.fetchall()
        for row in result:
            row["total_talk_time"] = seconds_to_hhmmss(row["total_talk_time"])
        cursor.close()
        conn.close()
        return {"data":result}
    except Error as e:
        print(e)
        raise HTTPException(status_code=500,detail=str(e))

#Dialer Performance 
@app.get('/dialerperformance')
def get_dialerperformance(current_user: str = Depends(get_current_user)):
    try:    
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        today_start = date.today()
        query  = """ SELECT
                        ROUND(COUNT(*) / NULLIF(
                                (SELECT COUNT(DISTINCT user) FROM vicidial_live_agents), 0), 2) AS dial_level,
                        ROUND((COUNT(*) / NULLIF((SELECT COUNT(DISTINCT user) FROM vicidial_live_agents), 0)) / 24, 2) AS calls_per_agent_per_hour,
                        (
                            SELECT  AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_call_length
                            FROM call_log
                            WHERE DATE(start_time) = %s
                        ) AS avg_answer_speed_sec,
                        ROUND(SUM(vl.status IN ('DROP','ABANDON')) * 100.0 /NULLIF(
                        (SELECT COUNT(*) FROM vicidial_dial_log WHERE DATE(call_date) = %s), 0), 2) AS drop_rate_percent,
                        ROUND(AVG(vl.length_in_sec), 2) AS avg_call_length,
                        ROUND((SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_percent
                    FROM vicidial_log vl
                    WHERE DATE(vl.call_date) = %s;
                    """
        cursor.execute(query,(today_start,today_start,today_start,))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"data":result}
    except Error as e:
        raise HTTPException(status_code=500,detail=str(e))


#Agent Productivity
@app.get('/agentsproductivity')
def get_agentsproductivity(request:Request, current_user: str = Depends(get_current_user)):
    try:    
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        sd = request.query_params.get("sd")
        ed = request.query_params.get("ed")

        # if sd and ed :

        today_start = date.today()
        query  = """ SELECT  CONCAT('SIP/',vl.user) AS STATION,vl.user AS USER_ID,
                        vu.full_name AS USER_NAME,vla.status AS STATUS,
                        COUNT(*) AS CALLS,SUM(vl.length_in_sec > 0) AS connected_calls,
                        MAX(vl.phone_number) AS phone_number,SEC_TO_TIME(IFNULL(al.login_seconds,0)) AS login_duration,
                        (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(vla.last_state_change)) AS TALK_TIME_SECONDS
                        FROM vicidial_log vl
                        LEFT JOIN vicidial_users vu  ON vl.user = vu.user
                        LEFT JOIN vicidial_live_agents vla ON vl.user = vla.user    
                        LEFT JOIN (
                            SELECT user,SUM(pause_sec + wait_sec + talk_sec + dispo_sec) AS login_seconds
                            FROM vicidial_agent_log WHERE DATE(event_time) BETWEEN %s AND %s
                            GROUP BY user
                        ) al  ON vl.user = al.user
                        WHERE DATE(vl.call_date) BETWEEN %s AND %s
                        GROUP BY vl.user, vl.campaign_id
                        ORDER BY vl.user;
                        """
        
        cursor.execute(query,(f"{sd}",f"{ed}",f"{sd}",f"{ed}",))
        result = cursor.fetchall()
        for row in result:
            seconds = row.get("TALK_TIME_SECONDS", 0)
            row["TALK_TIME_HH_MM_SS"] = format_time(seconds)
            if hasattr(row["login_duration"], "total_seconds"):
                row["login_duration"] = seconds_to_hhmmss(
                    row["login_duration"].total_seconds()
                )
        cursor.close()
        conn.close()
        return {"data":result}
    except Error as e:
        raise HTTPException(status_code=500,detail=str(e))


#Agent Productivity
@app.get('/campaignperformance')
def get_campaignperformance(request:Request,current_user: str = Depends(get_current_user)):
    try:    
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        sd = request.query_params.get("sd")
        ed = request.query_params.get("ed")
        today_start = date.today()

        query  = """ SELECT vl.campaign_id,COUNT(*) AS total_dials,SUM(vl.length_in_sec > 0) AS connected_calls,
                        ROUND( (SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2 ) AS connection_rate_pct,
                        SEC_TO_TIME(SUM(vl.length_in_sec)) AS total_talk_time,
                        AVG(vl.length_in_sec) AS avg_talk_time,
                        ROUND( (SUM(vl.status = 'DROP') / COUNT(*)) * 100, 2 ) AS drop_rate_pct,
                        SUM(vl.status IN ('SALE','SUCCESS','CONVERTED')) AS conversions FROM vicidial_log vl WHERE DATE(vl.call_date) between %s and %s  GROUP BY vl.campaign_id
                        ORDER BY total_dials DESC; """
                                            
        cursor.execute(query,(f"{sd}",f"{ed}",))
        result = cursor.fetchall()
        for row in result:
            if hasattr(row["total_talk_time"], "total_seconds"):
                row["total_talk_time"] = seconds_to_hhmmss(
                    row["total_talk_time"].total_seconds()
                )

            # if hasattr(row["avg_talk_time"], "total_seconds"):
            #     row["avg_talk_time"] = seconds_to_hhmmss(
            #         row["avg_talk_time"].total_seconds()
            #     )

        
        cursor.close()
        conn.close()
        return {"data":result}
    except Error as e:
        raise HTTPException(status_code=500,detail=str(e))



#Compliance Review
@app.get('/compliancereview')
def get_compliancereview( current_user: str = Depends(get_current_user)):
    try:    
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        today_start = date.today()
        query  = """ SELECT distinct (SELECT COUNT(*) FROM vicidial_dnc_log dnc) AS dnd_violations,
                        (SELECT COUNT(*) FROM vicidial_callbacks cb WHERE cb.callback_time >=%s) AS callback_sla_raised,
                        (SELECT dial_method FROM vicidial_campaigns order by dial_method asc limit 1) AS dial_method,
                        (SELECT sum(hopper_level) AS min_hopper  FROM vicidial_campaigns) AS hooper_level,
                        (SELECT COUNT(*) FROM vicidial_callbacks cb WHERE cb.callback_time < %s AND cb.status != 'COMPLETE'
                        ) AS callback_sla_missed,
                        (SELECT CASE WHEN COUNT(*) > 10 THEN 'HIGH'
                                    WHEN COUNT(*) BETWEEN 5 AND 10 THEN 'MEDIUM'
                                    ELSE 'LOW'
                                END FROM vicidial_dnc_log dnc
                        ) AS risk_level
                    FROM vicidial_log vl WHERE vl.call_date >= %s;"""
                                            
        cursor.execute(query,(today_start,today_start,today_start,))
        result = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return {"data":result}
    except Error as e:
        raise HTTPException(status_code=500,detail=str(e))

#Lead Funnel
@app.get('/leadfunnel')
def get_LeadFunnel(request:Request, current_user: str = Depends(get_current_user)):
    try:
        sd = request.query_params.get("sd")
        ed = request.query_params.get("ed")
        
        print(sd, ed,"------------------1")
    # start_date, end_date = resolve_date_range(sd, ed)

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if sd and ed:
            # 🔹 Single date
            query = """
                SELECT
                    COUNT(*) AS dialed,
                    SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                    SUM(length_in_sec > 0) AS connected,
                    SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                    SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log
                WHERE date(call_date) BETWEEN %s AND %s
            """
            params = (
                f"{sd}",
                f"{ed}"
            )
        else:
            today_start = date.today()
            # 🔹 Date range
            query = """
                SELECT
                    COUNT(*) AS dialed,
                    SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                    SUM(length_in_sec > 0) AS connected,
                    SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                    SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log
                WHERE date(call_date)= %s
            """
            params = (
                f"{today_start}",
                
            )

        cursor.execute(query, params)
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#Hourly Performance 
@app.get('/hourlyperformance')
def get_LeadFunnel( current_user: str = Depends(get_current_user)):
    try:    
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        today_start = date.today()
        query  = """ SELECT
                        HOUR(call_date) AS hour,
                        COUNT(*) AS total_calls,
                        SUM(length_in_sec > 0) AS connected_calls
                        FROM vicidial_log
                        WHERE DATE(call_date)=%s
                        GROUP BY HOUR(call_date)
                        ORDER BY hour; """
                                            
        cursor.execute(query,(today_start,))
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        hours = []
        total_calls =[]
        connected_calls =[] 
        for row in result:
            hours.append(row["hour"])
            total_calls.append(row["total_calls"])
            connected_calls.append(row["connected_calls"])
        return {
            "hours":hours,
            "total_calls":total_calls,
            "connected_calls":connected_calls
        }
    except Error as e:
        raise HTTPException(status_code=500,detail=str(e))
    

#Past Data For Graphs
@app.get('/graphdata')
def get_GraphData( current_user: str = Depends(get_current_user)):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DATE_FORMAT(vl.call_date, '%H:%i') AS time_slot,
            ROUND((SUM(vl.length_in_sec > 0) / COUNT(*)) * 100, 2) AS connection_rate_percentage,
            ROUND(SUM(vl.status IN ('DROP','ABANDON')) * 100.0 /NULLIF(
                        (SELECT COUNT(*)
                         FROM vicidial_dial_log vdl
                         WHERE vdl.call_date BETWEEN NOW() - INTERVAL 2 HOUR AND NOW()
                        ), 0
                    ), 2
                ) AS drop_rate_percentage
            FROM vicidial_log vl
            WHERE vl.call_date BETWEEN NOW() - INTERVAL 2 HOUR AND NOW()
            GROUP BY FLOOR(UNIX_TIMESTAMP(vl.call_date) / 720)
            ORDER BY time_slot DESC
            LIMIT 10;
        """

        cursor.execute(query)
        result = cursor.fetchall()
        time_slot = []
        connection_rate_percentage = []
        drop_rate_percentage = []
        for row in result:
            time_slot.append(row["time_slot"])
            connection_rate_percentage.append(row["connection_rate_percentage"])
            drop_rate_percentage.append(row["drop_rate_percentage"])

        cursor.close()
        conn.close()

        return {"time_slot": time_slot,"connection_rate_percentage":connection_rate_percentage,"drop_rate_percentage":drop_rate_percentage}

    except Error as e:
        raise HTTPException(status_code=500, detail=str(e))




def resolve_date_range(sd=None, ed=None):
    if sd and ed:
        start_date = datetime.strptime(sd, "%Y-%m-%d")
        end_date = datetime.strptime(ed, "%Y-%m-%d") + timedelta(days=1)
    elif sd:  # single date selected
        start_date = datetime.strptime(sd, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
    else:  # today (default)
        start_date = datetime.combine(date.today(), datetime.min.time())
        end_date = datetime.now()

    return start_date, end_date

# @app.get('/totaldialstodaydata')
# def get_totaldialsdata(request: Request, current_user: str = Depends(get_current_user)):
#     try:
#         sd = request.query_params.get("sd")
#         ed = request.query_params.get("ed")

#         start_date, end_date = resolve_date_range(sd, ed)

#         conn = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor(dictionary=True)

#         query = """ SELECT
#                         DATE(d.call_date) AS call_date,
#                         (select count(*)Total_calls from vicidial_log where date(call_date) BETWEEN %s AND %s) AS total_dials,
#                         (select count(*) from vicidial_log where date(call_date) BETWEEN %s AND %s  and length_in_sec > 0) AS connected_calls,
#                         ROUND((SUM(v.length_in_sec > 0) / COUNT(*)) * 100, 2 )  AS connection_rate_pct,
#                         (SELECT SUM(length_in_sec) FROM vicidial_log  WHERE DATE(call_date)BETWEEN %s AND %s) AS total_talk_time,
#                         AVG(v.length_in_sec) AS avg_talk_time_sec,
#                         COUNT(distinct v.lead_id) AS leads_connected,
#                         SUM(length_in_sec) AS total_seconds
#                     FROM vicidial_dial_log d
#                     LEFT JOIN vicidial_log v ON d.lead_id = v.lead_id AND d.call_date = v.call_date AND v.length_in_sec > 0
#                     where date(d.call_date) BETWEEN %s AND %s GROUP BY DATE(d.call_date) ;

#         """


#         cursor.execute(query, (start_date, end_date,start_date,end_date,start_date,end_date,start_date,end_date,))
#         result = cursor.fetchall()

#         for row in result:
#             row["total_talk_time"] = seconds_to_hhmmss(row["total_talk_time"])

#         cursor.close()
#         conn.close()

#         return {"data": result}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@app.get('/leadfunnelwithdate')
def get_LeadFunnel(request: Request, current_user: str = Depends(get_current_user)):
    try:
        sd = request.query_params.get("sd")
        ed = request.query_params.get("ed")
        
        print(sd, ed,"------------------150")
    # start_date, end_date = resolve_date_range(sd, ed)

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        if sd and ed:
            # 🔹 Single date
            query = """
                SELECT
                    COUNT(*) AS dialed,
                    SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                    SUM(length_in_sec > 0) AS connected,
                    SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                    SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log
                WHERE date(call_date) BETWEEN %s AND %s
            """
            params = (
                f"{sd}",
                f"{ed}"
            )
        else:
            today_start = date.today()
            # 🔹 Date range
            query = """
                SELECT
                    COUNT(*) AS dialed,
                    SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
                    SUM(length_in_sec > 0) AS connected,
                    SUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
                    SUM(status IN ('EC')) AS existing_clients
                FROM vicidial_log
                WHERE date(call_date)= %s
            """
            params = (
                f"{today_start}",
                
            )

        cursor.execute(query, params)
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        return {"data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#preveous code 
    # try:    
    #     conn = mysql.connector.connect(**DB_CONFIG)
    #     cursor = conn.cursor(dictionary=True)
    #     today_start = date.today()
    #     query  = """ SELECT COUNT(*) AS dialed,SUM(status IN ('IN','CBR','INTEREST','I')) AS Interested,
    #                     SUM(length_in_sec > 0) AS connected,
    #                     (SELECT COUNT(*) FROM vicidial_list WHERE status IN ('QUALIFIED','QCOK')) AS qualified,
    #                     sUM(status IN ('CON','SUCCESS','CONVERTED')) AS converted,
    #                     sum(status IN ('EC')) AS existing_clients
    #                     FROM vicidial_log; """
                                            
    #     cursor.execute(query,)
    #     result = cursor.fetchall()
    #     cursor.close()
    #     conn.close()
    #     return {"data":result}
    # except Error as e:
    #     raise HTTPException(status_code=500,detail=str(e))
    


# Add leads 



def normalize_phone(phone):
    return str(phone).strip()[-10:]

def load_existing_phones():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT distinct phone_number FROM vicidial_list  WHERE phone_number IS NOT NULL")
    phones = set()
    for (phone,) in cursor.fetchall():
        cleaned = clean_phone(phone)
        if cleaned:
            phones.add(cleaned)

    cursor.close()
    conn.close()
    return phones


vicidial_url = "http://192.168.15.165/vicidial/non_agent_api.php"

vici_user = 'AdminR'
Vici_pass = 'AdminR'
SOURCE = "FASTAPI"

# @app.post("/upload_excel_leads")
# def upload_excel_leads(file: UploadFile =File(...)):
#     if not file.filename.endswith(".xlsx"):
#         raise HTTPException(status_code=400 , detail="only .xlsx file allowed")

#     try:
#         contents = file.file.read()
#         df = pd.read_excel(io.BytesIO(contents))
#         print('file read-----------------')
#     except Exception as e:
#         raise HTTPException(status_code=400 , detail=f"invalid excel file : {e}")
    
#     required_columns = {"phone_number","list_id"}
#     if not required_columns.issubset(df.columns):
#         raise HTTPException(
#             status_code=400, detail=f"Excel must contain columns : {required_columns}"
#         )
    

#     existing_phones = load_existing_phones()

#     success = 0
#     failed = []

#     for index,row in df.iterrows():
#         phone = normalize_phone(row["phone_number"])

#         if phone in existing_phones:
#             failed.append({
#                 "row": index + 2,
#                 "phone": phone,
#                 "error": "Duplicate phone (already exists)"
#             })
#             continue
#         params={
#             "source": SOURCE,
#             "user" : vici_user,
#             "pass" : Vici_pass,
#             "function" : "add_lead",
#             "phone_number" : row['phone_number'],
#             "phone_code" : "1",
#             "list_id" : row["list_id"],
#             "first_name" : str(row.get("first_name","")),
#             "last_name" : str(row.get("last_name","")),
#             "campaign_id" : str(row.get("campaign_id",""))
#         }

#         try: 
#             response = requests.get(vicidial_url,params=params ,timeout=10)

#             if "SUCCESS" in response.text.upper():
#                 success +=1 
#             else:
#                 failed.append({
#                     "row": index +2,
#                     "phone": row["phone_number"],
#                     "error" : response.text
#                 })

#         except Exception as e: 
#             failed.append({
#                 "row" : index+2,
#                 "phone" : row["phone_number"],
#                 "error" : str(e)
#             })

#     return {
#         "total_records": len(df),
#         "success": success,
#         "failed": len(failed),
#         "failed_details": failed
#     }


#leads
@app.get("/leads")
def get_leads(request: Request,  current_user: str = Depends(get_current_user)):
    try:
        sd = request.query_params.get("sd")  # YYYY-MM-DD
        ed = request.query_params.get("ed")  # YYYY-MM-DD
        limit = int(request.query_params.get("limit", 50))

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        
        if sd and ed:
            query = """
            SELECT
                DATE(vl.entry_date) AS entry_date,
                vl.lead_id,
                vl.phone_number,
                vl.first_name,
                vl.last_name,
                vl.status,
                vl.list_id,
                vls.campaign_id,
                vl.user
            FROM vicidial_list vl JOIN vicidial_lists vls ON vl.list_id = vls.list_id
            WHERE date(vl.entry_date) between  %s AND  %s
            ORDER BY vl.entry_date DESC
            LIMIT %s """
            params = (sd, ed, limit)
        else:
            today_start = date.today()
            query = """
                SELECT
                    DATE(vl.entry_date) AS entry_date,
                    vl.lead_id,
                    vl.phone_number,
                    vl.first_name,
                    vl.last_name,
                    vl.status,
                    vl.list_id,
                    vls.campaign_id,
                    vl.user
                FROM vicidial_list vl JOIN vicidial_lists vls ON vl.list_id = vls.list_id
                WHERE date(vl.entry_date) = %s
                ORDER BY vl.entry_date DESC
                LIMIT %s """

            params = (today_start, limit)

        cursor.execute(query, params)
        data = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "limit": limit,
            "count": len(data),
            "leads": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

#validate list with campaign 
def validate_list_campaign(list_id, campaign_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        SELECT 1
        FROM vicidial_lists
        WHERE list_id = %s
          AND campaign_id = %s
          AND active = 'Y'
        LIMIT 1
    """
    cursor.execute(query, (list_id, campaign_id))
    valid = cursor.fetchone() is not None

    cursor.close()
    conn.close()

    return valid

#upload_excel_leads
@app.post("/upload_excel_leads")
def upload_excel_leads(
    campaign_id: str = Form(...),
    campaign_name: str = Form(...),
    file: UploadFile = File(...),  current_user: str = Depends(get_current_user)):
   

    # if not file.filename.endswith(".xlsx"):
    #     raise HTTPException(status_code=400, detail="Only .xlsx file allowed")\
        
    if not (file.filename.endswith(".xlsx") or file.filename.endswith(".csv")):
        raise HTTPException(
            status_code=400,
            detail="Only .xlsx or .csv files allowed"
        )


    try:
        contents = file.file.read()
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contents), dtype=str, encoding="utf-8")
        else:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
        print(df)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid excel file: {e}")

    # Normalize columns
    df.columns = df.columns.str.strip().str.lower()

    # Remove blank rows
    df.dropna(how="all", inplace=True)

    # Replace NaN
    df.fillna("", inplace=True)

    required_columns = {"phone_number", "list_id"}
    if not required_columns.issubset(df.columns):
        raise HTTPException(
            status_code=400,
            detail=f"Excel must contain columns: {required_columns}"
        )

    existing_phones = load_existing_phones()
    # print("DB phones sample:", list(existing_phones))
    print("Total DB phones:", len(existing_phones))

    success = 0
    failed = []
    skipped = []
    not_valid =[]

    for index, row in df.iterrows():
        excel_row = index + 2

        phone = clean_phone(row.get("phone_number"))
        list_id = str(row.get("list_id")).strip()
        # ✅= VALIDATE list belongs to selected campaign
        if not validate_list_campaign(list_id, campaign_id):
            not_valid.append({
                "row": excel_row,
                "list_id": list_id,
                "reason": f"List {list_id} does not belong to campaign {campaign_id}"
            })
            continue



        if not phone or not list_id:
            skipped.append({
                "row": excel_row,
                "reason": "Phone number or list_id missing"
            })
            continue
        

        if not phone.isdigit():
            skipped.append({
                "row": excel_row,
                "phone": phone,
                "reason": "Invalid phone number"
            })
            continue

        if phone in existing_phones:
            skipped.append({
                "row": excel_row,
                "phone": phone,
                "reason": "Duplicate phone"
            })
            continue

        

        params = {
            "source": SOURCE,
            "user": vici_user,
            "pass": Vici_pass,
            "function": "add_lead",
            "phone_number": phone,
            "phone_code": "1",
            "list_id": list_id,
            "first_name": str(row.get("first_name", "")).strip(),
            "last_name": str(row.get("last_name", "")).strip(),
            # "campaign_id": str(row.get("campaign_id", "")).strip(),
        }

        try:
            response = requests.get(vicidial_url, params=params, timeout=10)

            if "SUCCESS" in response.text.upper():
                success += 1
        
        
        
                existing_phones.add(phone)
            else:
                failed.append({
                    "row": excel_row,
                    "phone": phone,
                    "error": response.text
                })

        except Exception as e:
            failed.append({
                "row": excel_row,
                "phone": phone,
                "error": str(e)
            })

    return {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "total_rows": len(df),
        "success": success,
        "failed": len(failed),
        "skipped": len(skipped),
        "failed_details": failed,
        "skipped_details": skipped,
        "list_and_campaign":not_valid
    }


def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({
        "exp": datetime.utcnow() + timedelta(minutes=1440)
    })
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)



#Login
class LoginRequest(BaseModel):
    username: str
    password: str
    campaign_id: str
    campaign_name: str
    role :str

@app.post("/login")
def login(data: LoginRequest):
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()
        if data.role == 'Agent':
            query = """
                SELECT vu.user, full_name,vu.active, user_level,vca.campaign_id, vc.campaign_name
                FROM vicidial_users vu join vicidial_campaign_agents vca on vu.user=vca.user
                left join vicidial_campaigns vc on vc.campaign_id =vca.campaign_id 
                WHERE vu.user=%s AND pass=%s AND vca.campaign_id=%s AND vc.campaign_name=%s AND vc.Active='Y' AND vu.active='Y' and vu.user_level <> 9
                LIMIT 1
            """

            cursor.execute(query, (data.username, data.password,data.campaign_id,data.campaign_name))
            user = cursor.fetchone()
            print(user,"-----------------150")

            cursor.close()
            db.close()
        else :
            query = """
                SELECT vu.user, full_name,vu.active, user_level
                FROM vicidial_users vu 
                WHERE vu.user=%s AND pass=%s  AND vu.active='Y' and vu.user_level <= 9
                LIMIT 1
            """

            cursor.execute(query, (data.username, data.password,))
            user = cursor.fetchone()
            print(user,"-----------------150")

            cursor.close()
            db.close()

        if not user:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        if user[2] != "Y":
            raise HTTPException(status_code=403, detail="User is inactive")
        
        access_token = create_access_token(
            data={"sub": user[0],"isAdmin": user[3] == 9,"campaign_id": data.campaign_id,"campaign_name": data.campaign_name}   # MUST BE "sub"
        )

        return {
            "status": "success",
            "user": user[0],
            "full_name": user[1],
            "access_token": access_token,
            "isAdmin": user[3] == 9,
            "campaign_id": data.campaign_id,
            "campaign_name": data.campaign_name,
            "token_type": "bearer"
        }
    except Exception as e:
        print("ERROR:", e)
        raise HTTPException(status_code=500, detail=str(e))
    

# Callinggggggggggg
VICIDIAL_API_URL = "http://192.168.15.165/agc/api.php"

API_USER = "AdminR"
API_PASS = "AdminR"
AGENT_USER = "8015"

def pauseUser(current_user):
    print("Pausing agent : ", current_user)
    try:
        res = requests.get(
            VICIDIAL_API_URL,
            params={
                "source": "ctestrm",
                "user": API_USER,
                "pass": API_PASS,
                "agent_user": current_user["username"],
                "function": "external_pause",
                "value": "PAUSE"
            },
            timeout=10
        )
        print(f"Pause Response : ", res.content)
    except Exception as e:
        print("Failed to pause ", str(e))
    return res.text
 
def get_agent_status(username):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        "SELECT status FROM vicidial_live_agents WHERE user = %s",
        (username,)
    )
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    return row["status"] if row else None

# @app.post("/call")
# def call_number(phone: Optional[str] = None,current_user: str = Depends(get_current_user)):
#     userDetails = None

#     if not phone:
#         conn = mysql.connector.connect(**DB_CONFIG)
#         cursor = conn.cursor(dictionary=True)
#         today_start = date.today()
#         campaign_id = current_user["campaign_id"]
#         campaign_name = current_user["campaign_name"]
#         query = """
                
#                     select * from (
#                     select DISTINCT vl.phone_number, vl.lead_id, vl.first_name, vl.last_name, vl.status,""callback_time,""comments
#                     FROM vicidial_list vl  
#                     INNER JOIN vicidial_lists vls ON vl.list_id =vls.list_id 
#                     where vl.lead_id  not in (select distinct lead_id from vicidial_log) and vl.status in ('NEW') AND vls.campaign_id =%s
#                     )a   where  a.status  in ('NEW') order by a.lead_id  
#                           limit 1 """
#         params = (campaign_id,)
#         cursor.execute(query,params,)
#         userDetails = cursor.fetchone()
#         print(userDetails)
#         cursor.close()
#         conn.close()

#         if not userDetails:
#             raise HTTPException(404, "No callable leads found")

#         phone = userDetails["phone_number"]

#     params = {
#         "source": "crm",
#         "user": API_USER,
#         "pass": API_PASS,
#         "agent_user": current_user["username"],
#         "function": "external_dial",
#         "phone_code": "1",
#         "value": phone,
#         "preview": "NO",
#         "search": "YES",
#         "focus": "YES"
#     }

#     # agent_status = get_agent_status(current_user["username"])

#     # if agent_status != "PAUSED":
#     pause_response = pauseUser(current_user)
#     time.sleep(5)  # allow VICIdial to update state


#     try:
#         response = requests.get(VICIDIAL_API_URL, params=params, timeout=10)
#     except requests.exceptions.RequestException as e:
#         raise HTTPException(500, str(e))

#     return {
#         "status": "success",
#         "dialed_phone": phone,
#         "vicidial_response": response.text,
#         "details": jsonable_encoder(userDetails)
#     }

@app.post("/call")
def call_number(phone: Optional[str] = None, current_user: str = Depends(get_current_user)):
    userDetails = None
    lead_id     = None
    agent_user  = current_user["username"]
    campaign_id = current_user["campaign_id"]

    # ─────────────────────────────────────────
    # STEP 1 — Pause agent and confirm via DB
    # ─────────────────────────────────────────
    pauseUser(current_user)

    # Wait until DB confirms agent is paused
    paused = False
    for i in range(10):
        status = get_agent_status(agent_user)
        print(f"[AGENT STATUS {i+1}s]: {status}")
        if status in ("PAUSED", "PAUSE"):
            paused = True
            break
        time.sleep(1)

    if not paused:
        raise HTTPException(500, "Agent could not be paused. Please check agent is logged into VICIdial.")

    if not phone:
        try:
            conn   = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # ─────────────────────────────────────────
            # STEP 2 — Atomically claim lead
            # lock_token guarantees no two agents
            # ever claim the same lead simultaneously
            # ─────────────────────────────────────────
            lock_token = f"{agent_user}_{int(time.time()*1000)}"

            cursor.execute("""
                UPDATE vicidial_list vl
                INNER JOIN vicidial_lists vls ON vl.list_id = vls.list_id
                SET vl.status = 'INCALL',
                    vl.user   = %s
                WHERE vl.status       = 'NEW'
                  AND vls.campaign_id = %s
                  AND vl.lead_id NOT IN (SELECT lead_id FROM vicidial_log)
                ORDER BY vl.lead_id ASC
                LIMIT 1
            """, (lock_token, campaign_id))

            conn.commit()

            if cursor.rowcount == 0:
                raise HTTPException(404, "No callable leads found")

            # ─────────────────────────────────────────
            # STEP 3 — Fetch exactly the lead we claimed
            # ─────────────────────────────────────────
            cursor.execute("""
                SELECT  vl.lead_id,
                        vl.phone_number,
                        vl.first_name,
                        vl.last_name,
                        vl.comments
                FROM vicidial_list vl
                WHERE vl.user   = %s
                  AND vl.status = 'INCALL'
                ORDER BY vl.lead_id ASC
                LIMIT 1
            """, (lock_token,))

            row = cursor.fetchone()
            print(f"[CLAIMED LEAD]: {row}")

            if not row:
                raise HTTPException(404, "No callable leads found")

            lead_id     = row["lead_id"]
            phone       = row["phone_number"]
            userDetails = row

            # ─────────────────────────────────────────
            # STEP 4 — Set real agent on the lead
            # ─────────────────────────────────────────
            cursor.execute("""
                UPDATE vicidial_list
                SET user = %s
                WHERE lead_id = %s
            """, (agent_user, lead_id))
            conn.commit()

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"DB error: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    # ─────────────────────────────────────────
    # STEP 5 — Dial via VICIdial
    # ─────────────────────────────────────────
    dial_params = {
        "source":     "crm",
        "user":       API_USER,
        "pass":       API_PASS,
        "agent_user": agent_user,
        "function":   "external_dial",
        "phone_code": "1",
        "value":      phone,
        "preview":    "NO",
        "search":     "YES",
        "focus":      "YES",
        "lead_id":    lead_id,
    }

    try:
        response = requests.get(VICIDIAL_API_URL, params=dial_params, timeout=10)
        print(f"[DIAL RESPONSE]: {response.text}")
    except requests.exceptions.RequestException as e:
        raise HTTPException(500, str(e))

    # ─────────────────────────────────────────
    # STEP 6 — If dial fails release lead back
    # ─────────────────────────────────────────
    if "ERROR" in response.text.upper():
        try:
            conn   = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE vicidial_list
                SET status = 'NEW', user = ''
                WHERE lead_id = %s
            """, (lead_id,))
            conn.commit()
        except Exception:
            pass
        finally:
            cursor.close()
            conn.close()

        raise HTTPException(500, f"VICIdial dial error: {response.text}")

    return {
        "status":            "success",
        "dialed_phone":      phone,
        "lead_id":           lead_id,
        "vicidial_response": response.text,
        "details":           jsonable_encoder(userDetails),
    }
#Hangup
# Hangup (fallback-safe)
# @app.post("/hangup")
# def hangup_call(current_user: str = Depends(get_current_user)):

#     # 1️⃣ Lock the call first
#     requests.get(
#         VICIDIAL_API_URL,
#         params={
#             "source": "crm",
#             "user": API_USER,
#             "pass": API_PASS,
#             "agent_user": current_user,
#             "function": "external_status",
#             "value": "A"
#         },
#         timeout=10
#     )

#     time.sleep(1)

#     # 2️⃣ Hangup both legs
#     params = {
#         "source": "crm",
#         "user": API_USER,
#         "pass": API_PASS,
#         "function": "external_hangup",
#         "agent_user": current_user,
#         "value": "YES"
#     }

#     res = requests.get(VICIDIAL_API_URL, params=params, timeout=10)

#     if "SUCCESS" not in res.text:
#         raise HTTPException(status_code=400, detail=res.text)

#     return {
#         "status": "success",
#         "agent_user": current_user,
#         "vicidial_response": res.text
#     }

#hangup 
@app.post("/hangup")
def hangup_call(current_user: str = Depends(get_current_user)):
    user_id = current_user["username"]

    params = {
        "source": "crm",
        "user": API_USER,
        "pass": API_PASS,
        "function": "external_hangup",
        "agent_user": user_id,
        "value": 1
    }
    res = requests.get(VICIDIAL_API_URL, params=params, timeout=10)

    if "SUCCESS" not in res.text:
        raise HTTPException(status_code=400, detail=res.text)
    # unPauseUser(current_user)

    return {
        "status": "success",
        "agent_user":  user_id,
        "vicidial_response": res.text
    }


#vicidial_logData for status 

@app.post("/logdata")
def login(request: Request, current_user: str = Depends(get_current_user)):
    try:
        user = current_user["username"] 
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        query = """
            select count(*) as inCall from vicidial_live_agents where user = %s AND  lead_id in (select distinct Lead_id from vicidial_auto_calls);
        """
        params = (user,)

        cursor.execute(query,params,)
        data = cursor.fetchone()

        cursor.close()
        conn.close()

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#Status  
@app.post("/vicidial-agent")
def vicidial_agent_action(
    status: str,
    callback_datetime: str = None,
    callback_comments: str = None,
    current_user: str = Depends(get_current_user)
):
    responses = {}

    # 1️⃣ Build external_status params
    status_params = {
        "source": "crm",
        "user": API_USER,
        "pass": API_PASS,
        "agent_user": current_user["username"],
        "function": "external_status",
        "value": status
    }

    if status == "CBR":
        if not callback_datetime:
            raise HTTPException(
                400,
                "callback_datetime and callback_comments required for CALLBK"
            )

        status_params.update({
            "callback_datetime": callback_datetime,
            "callback_type": "USERONLY",
            "callback_comments": callback_comments
        })

    # 2️⃣ Disposition
    status_resp = requests.get(
        VICIDIAL_API_URL,
        params=status_params,
        timeout=10
    )
    responses["status"] = status_resp.text

    if "SUCCESS" not in status_resp.text:
        raise HTTPException(400, status_resp.text)

    # 3️⃣ Hangup
    hangup_resp = requests.get(
        VICIDIAL_API_URL,
        params={
            "source": "crm",
            "user": API_USER,
            "pass": API_PASS,
            "agent_user": current_user["username"],
            "function": "external_hangup",
            "value": 1
        },
        timeout=10
    )
    responses["hangup"] = hangup_resp.text

    # 4️⃣ Pause
    # time.sleep(5)
    # pause_resp = requests.get(
    #     VICIDIAL_API_URL,
    #     params={
    #         "source": "crm",
    #         "user": API_USER,
    #         "pass": API_PASS,
    #         "agent_user": current_user["username"],
    #         "function": "external_pause",
    #         "value": "PAUSE"
    #     },
    #     timeout=10
    # )
    # responses["pause"] = pause_resp.tex
    time.sleep(5)
    pause_text = pauseUser(current_user)  # ✅ reusing your existing function
    responses["pause"] = pause_text

    return {
        "success": True,
        "agent": current_user["username"],
        "vicidial_responses": responses
    }

# Client data for Agents
@app.post("/clients_for_agent")
def call_number(callbackOnly: Optional[bool] = False,  current_user: str = Depends(get_current_user)):
  
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    today_start = date.today()
    
    user_id = current_user["username"]
    campaign_id = current_user["campaign_id"]
    campaign_name = current_user["campaign_name"]

    if callbackOnly:
        query = """ SELECT DISTINCT vl.phone_number,vl.title,vl.first_name, vl.last_name,vl.city,vl.country_code,date(vl.entry_date)entry_date,vl.date_of_birth,vl.list_id,vc.callback_time,
                    vc.comments,vl.lead_id,vl.status,vls.campaign_id  
                    FROM vicidial_callbacks vc
                    INNER JOIN vicidial_list vl ON vc.lead_id = vl.lead_id
                    INNER JOIN vicidial_lists vls ON vl.list_id=vls.list_id
                    LEFT JOIN vicidial_live_agents vla  ON vl.lead_id = vla.lead_id
                    WHERE vc.status in ('ACTIVE','LIVE') and vc.user=%s AND vc.callback_time >= now() 
                    AND vla.lead_id IS NULL AND vls.campaign_id = %s
                    AND vl.status  IN ('CBR','CBHOLD') 

                """
        params=(user_id,campaign_id,)
    else: 
        query = """
                select * from (
                select distinct vl.title,vl.first_name, vl.last_name,vl.city,vl.country_code,date(vl.entry_date)entry_date,vl.date_of_birth,vl.list_id,NULL callback_time
                ,NULL comments,vl.lead_id,vl.status,vls.campaign_id
                FROM vicidial_list vl 
                INNER JOIN vicidial_lists vls ON vl.list_id =vls.list_id
                where vl.lead_id  not in (select distinct lead_id from vicidial_log) and vl.status in ('NEW') AND vls.campaign_id =%s
                )a   where  a.status  in ('NEW') 
                order by a.lead_id  
                        """
        params = (campaign_id,)
    cursor.execute(query,params,)
    data = cursor.fetchall() 

    cursor.close()
    conn.close()

    return {
        "status": "success",
        "total_records": len(data),
        "data": data
    }



pgsqlPool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=20,  
    dbname="customDialer",
    user="postgres",
    password="Soft!@7890",
    host="192.168.15.105"
)

BUFFER_SECONDS = 30
@app.get("/ping")
def ping(current_user: str = Depends(get_current_user)):
    user_id = current_user["username"]
    now = datetime.now()

    conn = pgsqlPool.getconn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, first_tick, last_tick
        FROM user_online_sessions
        WHERE user_id = %s
        ORDER BY last_tick DESC
        LIMIT 1
    """, (user_id,))

    row = cur.fetchone()

    if row:
        session_id, first_tick, last_tick = row
        if now.date() == last_tick.date() and now - last_tick <= timedelta(seconds=BUFFER_SECONDS):
            cur.execute("""
                UPDATE user_online_sessions
                SET last_tick = %s
                WHERE id = %s
            """, (now, session_id))
        else:
            cur.execute("""
                INSERT INTO user_online_sessions
                (user_id, first_tick, last_tick)
                VALUES (%s, %s, %s)
            """, (user_id, now, now))
    else:
        cur.execute("""
            INSERT INTO user_online_sessions
            (user_id, first_tick, last_tick)
            VALUES (%s, %s, %s)
        """, (user_id, now, now))

    conn.commit()
    cur.close()
    pgsqlPool.putconn(conn)
    return {"status": "ok"}


@app.get("/usertimeline")
def ping(current_user: str = Depends(get_current_user)):
    try:
        user_id = current_user["username"]
        conn = pgsqlPool.getconn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        today_start = date.today()
        
        query = """
            SELECT * FROM public.user_online_sessions where user_id=%s and date(first_tick)=CURRENT_DATE
            and date(last_tick)=CURRENT_DATE 
        """
        params = (user_id,)
        # params = (user_id,today_start,today_start,)

        cursor.execute(query, params,)
        data = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "count": len(data),
            "leads": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/health/db")
def check_db():
    conn = None
    try:
        conn = pgsqlPool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        return {"db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="DB not connected")
    finally:
        if conn:
            pgsqlPool.putconn(conn)


@app.get("/campaigns")
def get_active_campaigns():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT campaign_id, campaign_name, active
        FROM vicidial_campaigns
        WHERE active = 'Y'
        """
        cursor.execute(query)
        campaigns = cursor.fetchall()

        return {
            "status": "success",
            "count": len(campaigns),
            "data": campaigns
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

class DeleteLeadRequest(BaseModel):
    phone_number: List[str]

@app.post("/delete_lead")
def delet_lead(data: DeleteLeadRequest, current_user: str = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    #r phone_number = request.query_params.get("phone_numbe") 
    try : 
        
        
        phone_list = data.phone_number #.strip("[]").split(",")

        phone_list = [p.strip() for p in phone_list]

        placeholders = ",".join(["%s"] * len(phone_list))

        delete_leads_query = f"""
            DELETE FROM vicidial_list
            WHERE phone_number IN ({placeholders})
        """
        cursor.execute(delete_leads_query, tuple(phone_list))

        delete_logs_query = f"""
            DELETE FROM vicidial_log
            WHERE phone_number IN ({placeholders})
        """
        cursor.execute(delete_logs_query, tuple(phone_list))

        # delete_carrier_log_query = "DELETE FROM vicidial_carrier_log WHERE phone_number = %s"
        # cursor.execute(delete_carrier_log_query, (phone_number,))

        conn.commit()

        cursor.close()
        conn.close()
        return {"message": f"Records for phone number {data.phone_number} deleted successfully"}
    except Error as e :
        raise HTTPException(status_code=500, detail=f"Error deleting records{e}")
    


@app.get("/status_data")
def get_status(current_user: str = Depends(get_current_user)):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    user_id = current_user["username"]


    try:

        params = {
            "source": "fastapi",
            "user": API_USER,
            "pass": API_PASS,
            "function": "agent_status",
            "agent_user": user_id
        }

        response = requests.get(vicidial_url, params=params)

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="VICIdial API not reachable")

        data = response.text

        # -------- STATUS LOGIC --------
        if "INCALL" in data:
            call_status = "IN_CALL"

        elif "QUEUE" in data or "RINGING" in data:
            call_status = "RINGING"

        elif "DISPO" in data:
            call_status = "DISPOSITION_PENDING"

        elif "PAUSED" in data:
            call_status = "PAUSED"

        elif "READY" in data:
            call_status = "READY"

        else:
            call_status = "DISCONNECTED"


        return {
            "status": "success",
            "data": {
                "agent": user_id,
                "call_status": call_status
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting status: {e}")

@app.get("/callusingzoho")
def call_number(phone: Optional[str] = None):
    userDetails = None

    params = {
        "source": "crm",
        "user": API_USER,
        "pass": API_PASS,
        "agent_user": '8999',
        "function": "external_dial",
        "phone_code": "1",
        "value": phone,
        "preview": "NO",
        "search": "YES",
        "focus": "YES"
    }

    pause_response = pauseUser({"username": 8999})
    time.sleep(5)  # allow VICIdial to update state


    try:
        response = requests.get(VICIDIAL_API_URL, params=params, timeout=10)
    except requests.exceptions.RequestException as e:
        raise HTTPException(500, str(e))

    return {
        "status": "success",
        "dialed_phone": phone,
        "vicidial_response": response.text,
        "details": jsonable_encoder(userDetails)
    }
