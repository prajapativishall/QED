import os
import mysql.connector
from datetime import datetime
from django.http import JsonResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required

# ---------------- CONFIG ----------------
CIRCLE_LIST = [
    "BH","CG","JH","RJ","WB","NESA","OR","KK","MP","HR",
    "UP East","PB","MG","JK","DL","TN","KL","HP","UP West","AP","GUJ"
]

ACTIVITY_LIST = [
    "BFS","PLVA","TLVA","PLVA + STR","TLVA + STR","Verticality",
    "ALS","RR","JV - Thar","RR Str.","JV","BFS Str.",
    "Civil Survey + Dwgs.","Foundation Design","Foundation Str."
]

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# ------------------------------------------------
# DB HELPERS
# ------------------------------------------------
def get_circle_head_summary(start, end, circle, activity):
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            if not start:
                start = "1900-01-01"
            if not end:
                end = "9999-12-31"

            query = """
            WITH base AS (
                SELECT DISTINCT P.ID_
                FROM ACT_HI_PROCINST P
                WHERE DATE(P.START_TIME_) BETWEEN %s AND %s
                  AND EXISTS (
                    SELECT 1 FROM ACT_HI_VARINST V2
                    WHERE V2.PROC_INST_ID_ = P.ID_
                      AND V2.NAME_ = 'qacajobid'
                      AND V2.TEXT_ IS NOT NULL
                  )
            )
            SELECT
                SUM(CASE WHEN allot IS NOT NULL AND asurvey IS NULL THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN allot IS NOT NULL AND asurvey IS NOT NULL THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN allot IS NULL THEN 1 ELSE 0 END) AS flag
            FROM (
                SELECT
                    MAX(CASE WHEN V.NAME_ IN ('allotmentdate','allocationdate') THEN COALESCE(V.LONG_, V.BYTEARRAY_ID_) END) AS allot,
                    MAX(CASE WHEN V.NAME_='actualsurveydate' THEN COALESCE(V.LONG_, V.BYTEARRAY_ID_) END) AS asurvey,
                    MAX(CASE WHEN V.NAME_='circle' THEN V.TEXT_ END) AS circle,
                    MAX(CASE WHEN V.NAME_='activitytype' THEN V.TEXT_ END) AS activity
                FROM base B
                JOIN ACT_HI_VARINST V ON B.ID_ = V.PROC_INST_ID_
                GROUP BY B.ID_
            ) X
            WHERE (%s = '' OR circle = %s)
              AND (%s = '' OR activity = %s);
            """

            cursor.execute(query, (start, end, circle, circle, activity, activity))
            row = cursor.fetchone()

            return {
                "Pending": int(row[0] or 0),
                "Completed": int(row[1] or 0),
                "Flag": int(row[2] or 0),
            }
    except Exception as e:
        print("Circle Head summary error:", e)

    return {"Pending": 0, "Completed": 0, "Flag": 0}


def get_design_team_summary(start, end, circle, activity):
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            if not start:
                start = "1900-01-01"
            if not end:
                end = "9999-12-31"

            query = """
            WITH base AS (
                SELECT DISTINCT P.ID_
                FROM ACT_HI_PROCINST P
                WHERE DATE(P.START_TIME_) BETWEEN %s AND %s
                  AND EXISTS (
                    SELECT 1 FROM ACT_HI_VARINST V2
                    WHERE V2.PROC_INST_ID_ = P.ID_
                      AND V2.NAME_ = 'qacajobid'
                      AND V2.TEXT_ IS NOT NULL
                  )
            )
            SELECT
                SUM(CASE WHEN srecv IS NOT NULL AND rcomp IS NULL THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN srecv IS NOT NULL AND rcomp IS NOT NULL THEN 1 ELSE 0 END) AS completed
            FROM (
                SELECT
                    MAX(CASE WHEN V.NAME_='surveydatereceived' THEN COALESCE(V.LONG_, V.BYTEARRAY_ID_) END) AS srecv,
                    MAX(CASE WHEN V.NAME_='reviewcompletiondate' THEN COALESCE(V.LONG_, V.BYTEARRAY_ID_) END) AS rcomp,
                    MAX(CASE WHEN V.NAME_='circle' THEN V.TEXT_ END) AS circle,
                    MAX(CASE WHEN V.NAME_='activitytype' THEN V.TEXT_ END) AS activity
                FROM base B
                JOIN ACT_HI_VARINST V ON B.ID_ = V.PROC_INST_ID_
                GROUP BY B.ID_
            ) X
            WHERE (%s = '' OR circle = %s)
              AND (%s = '' OR activity = %s);
            """

            cursor.execute(query, (start, end, circle, circle, activity, activity))
            row = cursor.fetchone()

            return {
                "Pending": int(row[0] or 0),
                "Completed": int(row[1] or 0),
            }
    except Exception as e:
        print("Design Team summary error:", e)

    return {"Pending": 0, "Completed": 0}


def get_flowable_users():
    users = []
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            query = "SELECT ID_ AS id, CONCAT(IFNULL(FIRST_,''),' ',IFNULL(LAST_,'')) AS name, EMAIL_ AS email FROM ACT_ID_USER ORDER BY FIRST_, LAST_;"
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            users = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print("Error fetching flowable users:", e)
    return users


def get_flowable_groups():
    groups = []
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            query = "SELECT ID_ AS id, NAME_ AS name FROM ACT_ID_GROUP ORDER BY NAME_;"
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            groups = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print("Error fetching flowable groups:", e)
    return groups


def get_user_groups(user_id):
    """Fetch group IDs for a user from Flowable DB."""
    groups = []
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT GROUP_ID_ FROM ACT_ID_MEMBERSHIP WHERE USER_ID_ = %s", (user_id,))
            groups = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching groups for user {user_id}: {e}")
    return groups


def get_user_activity_sites(user_id):
    results = []
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            # Fetch activities for sites where the user is either the START_USER_ID_ 
            # OR listed as the 'initiator' variable (common in API-triggered processes).
            query = """
            SELECT activity, COUNT(DISTINCT siteid) AS completed_sites 
            FROM ( 
                SELECT P.ID_, 
                    MAX(CASE WHEN V.NAME_='activitytype' THEN V.TEXT_ END) AS activity, 
                    MAX(CASE WHEN V.NAME_='siteid' THEN V.TEXT_ END) AS siteid,
                    MAX(CASE WHEN V.NAME_='initiator' THEN V.TEXT_ END) AS initiator,
                    P.START_USER_ID_
                FROM ACT_HI_PROCINST P 
                JOIN ACT_HI_VARINST V ON V.PROC_INST_ID_ = P.ID_ 
                WHERE P.END_TIME_ IS NOT NULL 
                GROUP BY P.ID_ 
            ) X 
            WHERE activity IS NOT NULL AND siteid IS NOT NULL 
            AND (START_USER_ID_ = %s OR initiator = %s)
            GROUP BY activity 
            ORDER BY completed_sites DESC;
            """
            cursor.execute(query, (user_id, user_id))
            results = [{"activity": row[0], "completed_sites": row[1]} for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching activity sites for user {user_id}:", e)
    return results


def get_unique_activity_types():
    activities = []
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            query = "SELECT DISTINCT TEXT_ FROM ACT_HI_VARINST WHERE NAME_ = 'activitytype' ORDER BY TEXT_ ASC;"
            cursor.execute(query)
            activities = [row[0] for row in cursor.fetchall() if row[0]]
    except Exception as e:
        print("Error fetching activity types:", e)
    return activities


def get_unique_site_ids():
    sites = []
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            query = "SELECT DISTINCT TEXT_ FROM ACT_HI_VARINST WHERE NAME_ = 'siteid' AND TEXT_ IS NOT NULL AND TEXT_ != '' ORDER BY TEXT_ ASC;"
            cursor.execute(query)
            sites = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print("Error fetching site IDs:", e)
    return sites


def get_user_task_stats(user_id, site_id=None, activity_type=None):
    results = {
        "summary": {"completed": 0, "pending": 0},
        "tasks": []
    }
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            cursor = conn.cursor()
            
            # Base query joins Task Instances with Variables
            # We fetch both Completed (END_TIME_ IS NOT NULL) and Pending (END_TIME_ IS NULL)
            # Pending tasks might also be in ACT_RU_TASK, but ACT_HI_TASKINST usually covers both history and active.
            # Active tasks have END_TIME_ = NULL in ACT_HI_TASKINST.
            
            # Construct dynamic WHERE clause
            # We want tasks where:
            # 1. Assignee is the user
            # OR
            # 2. Assignee is NULL AND (User is candidate OR User's group is candidate)
            
            user_groups = get_user_groups(user_id)
            
            candidate_subquery_parts = ["USER_ID_ = %s"]
            candidate_params = [user_id]
            
            if user_groups:
                placeholders = ', '.join(['%s'] * len(user_groups))
                candidate_subquery_parts.append(f"GROUP_ID_ IN ({placeholders})")
                candidate_params.extend(user_groups)
                
            candidate_condition_str = " OR ".join(candidate_subquery_parts)
            
            # Main WHERE clause
            # (T.ASSIGNEE_ = user_id) OR (T.ASSIGNEE_ IS NULL AND T.ID_ IN (SELECT TASK_ID_ FROM ACT_HI_IDENTITYLINK WHERE TYPE_='candidate' AND (...)))
            
            where_clause = f"""
            WHERE (
                T.ASSIGNEE_ = %s 
                OR (
                    T.ASSIGNEE_ IS NULL 
                    AND T.ID_ IN (
                        SELECT TASK_ID_ 
                        FROM ACT_HI_IDENTITYLINK 
                        WHERE TYPE_ = 'candidate' 
                        AND ({candidate_condition_str})
                    )
                )
            )
            """
            params = [user_id] + candidate_params
            
            if site_id:
                where_clause += " AND siteid LIKE %s"
                params.append(f"%{site_id}%")
            
            if activity_type:
                where_clause += " AND activitytype = %s"
                params.append(activity_type)
            
            query = f"""
            SELECT 
                T.ID_,
                T.NAME_, 
                T.START_TIME_,
                T.END_TIME_,
                siteid,
                activitytype,
                T.PROC_INST_ID_
            FROM (
                SELECT T.ID_, T.NAME_, T.ASSIGNEE_, T.START_TIME_, T.END_TIME_, T.PROC_INST_ID_
                FROM ACT_HI_TASKINST T
            ) T
            JOIN (
                SELECT PROC_INST_ID_, 
                       MAX(CASE WHEN NAME_='siteid' THEN TEXT_ END) as siteid,
                       MAX(CASE WHEN NAME_='activitytype' THEN TEXT_ END) as activitytype
                FROM ACT_HI_VARINST
                GROUP BY PROC_INST_ID_
            ) V ON V.PROC_INST_ID_ = T.PROC_INST_ID_
            {where_clause}
            ORDER BY (T.END_TIME_ IS NULL) DESC, T.END_TIME_ DESC
            """
            
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            
            completed_count = 0
            pending_count = 0
            task_list = []
            
            for row in rows:
                task_id, name, start_time, end_time, site, activity, proc_inst_id = row
                status = "Completed" if end_time else "Pending"
                display_date = end_time or start_time
                if status == "Completed":
                    completed_count += 1
                else:
                    pending_count += 1
                
                task_list.append({
                    "id": task_id,
                    "name": name,
                    "siteid": site,
                    "activity": activity,
                    "status": status,
                    "date": display_date.strftime("%Y-%m-%d %H:%M") if display_date else "-",
                    "proc_inst_id": proc_inst_id,
                })
            
            results["summary"]["completed"] = completed_count
            results["summary"]["pending"] = pending_count
            results["tasks"] = task_list
            
    except Exception as e:
        print(f"Error fetching task stats for user {user_id}:", e)
    return results


# ------------------------------------------------
# VIEWS
# ------------------------------------------------
@login_required
def dashboard_view(request):
    default_start = "2025-09-01"
    default_end = datetime.today().strftime("%Y-%m-%d")

    circle_stats = get_circle_head_summary(default_start, default_end, "", "")
    design_stats = get_design_team_summary(default_start, default_end, "", "")

    return render(
        request,
        "qed_utility/dashboard.html",
        {
            "circles": CIRCLE_LIST,
            "activity_list": ACTIVITY_LIST,
            "circle_stats": circle_stats,
            "design_stats": design_stats,
            "default_start": default_start,
            "default_end": default_end,
        }
    )


@login_required
def api_ch_summary(request):
    start = request.GET.get("ch_start", "2025-09-01")
    end = request.GET.get("ch_end", datetime.today().strftime("%Y-%m-%d"))
    circle = request.GET.get("ch_circle", "")
    activity = request.GET.get("ch_activity", "")

    return JsonResponse(get_circle_head_summary(start, end, circle, activity))


@login_required
def api_dt_summary(request):
    start = request.GET.get("dt_start", "2025-09-01")
    end = request.GET.get("dt_end", datetime.today().strftime("%Y-%m-%d"))
    circle = request.GET.get("dt_circle", "")
    activity = request.GET.get("dt_activity", "")

    return JsonResponse(get_design_team_summary(start, end, circle, activity))


@login_required
def api_flowable_users(request):
    return JsonResponse(get_flowable_users(), safe=False)


@login_required
def api_flowable_groups(request):
    return JsonResponse(get_flowable_groups(), safe=False)


@login_required
def api_user_activity_sites(request):
    user_id = request.GET.get("user", "")
    return JsonResponse(get_user_activity_sites(user_id), safe=False)


@login_required
def api_activity_types(request):
    return JsonResponse(get_unique_activity_types(), safe=False)


@login_required
def api_site_ids(request):
    return JsonResponse(get_unique_site_ids(), safe=False)


@login_required
def api_user_tasks(request):
    user_id = request.GET.get("user", "")
    site_id = request.GET.get("site", "")
    activity_type = request.GET.get("activity", "")
    return JsonResponse(get_user_task_stats(user_id, site_id, activity_type), safe=False)
