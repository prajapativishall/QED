import os
import logging
import requests
import mysql.connector
import pandas as pd
from django.http import HttpResponse
from django.shortcuts import render
from django.conf import settings
from django.contrib.auth.decorators import login_required
from qed_utility.access import role_required


logger = logging.getLogger(__name__)

# ================= FLOWABLE CONFIG =================
FLOWABLE_BASE = os.getenv("FLOWABLE_BASE")
FLOWABLE_USER = os.getenv("FLOWABLE_USER")
FLOWABLE_PASS = os.getenv("FLOWABLE_PASS")
PAGE_SIZE = int(os.getenv("FLOWABLE_PAGE_SIZE", 200))

# ================= MYSQL CONFIG =================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# =================================================
# FETCH PROCESS INSTANCE IDS (REST)
# =================================================
def fetch_instance_ids(start_date, end_date):
    ids = []
    start = 0

    while True:
        payload = {
            "start": start,
            "size": PAGE_SIZE,
            "startedAfter": start_date,
            "startedBefore": end_date
        }

        r = requests.post(
            f"{FLOWABLE_BASE}/process-api/query/historic-process-instances",
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            json=payload,
            timeout=60
        )

        data = r.json().get("data", [])
        if not data:
            break

        ids.extend(inst["id"] for inst in data)
        start += PAGE_SIZE

    return ids


# =================================================
# FETCH EXPORT DATA FROM DB
# =================================================
def fetch_export_data(instance_ids):
    if not instance_ids:
        return pd.DataFrame()

    placeholders = ",".join(["%s"] * len(instance_ids))

    query = f"""
    SELECT 
        P.ID_ AS `Process Instance ID`,
        DATE(P.START_TIME_) AS `Start Date`,
        DATE(P.END_TIME_) AS `End Date`,
        CASE WHEN P.END_TIME_ IS NULL THEN 'Pending' ELSE 'Completed' END AS `Status`,

        -- General Info
        MAX(CASE WHEN V.NAME_ = 'initiator' THEN V.TEXT_ END) AS `Circle Head Name`,
        MAX(CASE WHEN V.NAME_ = 'qacajobid' THEN V.TEXT_ END) AS `QACA Job ID`,
        MAX(CASE WHEN V.NAME_ = 'odooid' THEN V.TEXT_ END) AS `Odoo ID`,
        MAX(CASE WHEN V.NAME_ = 'siteid' THEN V.TEXT_ END) AS `Site ID`,
        MAX(CASE WHEN V.NAME_ = 'sitename' THEN V.TEXT_ END) AS `Site Name`,
        MAX(CASE WHEN V.NAME_ = 'circle' THEN V.TEXT_ END) AS `Circle`,
        MAX(CASE WHEN V.NAME_ = 'client' THEN V.TEXT_ END) AS `Client`,
        MAX(CASE WHEN V.NAME_ = 'activitytype' THEN V.TEXT_ END) AS `Activity Type`,
        MAX(CASE WHEN V.NAME_ = 'rateofaudit' THEN V.TEXT_ END) AS `Rate of Audit`,

        MAX(CASE WHEN V.NAME_ = 'allocationtype' THEN V.TEXT_ END) AS `Allocation Type`,
        MAX(CASE WHEN V.NAME_ = 'assignsurveycoordinator' THEN V.TEXT_ END) AS `Assigned Survey Co-ordinator`,
        MAX(CASE WHEN V.NAME_ = 'surveyengineername' THEN V.TEXT_ END) AS `Assigned Survey Engineer`,
        MAX(CASE WHEN V.NAME_ = 'surveyupload-clientportal' THEN V.TEXT_ END) AS `Survey Upload Client Portal`,

        -- Survey Meta
        MAX(CASE WHEN V.NAME_ = 'issurveyincluded' THEN V.TEXT_ END) AS `Is Survey Included`,
        MAX(CASE WHEN V.NAME_ = 'surveytargetdate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Survey Target Date`,
        MAX(CASE WHEN V.NAME_ = 'surveysenttodesign'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Survey Sent To Design`,
        MAX(CASE WHEN V.NAME_ IN ('allotmentdate','allocationdate')
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Allocation Date`,

        MAX(CASE WHEN V.NAME_ = 'finalduedate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Final Due Date`,
        MAX(CASE WHEN V.NAME_ = 'assigndesignlead' THEN V.TEXT_ END) AS `Assigned Design Lead`,
        MAX(CASE WHEN V.NAME_ = 'actualsurveydate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Actual Survey Date`,
        MAX(CASE WHEN V.NAME_ = 'surveydatereceived'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Survey Date Received`,
        MAX(CASE WHEN V.NAME_ = 'reportcategoryonlyforbfs' THEN V.TEXT_ END) AS `Report Category Only For BFS`,

        -- Assigned Roles + Time Taken
        MAX(CASE WHEN V.NAME_ = 'assignsurveyvalidator' THEN V.TEXT_ END) AS `Assigned Survey Validator`,
        MAX(CASE WHEN V.NAME_ = 'surveyftr' THEN V.TEXT_ END) AS `Survey-FTR`,
        MAX(CASE WHEN V.NAME_ = 'timetaken' THEN V.TEXT_ END) AS `Time Taken - Survey Validator`,
        MAX(CASE WHEN V.NAME_ = 'surveyvalidationdate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Survey Validation Date`,

        MAX(CASE WHEN V.NAME_ = 'assigndesignstr' THEN V.TEXT_ END) AS `Assigned Design Engineer Str`,
        MAX(CASE WHEN V.NAME_ = 'timetaken3' THEN V.TEXT_ END) AS `Time Taken - Design Engineer Str`,
        MAX(CASE WHEN V.NAME_ = 'reportcompletiondate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Report Completion Date Design Eng Str`,

        MAX(CASE WHEN V.NAME_ = 'assigndesignengineer' THEN V.TEXT_ END) AS `Assigned Design Engineer`,
        MAX(CASE WHEN V.NAME_ = 'timetaken0' THEN V.TEXT_ END) AS `Time Taken - Design Engineer`,
        MAX(CASE WHEN V.NAME_ = 'reportcompletiondate1'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Report Completion Date`,

        MAX(CASE WHEN V.NAME_ = 'assigndraftpersonlayout' THEN V.TEXT_ END) AS `Assigned Draftperson Layout`,
        MAX(CASE WHEN V.NAME_ = 'timetaken2' THEN V.TEXT_ END) AS `Time Taken - Draftperson Layout`,
        MAX(CASE WHEN V.NAME_ = 'layoutpreparationdate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Layout Preparation Date`,

        MAX(CASE WHEN V.NAME_ = 'assigndraftpersonstrdetail' THEN V.TEXT_ END) AS `Assigned Draftperson Str/Detail`,
        MAX(CASE WHEN V.NAME_ = 'timetaken1' THEN V.TEXT_ END) AS `Time Taken - Draftperson Str/Detail`,
        MAX(CASE WHEN V.NAME_ = 'strdetailcompletiondate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `STR /Detail Completion Date`,

        -- Quality Check
        MAX(CASE WHEN V.NAME_ = 'assignqualitychecker' THEN V.TEXT_ END) AS `Assigned Quality Checker`,
        MAX(CASE WHEN V.NAME_ = 'reviewcompletiondate'
            THEN DATE_FORMAT(FROM_UNIXTIME(V.LONG_ / 1000), '%d-%m-%Y') END) AS `Review Completion Date`,
        MAX(CASE WHEN V.NAME_ = 'reasonbehindtatinlay' THEN V.TEXT_ END) AS `Reason Behind Delay in TAT`,
        MAX(CASE WHEN V.NAME_ = 'sitestatus' THEN V.TEXT_ END) AS `Site Status`,
        MAX(CASE WHEN V.NAME_ = 'approvalstatus' THEN V.TEXT_ END) AS `Approval Status`,
        MAX(CASE WHEN V.NAME_ = 'workdonein' THEN V.TEXT_ END) AS `Work Done In`,
        MAX(CASE WHEN V.NAME_ = 'mailsenttocirclehead' THEN V.TEXT_ END) AS `Mail sent to Circle Head`,

        -- Rejection / Remarks
        MAX(CASE WHEN V.NAME_ = 'rejectioncomment' THEN V.TEXT_ END) AS `Rejection By Design Lead`,
        MAX(CASE WHEN V.NAME_ = 'remarkfromdesigncoordinator' THEN V.TEXT_ END) AS `Remark from Design coordinator`,
        MAX(CASE WHEN V.NAME_ = 'rejectionreason' THEN V.TEXT_ END) AS `Rejection By Quality Checker`

    FROM ACT_HI_PROCINST P
    JOIN ACT_HI_VARINST V ON P.ID_ = V.PROC_INST_ID_
    WHERE P.ID_ IN ({placeholders})
    GROUP BY P.ID_
    ORDER BY P.START_TIME_ DESC
    """

    with mysql.connector.connect(**DB_CONFIG) as conn:
        return pd.read_sql(query, conn, params=instance_ids)


# =================================================
# VIEW
# =================================================
@role_required("designcoordinator")
def export_view(request):
    if request.method == "POST":
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        
        logger.info(f"User '{request.user.username}' (ID: {request.user.id}) requesting export from {start_date} to {end_date}.")

        instance_ids = fetch_instance_ids(start_date, end_date)
        if not instance_ids:
            return HttpResponse("No process instances found", status=404)

        df = fetch_export_data(instance_ids)
        if df.empty:
            return HttpResponse("No data found", status=404)

        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = 'attachment; filename="flowable_export.xlsx"'

        df.to_excel(response, index=False)
        return response

    logger.info(f"User '{request.user.username}' (ID: {request.user.id}) accessed export page.")
    return render(request, "qed_utility/export.html")
