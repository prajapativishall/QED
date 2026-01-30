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

    # 1. Fetch Process Instance Basic Info
    query_proc = f"""
    SELECT 
        ID_ AS `Process Instance ID`,
        DATE(START_TIME_) AS `Start Date`,
        DATE(END_TIME_) AS `End Date`,
        CASE WHEN END_TIME_ IS NULL THEN 'Pending' ELSE 'Completed' END AS `Status`
    FROM ACT_HI_PROCINST
    WHERE ID_ IN ({placeholders})
    ORDER BY START_TIME_ DESC
    """

    # 2. Fetch All Variables
    query_vars = f"""
    SELECT 
        PROC_INST_ID_ AS `Process Instance ID`,
        NAME_,
        TEXT_,
        LONG_,
        DOUBLE_
    FROM ACT_HI_VARINST
    WHERE PROC_INST_ID_ IN ({placeholders})
    """

    with mysql.connector.connect(**DB_CONFIG) as conn:
        df_proc = pd.read_sql(query_proc, conn, params=instance_ids)
        df_vars = pd.read_sql(query_vars, conn, params=instance_ids)

    if df_vars.empty:
        return df_proc

    # ---------------------------------------------------------
    # Process Variables in Python to handle types and pivoting
    # ---------------------------------------------------------
    
    # Define fields that should be treated as dates (from LONG_ timestamp)
    date_fields = {
        'surveytargetdate', 'surveysenttodesign', 'allotmentdate', 'allocationdate',
        'finalduedate', 'actualsurveydate', 'surveydatereceived', 'surveyvalidationdate',
        'reportcompletiondate', 'reportcompletiondate1', 'layoutpreparationdate',
        'strdetailcompletiondate', 'reviewcompletiondate'
    }

    # Initialize Value column with TEXT_
    df_vars['Value'] = df_vars['TEXT_']

    # Fill missing Value with DOUBLE_
    mask_double = df_vars['Value'].isna() & df_vars['DOUBLE_'].notna()
    if mask_double.any():
        df_vars.loc[mask_double, 'Value'] = df_vars.loc[mask_double, 'DOUBLE_'].astype(str)

    # Handle LONG_ (Dates vs Integers)
    # If it's a known date field, convert timestamp to string
    mask_date = df_vars['NAME_'].isin(date_fields) & df_vars['LONG_'].notna()
    if mask_date.any():
        df_vars.loc[mask_date, 'Value'] = pd.to_datetime(df_vars.loc[mask_date, 'LONG_'], unit='ms').dt.strftime('%d-%m-%Y')

    # For other LONG_ values that are not dates, use them as is if Value is still empty
    mask_long_other = (~mask_date) & df_vars['Value'].isna() & df_vars['LONG_'].notna()
    if mask_long_other.any():
        df_vars.loc[mask_long_other, 'Value'] = df_vars.loc[mask_long_other, 'LONG_'].astype(str)

    # Pivot: Index=Process Instance ID, Columns=NAME_, Values=Value
    # We use pivot_table with aggfunc='first' to handle potential duplicates (though unlikely for same var name in one instance)
    df_pivot = df_vars.pivot_table(index='Process Instance ID', columns='NAME_', values='Value', aggfunc='first')

    # Merge Process Info with Pivoted Variables
    df_final = pd.merge(df_proc, df_pivot, on='Process Instance ID', how='left')

    # ---------------------------------------------------------
    # Rename Columns to "Nice Names"
    # ---------------------------------------------------------
    column_mapping = {
        # Group 1
        'initiator': 'Circle Head Name',
        'qacajobid': 'QACA Job ID',
        'odooid': 'Odoo ID',
        'siteid': 'Site ID',
        'sitename': 'Site Name',
        'circle': 'Circle',
        'client': 'Client',
        'activitytype': 'Activity Type',
        'rateofaudit': 'Rate of Audit',
        
        # Group 2
        'allocationtype': 'Allocation Type',
        'assignsurveycoordinator': 'Assigned Survey Co-ordinator',
        'surveyengineername': 'Assigned Survey Engineer',
        'surveyupload-clientportal': 'Survey Upload Client Portal',
        'issurveyincluded': 'Is Survey Included',
        'surveytargetdate': 'Survey Target Date',
        'surveysenttodesign': 'Survey Sent To Design',
        'allotmentdate': 'Allocation Date',
        'allocationdate': 'Allocation Date',
        'finalduedate': 'Final Due Date',
        'assigndesignlead': 'Assigned Design Lead',
        'actualsurveydate': 'Actual Survey Date',
        'surveydatereceived': 'Survey Date Received',
        'reportcategoryonlyforbfs': 'Report Category Only For BFS',
        
        # Group 3
        'assignsurveyvalidator': 'Survey Validator',
        'timetaken': 'Time Taken - Survey Validator',
        'surveyvalidationdate': 'Survey Validation Date',
        
        'assigndesignstr': 'Design Engineer Str',
        'timetaken3': 'Time Taken - Design Engineer Str',
        'reportcompletiondate': 'Report Completion Date Design Eng Str',
        
        'assigndesignengineer': 'Design Engineer',
        'timetaken0': 'Time Taken - Design Engineer',
        'reportcompletiondate1': 'Report Completion Date',
        
        'assigndraftpersonlayout': 'Draftperson Layout',
        'timetaken2': 'Time Taken - Draftperson Layout',
        'layoutpreparationdate': 'Layout Preparation Date',
        
        'assigndraftpersonstrdetail': 'Draftperson Str/Detail',
        'timetaken1': 'Time Taken - Draftperson Str/Detail',
        'strdetailcompletiondate': 'STR /Detail Completion Date',
        
        'surveyftr': 'Survey-FTR',
        
        # Group 4
        'assignqualitychecker': 'Assigned Quality Checker',
        'reviewcompletiondate': 'Review Completion Date',
        'reasonbehindtatinlay': 'Reason Behind Delay in TAT',
        'sitestatus': 'Site Status',
        'approvalstatus': 'Approval Status',
        'workdonein': 'Work Done In',
        'mailsenttocirclehead': 'Mail sent to Circle Head',
        'rejectioncomment': 'Rejection By Design Lead',
        'remarkfromdesigncoordinator': 'Remark from Design coordinator',
        'rejectionreason': 'Rejection By Quality Checker',

        #Group 5
        " assigndesignstrlead1": "Assign Design STR-Lead",
        'assigndesignstr1': 'Assigned Design-STR',
        'timetaken3': 'Time Taken',
        'reportcompletiondate': 'Report Completion Date',
        'strapprovalstatus': 'Design STR Approval',

        #Group 6
        "assigndraftpersonstrdetaillead1": "Assigned DraftPerson STR/Detail-Lead",
        'assigndraftpersondetail': 'Assigned Draftperson STR/Detail',
        'timetaken1': 'Time Taken',
        'strdetailcompletiondate': 'STR/Detail Completion Date',
        'approvalstatus': 'Approval Status',
       
    }

    df_final.rename(columns=column_mapping, inplace=True)

    # ---------------------------------------------------------
    # Filter Columns: Only keep Process Instance info and mapped columns
    # ---------------------------------------------------------
    
    # 1. Define the exact ordered list of columns to output
    final_columns = [
        # Process Instance Info
        "Process Instance ID", "Start Date", "End Date", "Status",
        
        # Mapped Variables
        "Circle Head Name", "QACA Job ID", "Odoo ID", "Site ID", "Site Name", 
        "Circle", "Client", "Activity Type", "Rate of Audit",
        
        "Allocation Type", "Assigned Survey Co-ordinator", "Assigned Survey Engineer", 
        "Survey Upload Client Portal", "Is Survey Included", "Survey Target Date", 
        "Survey Sent To Design", "Allocation Date", "Final Due Date", "Assigned Design Lead", 
        "Actual Survey Date", "Survey Date Received", "Report Category Only For BFS",
        
        "Survey Validator", "Time Taken - Survey Validator", "Survey Validation Date",
        
        "Design Engineer Str", "Time Taken - Design Engineer Str", "Report Completion Date Design Eng Str",
        
        "Design Engineer", "Time Taken - Design Engineer", "Report Completion Date",
        
        "Draftperson Layout", "Time Taken - Draftperson Layout", "Layout Preparation Date",
        
        "Draftperson Str/Detail", "Time Taken - Draftperson Str/Detail", "STR /Detail Completion Date",
        
        "Survey-FTR",
        
        "Assigned Quality Checker", "Review Completion Date", "Reason Behind Delay in TAT", 
        "Site Status", "Approval Status", "Work Done In", "Mail sent to Circle Head",
        
        "Rejection By Design Lead", "Remark from Design coordinator", "Rejection By Quality Checker",
        "Assign Design STR-Lead", "Assigned Design-STR", "Time Taken", "Report Completion Date", "Design STR Approval",
        "Assigned DraftPerson STR/Detail-Lead", "Assigned Draftperson STR/Detail", "Time Taken", "STR/Detail Completion Date", "Approval Status"

    ]

    # 2. Add missing columns with "N/A"
    for col in final_columns:
        if col not in df_final.columns:
            df_final[col] = "N/A"

    # 3. Filter and reorder
    df_final = df_final[final_columns]

    # Fill empty values with N/A
    df_final.fillna("N/A", inplace=True)
    df_final.replace("", "N/A", inplace=True)

    return df_final


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
