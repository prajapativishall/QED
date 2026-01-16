import os
import json
import logging
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required

from qed_utility.access import role_required

# ================= CONFIG =================
FLOWABLE_BASE = os.getenv("FLOWABLE_BASE")
FLOWABLE_USER = os.getenv("FLOWABLE_USER")
FLOWABLE_PASS = os.getenv("FLOWABLE_PASS")

PROCESS_KEY = "multilevelapproval"
FORM_DEFINITION_ID = os.getenv("FORM_DEFINITION_ID")

MAX_ROWS = 200
MAX_PARALLEL = 5
REQUEST_DELAY = 0.2

semaphore = Semaphore(MAX_PARALLEL)
logger = logging.getLogger(__name__)

# ================= VIEWS =================

@role_required("Circlecoordinator", "Surveycoordinator", "designcoordinator")
def bulk_upload_view(request):
    logger.info(f"User '{request.user.username}' (ID: {request.user.id}) accessed bulk upload page.")
    return render(request, "qed_utility/upload.html")

@csrf_exempt
@role_required("Circlecoordinator", "Surveycoordinator", "designcoordinator")
def validate_excel_view(request):
    if request.method != "POST":
        return JsonResponse({"valid": False, "error": "Method not allowed"}, status=405)
    
    file = request.FILES.get("file")
    if not file:
        return JsonResponse({"valid": False, "error": "No file uploaded"})
    
    logger.info(f"User '{request.user.username}' (ID: {request.user.id}) uploaded file '{file.name}' for validation.")
    
    try:
        result = validate_excel(file)
        return JsonResponse(result)
    except Exception as e:
        logger.exception("Validation error")
        return JsonResponse({"valid": False, "error": str(e)})

@csrf_exempt
@role_required("admin", "Circlecoordinator", "Surveycoordinator")
def bulk_start_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)
    
    try:
        data = json.loads(request.body)
        rows = data.get("rows", [])
        
        logger.info(f"User '{request.user.username}' (ID: {request.user.id}) initiating bulk start for {len(rows)} items.")

        if not rows:
            return JsonResponse({"started": 0, "failed": 0})
            
        success_log = []
        fail_log = []
        
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
            # Pass full row so we can get qacajobid
            future_to_row = {executor.submit(start_process_instance, row): row for row in rows}
            
            for future in as_completed(future_to_row):
                row_data = future_to_row[future]
                job_id = row_data.get("qacajobid", "Unknown")
                try:
                    is_success, msg = future.result()
                    if is_success:
                        success_log.append(job_id)
                    else:
                        fail_log.append({"id": job_id, "error": msg})
                except Exception as e:
                    fail_log.append({"id": job_id, "error": str(e)})
                    
        return JsonResponse({
            "started": len(success_log),
            "failed": len(fail_log),
            "success_log": success_log,
            "fail_log": fail_log
        })
        
    except Exception as e:
        logger.exception("Bulk start error")
        return JsonResponse({"error": str(e)}, status=500)

# ================= HELPERS =================

def fetch_form_model():
    if not FORM_DEFINITION_ID:
        raise ValueError("FORM_DEFINITION_ID environment variable is not set")
        
    url = f"{FLOWABLE_BASE}/form-api/form-repository/form-definitions/{FORM_DEFINITION_ID}/model"
    try:
        r = requests.get(url, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch form model: {e}")
        # Return a dummy structure or re-raise depending on strictness
        # For now re-raise to fail validation early
        raise

def extract_rules(form_json):
    dropdowns = {}
    required = set()

    for field in form_json.get("fields", []):
        fid = field.get("id")
        
        if field.get("required"):
            required.add(fid)
            
        if field.get("fieldType") == "OptionFormField":
            dropdowns[fid] = {
                opt.get("name").strip().lower() 
                for opt in field.get("options", []) 
                if opt.get("name")
            }
            
    return dropdowns, required

def validate_excel(file_obj):
    try:
        df = pd.read_excel(file_obj)
    except Exception as e:
        return {"valid": False, "error": "Invalid Excel file format"}

    if df.empty or df.dropna(how="all").empty:
        return {"valid": False, "error": "Excel file is empty"}

    if len(df) > MAX_ROWS:
        return {"valid": False, "error": f"Maximum {MAX_ROWS} rows allowed"}

    # Normalize column names to lowercase for consistent checking
    df.columns = df.columns.str.lower()
    
    if "qacajobid" not in df.columns:
        return {"valid": False, "error": "Missing required column: qacajobid"}

    try:
        form = fetch_form_model()
    except Exception as e:
        return {"valid": False, "error": f"Could not fetch form definition: {str(e)}"}
        
    dropdowns, required = extract_rules(form)
    
    # Map required fields to lowercase for comparison if needed, 
    # but Flowable IDs are case sensitive usually. 
    # Let's assume Excel headers match Flowable field IDs (case-insensitive?).
    # For now, we'll try exact match first, then case-insensitive.
    
    errors = []
    valid_rows = []

    for idx, row in df.iterrows():
        row_errors = []
        row_no = idx + 2 # Excel row number (1-based + header)
        
        # Helper to get value case-insensitively
        def get_val(field_id):
            if field_id.lower() in df.columns:
                return row[field_id.lower()]
            return None

        # Check required fields
        for field in required:
            val = get_val(field)
            if pd.isna(val) or str(val).strip() == "":
                row_errors.append(f"Missing required field: {field}")

        # Check dropdowns
        for field, allowed_values in dropdowns.items():
            val = get_val(field)
            if not pd.isna(val) and str(val).strip() != "":
                str_val = str(val).strip().lower()
                if str_val not in allowed_values:
                    row_errors.append(f"Invalid value '{val}' for {field}")

        if row_errors:
            errors.append({
                "row": row_no,
                "qacajobid": row.get("qacajobid", "N/A"),
                "errors": row_errors
            })
        else:
            # Construct payload for this row
            # We convert the row to a dict, ensuring we match Flowable field IDs
            payload_vars = {}
            for col in df.columns:
                # Simple mapping: Use column name as variable name
                # In a real app, you might need a mapping strategy
                val = row[col]
                if pd.notna(val):
                    payload_vars[col] = str(val)
            
            valid_rows.append(payload_vars)

    if errors:
        return {"valid": False, "errors": errors}
    
    return {"valid": True, "rows": valid_rows}

def start_process_instance(variables):
    with semaphore:
        url = f"{FLOWABLE_BASE}/process-api/runtime/process-instances"
        
        payload = {
            "processDefinitionKey": PROCESS_KEY,
            "variables": [
                {"name": k, "value": v} for k, v in variables.items()
            ],
            "returnVariables": False
        }
        
        try:
            r = requests.post(
                url, 
                json=payload, 
                auth=(FLOWABLE_USER, FLOWABLE_PASS),
                timeout=10
            )
            r.raise_for_status()
            return True, "OK"
        except Exception as e:
            err_msg = str(e)
            logger.error(f"Failed to start process for {variables.get('qacajobid', 'Unknown')}: {err_msg}")
            return False, err_msg
