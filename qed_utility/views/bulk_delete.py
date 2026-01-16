import logging
import os
import time
import pandas as pd
import requests

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from qed_utility.access import role_required


logger = logging.getLogger(__name__)

# ================= CONFIG =================
FLOWABLE_BASE = os.getenv("FLOWABLE_BASE")
FLOWABLE_USER = os.getenv("FLOWABLE_USER")
FLOWABLE_PASS = os.getenv("FLOWABLE_PASS")

COLUMN_NAME = "process_instance_id"
DELAY_SECONDS = 0.3

REPORT_FILE = "delete_report.xlsx"
# =========================================


# ================= FLOWABLE DELETE =================
def delete_runtime(instance_id):
    url = f"{FLOWABLE_BASE}/process-api/runtime/process-instances/{instance_id}"
    try:
        return requests.delete(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            params={"deleteReason": "BulkCleanup"},
            timeout=60
        )
    except requests.RequestException as e:
        logger.error(f"Runtime delete connection error for {instance_id}: {e}")
        return None


def delete_history(instance_id):
    url = f"{FLOWABLE_BASE}/process-api/history/historic-process-instances/{instance_id}"
    try:
        return requests.delete(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=60
        )
    except requests.RequestException as e:
        logger.error(f"History delete connection error for {instance_id}: {e}")
        return None


def delete_process_instance(instance_id):
    # 1️⃣ Try runtime delete
    r = delete_runtime(instance_id)

    if r and r.status_code in (200, 204):
        return True, "DELETED_RUNTIME"

    if r and r.status_code not in (404,):
        return False, f"RUNTIME_ERROR: {r.text}"

    # 2️⃣ Try history delete
    h = delete_history(instance_id)

    if h and h.status_code in (200, 204):
        return True, "DELETED_HISTORY"

    if h and h.status_code == 404:
        return False, "NOT_FOUND"

    if h:
        return False, f"HISTORY_ERROR: {h.text}"
    
    return False, "CONNECTION_ERROR"


# ================= VIEWS =================

@role_required("designcoordinator")
def bulk_delete(request):
    """Render delete page"""
    logger.info(f"User '{request.user.username}' (ID: {request.user.id}) accessed bulk delete page.")
    return render(request, "qed_utility/delete.html")


@role_required("designcoordinator")
@csrf_exempt
def bulk_delete_execute(request):
    if request.method != "POST" or "file" not in request.FILES:
        return JsonResponse({"error": "Excel file is required"}, status=400)

    try:
        df = pd.read_excel(request.FILES["file"])
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return JsonResponse({"error": "Invalid Excel file"}, status=400)

    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    target_col = COLUMN_NAME.lower()

    if target_col not in df.columns:
        return JsonResponse(
            {"error": f"Missing column: {COLUMN_NAME}"},
            status=400
        )
    
    # Log the attempt
    logger.info(f"User '{request.user.username}' (ID: {request.user.id}) started bulk delete for {len(df)} rows.")

    df[target_col] = df[target_col].astype(str).str.strip()
    df = df[df[target_col].str.lower() != "nan"]

    total = len(df)
    success = 0
    failed = 0
    results = []

    for _, row in df.iterrows():
        instance_id = row[target_col]

        try:
            ok, status = delete_process_instance(instance_id)

            if ok:
                success += 1
                results.append({
                    "process_instance_id": instance_id,
                    "status": status
                })
            else:
                failed += 1
                results.append({
                    "process_instance_id": instance_id,
                    "status": "FAILED",
                    "reason": status
                })

        except Exception as e:
            failed += 1
            logger.exception(f"Exception deleting {instance_id}")
            results.append({
                "process_instance_id": instance_id,
                "status": "ERROR",
                "reason": str(e)
            })

        time.sleep(DELAY_SECONDS)

    # Log completion
    logger.info(f"Bulk delete completed. Total: {total}, Success: {success}, Failed: {failed}")

    return JsonResponse({
        "total": total,
        "deleted": success,
        "failed": failed,
        "results": results
    })   
