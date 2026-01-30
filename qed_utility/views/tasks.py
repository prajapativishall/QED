import os
import re
from datetime import datetime
from urllib.parse import quote

import mysql.connector
import requests
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpRequest, HttpResponse, Http404, StreamingHttpResponse
from django.shortcuts import render, redirect

from qed_utility.views.dashboard import get_user_task_stats, DB_CONFIG, get_user_groups, get_flowable_users, CIRCLE_LIST, ACTIVITY_LIST


FLOWABLE_BASE = os.getenv("FLOWABLE_BASE") or ""
FLOWABLE_USER = os.getenv("FLOWABLE_USER")
FLOWABLE_PASS = os.getenv("FLOWABLE_PASS")


def _load_task_detail(user_id: str, task_id: str) -> dict | None:
    with mysql.connector.connect(**DB_CONFIG) as conn:
        cursor = conn.cursor()
        
        # 1. Fetch Task Info (including Assignee)
        cursor.execute(
            """
            SELECT T.ID_, T.NAME_, T.START_TIME_, T.END_TIME_, T.PROC_INST_ID_, T.ASSIGNEE_
            FROM ACT_HI_TASKINST T
            WHERE T.ID_ = %s
            """,
            (task_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
            
        t_id, name, start_time, end_time, proc_inst_id, assignee = row
        
        # 2. Check Permissions (Assignee or Candidate)
        is_assignee = (assignee == user_id)
        can_claim = False
        
        if not is_assignee:
            if assignee:
                # Task is assigned to someone else. 
                # Strict privacy: Don't show.
                # Or maybe show if user is admin/manager? For now, strict.
                return None
            else:
                # Task is unassigned. Check if user is a candidate.
                groups = get_user_groups(user_id)
                
                # Check identity links
                # We need to check for user candidacy OR group candidacy
                query_parts = ["USER_ID_ = %s"]
                params = [task_id, user_id]
                
                if groups:
                    placeholders = ', '.join(['%s'] * len(groups))
                    query_parts.append(f"GROUP_ID_ IN ({placeholders})")
                    params.extend(groups)
                
                condition = " OR ".join(query_parts)
                
                # Check both Runtime and History identity links just in case, 
                # but for claiming, runtime is what matters. ACT_HI_IDENTITYLINK usually has a copy.
                # We check ACT_RU_IDENTITYLINK first as it is the source of truth for active tasks.
                
                # Check ACT_RU_IDENTITYLINK
                query_ru = f"""
                    SELECT 1 FROM ACT_RU_IDENTITYLINK
                    WHERE TASK_ID_ = %s 
                      AND TYPE_ = 'candidate'
                      AND ({condition})
                    LIMIT 1
                """
                
                try:
                    cursor.execute(query_ru, tuple(params))
                    if cursor.fetchone():
                        can_claim = True
                except Exception:
                    # Table might not exist or other error, proceed to history check
                    pass

                if not can_claim:
                    # Check ACT_HI_IDENTITYLINK as fallback
                    query = f"""
                        SELECT 1 FROM ACT_HI_IDENTITYLINK
                        WHERE TASK_ID_ = %s 
                          AND TYPE_ = 'candidate'
                          AND ({condition})
                        LIMIT 1
                    """
                    
                    cursor.execute(query, tuple(params))
                    if cursor.fetchone():
                        can_claim = True
                
                if not can_claim:
                    # Not a candidate
                    return None
        
        # Fetch variables (Process and Task scope)
        cursor.execute(
            """
            SELECT V.NAME_, V.VAR_TYPE_, V.TEXT_, V.LONG_, V.DOUBLE_, BA.BYTES_, V.TASK_ID_
            FROM ACT_HI_VARINST V
            LEFT JOIN ACT_GE_BYTEARRAY BA ON V.BYTEARRAY_ID_ = BA.ID_
            WHERE V.PROC_INST_ID_ = %s OR V.TASK_ID_ = %s
            """,
            (proc_inst_id, t_id),
        )
        variables_dict = {}
        for v_name, v_type, v_text, v_long, v_double, v_bytes, v_task_id in cursor.fetchall():
            value = None
            if (v_type == "date" or v_type == "jodadate") and v_long is not None:
                # Convert timestamp (ms) to YYYY-MM-DD
                try:
                    dt = datetime.fromtimestamp(v_long / 1000.0)
                    value = dt.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    value = v_long
            elif v_type == "serializable" and v_bytes:
                # Try to parse Joda LocalDate from binary
                # Pattern: Header ... org.joda.time.LocalDate ... 0x78 0x70 [8 bytes long]
                try:
                    # Look for the marker 0x78 0x70 (TC_ENDBLOCKDATA, TC_NULL)
                    # This usually precedes the field values in default serialization
                    marker_index = v_bytes.find(b'\x78\x70')
                    if marker_index != -1 and marker_index + 10 <= len(v_bytes):
                        # Read 8 bytes after marker
                        timestamp_bytes = v_bytes[marker_index+2 : marker_index+10]
                        timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=True)
                        
                        # Check if it looks like a reasonable timestamp (milliseconds)
                        # Joda LocalDate stores millis from epoch for the date
                        # e.g. 2026 is around 1.76e12
                        if 1000000000000 < timestamp < 3000000000000:
                            dt = datetime.fromtimestamp(timestamp / 1000.0)
                            value = dt.strftime("%Y-%m-%d")
                except Exception as e:
                    print(f"Error parsing serializable variable {v_name}: {e}")
            
            if value is None:
                if v_text is not None:
                    value = v_text
                elif v_long is not None:
                    value = v_long
                elif v_double is not None:
                    value = v_double

            # If we still have None, and it was a serializable with bytes, 
            # it means we failed to parse it (or don't know how). 
            # We should NOT include this in the variables list to avoid 
            # overwriting valid Form API data with None.
            if value is None and v_type == "serializable" and v_bytes:
                 continue

            # Priority: Task variable > Process variable
            # If we already have this variable, checks if the new one is task-scoped (priority)
            # or if the existing one was process-scoped.
            # However, simpler logic:
            # If v_task_id is set, it's a task variable. It should overwrite process variable.
            # If v_task_id is None, it's a process variable. Only set if not already set.
            
            is_task_var = (v_task_id == t_id)
            
            if v_name not in variables_dict or is_task_var:
                 variables_dict[v_name] = {
                    "name": v_name,
                    "value": value,
                }

        variables = list(variables_dict.values())

        # Fetch content items
        content_items = []
        try:
            cursor.execute(
                """
                SELECT ID_, NAME_, MIME_TYPE_, CREATED_, CREATED_BY_, FIELD_
                FROM ACT_CO_CONTENT_ITEM
                WHERE PROC_INST_ID_ = %s
                ORDER BY CREATED_ DESC
                """,
                (proc_inst_id,),
            )
            for c_id, c_name, c_mime, c_created, c_created_by, c_field in cursor.fetchall():
                content_items.append({
                    "id": c_id,
                    "name": c_name,
                    "mime_type": c_mime,
                    "created": c_created,
                    "created_by": c_created_by,
                    "field": c_field
                })
        except Exception as e:
            print(f"Error fetching content items: {e}")

        status = "Completed" if end_time else "Pending"
        return {
            "id": t_id,
            "name": name,
            "status": status,
            "start": start_time,
            "end": end_time,
            "proc_inst_id": proc_inst_id,
            "variables": variables,
            "content_items": content_items,
            "assignee": assignee,
            "can_claim": can_claim,
            "is_assignee": is_assignee,
        }


def _fetch_task_form(task_id: str) -> dict | None:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return None
    url = f"{base}/process-api/runtime/tasks/{task_id}/form"
    try:
        r = requests.get(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching form data for task {task_id}: {e}")
        return None


def _fetch_historic_task_form(task_id: str) -> dict | None:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return None
    url = f"{base}/process-api/history/historic-task-instances/{task_id}/form"
    try:
        r = requests.get(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching historic form data for task {task_id}: {e}")
        return None


def _submit_task_form(
    task_id: str, fields: list[dict], outcome: str | None = None
) -> tuple[bool, str]:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return False, "Flowable base URL is not configured"
    
    # Use process-api instead of form-api to ensure compatibility with standard Flowable UI/REST
    url = f"{base}/process-api/form/form-data"
    
    payload = {
        "taskId": task_id,
        "properties": fields,
    }
    if outcome:
        payload["outcome"] = outcome

    try:
        r = requests.post(
            url,
            json=payload,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        
        # Fallback: if process-api 404s, try form-api (unlikely but safe)
        if r.status_code == 404:
             url_form = f"{base}/form-api/form-data"
             r = requests.post(
                url_form,
                json=payload,
                auth=(FLOWABLE_USER, FLOWABLE_PASS),
                timeout=30,
             )

        r.raise_for_status()
        return True, ""
    except requests.RequestException as e:
        return False, str(e)


def _fetch_process_definitions() -> list[dict]:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return []
    url = f"{base}/process-api/repository/process-definitions"
    params = {"latest": "true", "suspended": "false", "sort": "name"}
    try:
        r = requests.get(
            url,
            params=params,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("data", [])
    except requests.RequestException as e:
        print(f"Error fetching process definitions: {e}")
        return []


def _fetch_process_definition_details(process_definition_id: str) -> dict | None:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return None
    safe_id = quote(process_definition_id)
    url = f"{base}/process-api/repository/process-definitions/{safe_id}"
    try:
        r = requests.get(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching process definition details {process_definition_id}: {e}")
        return None


def _fetch_start_form(process_definition_id: str) -> dict | None:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return None
    safe_id = quote(process_definition_id)
    url = f"{base}/process-api/repository/process-definitions/{safe_id}/start-form"
    try:
        r = requests.get(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching start form for process {process_definition_id}: {e}")
        return None


def _submit_start_form(
    process_definition_id: str,
    fields: list[dict] | None = None,
    outcome: str | None = None,
    user_id: str | None = None,
) -> tuple[bool, str, str | None]:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return False, "Flowable base URL is not configured", None

    # Always use process-api to support variables (initiator)
    # We convert form fields (properties) to variables
    url = f"{base}/process-api/runtime/process-instances"
    
    variables = []
    
    # 1. Add form fields as variables
    if fields:
        for f in fields:
            variables.append({
                "name": f["id"],
                "value": f["value"]
            })
            
    # 2. Add initiator and startUserId if user_id is provided
    if user_id:
        variables.append({"name": "initiator", "value": user_id})
        # Try to set startUserId via variable (some implementations use this)
        variables.append({"name": "startUserId", "value": user_id})
        
    payload = {
        "processDefinitionId": process_definition_id,
        "returnVariables": True,
        "variables": variables,
        "startUserId": user_id,  # Try setting this directly in payload as well
    }
    
    # Note: 'outcome' is typically not used in process-api start payload 
    # unless mapped to a variable. We'll ignore it for now as start events 
    # usually don't have multiple outcomes like tasks.

    try:
        r = requests.post(
            url,
            json=payload,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30,
        )
        if r.status_code >= 400:
             try:
                 err_msg = r.json().get("message", r.text)
             except:
                 err_msg = r.text
             return False, err_msg, None
        
        r.raise_for_status()
        data = r.json()
        pid = data.get("id")

        # Post-start fixup: Ensure initiator variable and task assignment are correct
        # because Flowable might have used the authenticated user (admin) despite startUserId.
        if user_id and pid:
            try:
                # 1. Force update 'initiator' variable
                var_url = f"{base}/process-api/runtime/process-instances/{pid}/variables"
                requests.put(
                    var_url,
                    json=[{"name": "initiator", "value": user_id}],
                    auth=(FLOWABLE_USER, FLOWABLE_PASS),
                    timeout=10
                )

                # 2. Reassign any tasks currently assigned to admin (FLOWABLE_USER)
                tasks_url = f"{base}/process-api/runtime/tasks"
                t_r = requests.get(
                    tasks_url,
                    params={"processInstanceId": pid},
                    auth=(FLOWABLE_USER, FLOWABLE_PASS),
                    timeout=10
                )
                if t_r.status_code == 200:
                    for task in t_r.json().get("data", []):
                        # If assigned to the admin (authenticated user), reassign to the actual starter
                        if task.get("assignee") == FLOWABLE_USER:
                             t_update_url = f"{base}/process-api/runtime/tasks/{task['id']}"
                             requests.put(
                                 t_update_url,
                                 json={"assignee": user_id},
                                 auth=(FLOWABLE_USER, FLOWABLE_PASS),
                                 timeout=10
                             )
            except Exception as e:
                print(f"Error during process start fixup: {e}")

        return True, "", pid
    except requests.RequestException as e:
        return False, str(e), None


@login_required
def my_tasks_api(request: HttpRequest) -> JsonResponse:
    flowable_user_id = request.user.username
    stats = get_user_task_stats(flowable_user_id)
    return JsonResponse(stats)


@login_required
def my_tasks_view(request: HttpRequest):
    flowable_user_id = request.user.username
    stats = get_user_task_stats(flowable_user_id)
    tasks = stats.get("tasks", [])
    summary = stats.get("summary", {})
    status_filter = request.GET.get("status", "").lower()
    name_query = request.GET.get("name", "").strip()
    site_query = request.GET.get("site", "").strip()
    activity_query = request.GET.get("activity", "").strip()
    filtered_tasks = list(tasks)
    if status_filter == "pending":
        filtered_tasks = [t for t in filtered_tasks if t.get("status") == "Pending"]
    elif status_filter == "completed":
        filtered_tasks = [t for t in filtered_tasks if t.get("status") == "Completed"]
    else:
        status_filter = ""
    if name_query:
        q = name_query.lower()
        filtered_tasks = [t for t in filtered_tasks if q in (t.get("name") or "").lower()]
    if site_query:
        q = site_query.lower()
        filtered_tasks = [t for t in filtered_tasks if q in (t.get("siteid") or "").lower()]
    if activity_query:
        q = activity_query.lower()
        filtered_tasks = [t for t in filtered_tasks if q in (t.get("activity") or "").lower()]
    has_filters = bool(status_filter or name_query or site_query or activity_query)

    return render(
        request,
        "qed_utility/tasks.html",
        {
            "tasks": filtered_tasks,
            "summary": summary,
            "current_filter": status_filter,
            "filters": {
                "name": name_query,
                "site": site_query,
                "activity": activity_query,
            },
            "has_filters": has_filters,
        },
    )


@login_required
def process_definitions_api(request: HttpRequest) -> JsonResponse:
    definitions = _fetch_process_definitions()
    return JsonResponse({"data": definitions})


@login_required
def task_detail_api(request: HttpRequest, task_id: str) -> JsonResponse:
    flowable_user_id = request.user.username
    detail = _load_task_detail(flowable_user_id, task_id)
    if not detail:
        return JsonResponse({"error": "Task not found"}, status=404)
    return JsonResponse(detail)


def _fetch_form_model_layout(task_id: str, form_def_id: str | None = None) -> dict | None:
    """
    Fetches the form model (layout) for a task using the Flowable Form API.
    Returns the JSON model containing rows/cols/fields structure.
    """
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return None
        
    try:
        # If form_def_id is not provided, try to find it
        if not form_def_id:
            # 1. Get Task details from Form API to find formDefinitionId
            # Note: This works if the task was created via Form Engine or synced to it.
            # If it's a pure Process Engine task, it might not exist in form-runtime/tasks if not using Form Engine.
            # But we can try process-api task to get formKey/formDefinitionId too.
            
            # Try process-api first as it's more reliable for process tasks
            url_proc_task = f"{base}/process-api/runtime/tasks/{task_id}"
            r = requests.get(url_proc_task, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=10)
            if r.status_code == 200:
                t_data = r.json()
                # In Process API, it's usually formKey. 
                # But we need formDefinitionId for the model endpoint.
                # If the process uses a form reference, formKey is the key.
                # We need to resolve formKey to formDefinitionId.
                form_key = t_data.get("formKey")
                
                if form_key:
                    # Resolve formKey to latest form definition
                    # GET /form-api/form-repository/form-definitions?key={formKey}&latest=true
                    url_resolve = f"{base}/form-api/form-repository/form-definitions"
                    r_res = requests.get(
                        url_resolve, 
                        params={"key": form_key, "latest": "true"},
                        auth=(FLOWABLE_USER, FLOWABLE_PASS),
                        timeout=10
                    )
                    if r_res.status_code == 200:
                        data = r_res.json()
                        if data.get("data"):
                            form_def_id = data["data"][0]["id"]
            
            if not form_def_id:
                # Fallback: Try form-runtime/tasks
                url_form_task = f"{base}/form-api/form-runtime/tasks/{task_id}"
                r = requests.get(url_form_task, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=10)
                if r.status_code == 200:
                    form_def_id = r.json().get("formDefinitionId")

        if form_def_id:
            # 2. Fetch Form Model
            # Try multiple paths for robustness
            paths = [
                f"{base}/form-api/form-repository/form-definitions/{form_def_id}/model",
                f"{base}/process-api/form-repository/form-definitions/{form_def_id}/model",
                f"{base}/app-api/form-repository/form-definitions/{form_def_id}/model"
            ]
            
            for url_model in paths:
                r_model = requests.get(url_model, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
                if r_model.status_code == 200:
                    return r_model.json()
                
    except Exception as e:
        print(f"Error fetching form model layout for task {task_id}: {e}")
        
    return None


def _populate_model_values(model: dict, values_map: dict):
    """
    Recursively populate values into the form model fields from a values map.
    """
    if not model:
        return
    
    # Create normalized map for case-insensitive lookup
    # Also create a map with stripped underscores/hyphens for fuzzy lookup
    normalized_map = {}
    fuzzy_map = {}
    
    print(f"DEBUG: values_map keys: {list(values_map.keys())}")
    
    for k, v in values_map.items():
        # Handle dict values (if complex object) - Extract a useful string
        if isinstance(v, dict):
             v_str = v.get("id") or v.get("value") or v.get("name") or str(v)
             values_map[k] = v_str # Update original map too for direct lookups
             v = v_str

        k_lower = k.lower()
        normalized_map[k_lower] = v
        # Update fuzzy map to strip spaces too
        fuzzy_map[k_lower.replace("_", "").replace("-", "").replace(" ", "")] = v

    # Helper to process a list of fields
    def process_fields(field_list):
        for field in field_list:
            f_id = field.get("id")
            f_name = field.get("name", "")
            # print(f"DEBUG: Processing field {f_id} (Name: {f_name}, Type: {field.get('type')})")
            
            if f_id:
                # Debug specific fields of interest
                if "circle" in f_id.lower() or "client" in f_id.lower() or "date" in f_id.lower():
                    print(f"DEBUG: Found relevant field ID: {f_id}, Type: {field.get('type')}, Current Value: {field.get('value')}")
            
            # --- OPTIONS POPULATION (Strictly for Dropdowns) ---
            # Ensure options are populated for known dropdown fields if missing
            fid_lower = f_id.lower() if f_id else ""
            fname_lower = f_name.lower() if f_name else ""
            
            if field.get("type") in ["dropdown", "select", "radio-buttons"]:
                if "circle" in fid_lower and ("options" not in field or not field["options"]):
                    field["options"] = [{"name": c, "id": c} for c in CIRCLE_LIST]
                    print(f"DEBUG: Populated options for circle field {f_id}")
                
                elif "activity" in fid_lower and ("options" not in field or not field["options"]):
                    field["options"] = [{"name": c, "id": c} for c in ACTIVITY_LIST]
                
                elif "client" in fid_lower and ("options" not in field or not field["options"]):
                     field["options"] = [{"name": c, "id": c} for c in ["Indus", "ATC", "Sitel", "Other"]]
                
                elif "allocation" in fid_lower and ("options" not in field or not field["options"]):
                     field["options"] = [{"name": c, "id": c} for c in ["Single", "Bulk", "Auto"]]

                # Fix for "Forward" field showing "Option 1" or generic options
                # Check ID OR Name (Label) for "Forward"
                elif ("forward" in fid_lower or "outcome" in fid_lower or "forward" in fname_lower) and field.get("type") in ["dropdown", "select", "radio-buttons"]:
                     # Check if options are missing OR contain generic "Option 1"
                     has_generic = False
                     if field.get("options"):
                         for opt in field["options"]:
                             if "option 1" in str(opt.get("name", "")).lower():
                                 has_generic = True
                                 break
                     
                     if "options" not in field or not field["options"] or has_generic:
                         field["options"] = [
                             {"name": "Yes", "id": "Yes"},
                             {"name": "No", "id": "No"}
                         ]
                         # Also clear generic value if present (so "Option 1" isn't selected)
                         if "option 1" in str(field.get("value", "")).lower():
                             field["value"] = None
                         
                         print(f"DEBUG: Forced Yes/No options for forward field {f_id} (was missing or generic)")

            # --- FORCE FILL LOGIC FOR KNOWN FIELDS (Values Only) ---
            # Restoring heuristic mapping for values to fix "blank fields" issue.
            # We strictly DO NOT touch 'options' here to avoid "unnecessary options" bug.
            if not field.get("value") and f_id:
                fid_lower = f_id.lower()
                
                # Circle
                if "circle" in fid_lower:
                    if "circle" in normalized_map:
                        field["value"] = normalized_map["circle"]
                        print(f"DEBUG: Heuristic match: {f_id} -> circle = {field['value']}")
                
                # Client
                elif "client" in fid_lower:
                    for v_name in ["client", "clientname", "customer", "vendor"]:
                        if v_name in normalized_map:
                            field["value"] = normalized_map[v_name]
                            print(f"DEBUG: Heuristic match: {f_id} -> {v_name} = {field['value']}")
                            break
                            
                # Date (Allotment/Survey)
                elif "date" in fid_lower:
                    # Refined Heuristic: Match specific date types
                    target_vars = []
                    if "survey" in fid_lower:
                        # Avoid aggressive matching like "actualsurveydate" for "surveydatereceived"
                        target_vars = ["surveydate"] 
                    elif "allotment" in fid_lower or "allocation" in fid_lower:
                        target_vars = ["allotmentdate", "allocationdate"]
                    else:
                        # Fallback for generic "Date" fields? Maybe safer to do nothing or check generic names
                        target_vars = ["date"]

                    for v_name in target_vars:
                        if v_name in normalized_map:
                            field["value"] = normalized_map[v_name]
                            print(f"DEBUG: Heuristic match: {f_id} -> {v_name} = {field['value']}")
                            break

                # Activity Type
                elif "activity" in fid_lower:
                    if "activitytype" in normalized_map:
                         field["value"] = normalized_map["activitytype"]
                         print(f"DEBUG: Heuristic match: {f_id} -> activitytype = {field['value']}")
                    elif "activity" in normalized_map:
                         field["value"] = normalized_map["activity"]
                         print(f"DEBUG: Heuristic match: {f_id} -> activity = {field['value']}")

            # -----------------------------------------

            # 1. Standard Value Mapping (Exact match)
            val_found = False

            # Helper to safely set value (ignore None if field already has value)
            def safe_set_value(new_val):
                if new_val is not None:
                    field["value"] = new_val
                    return True
                # If new_val is None, only set if field has no value to begin with
                # This prevents overwriting valid Form API data (e.g. unparseable serializable dates)
                if not field.get("value"):
                    field["value"] = new_val
                    return True
                return False

            if f_id and f_id in values_map:
                if safe_set_value(values_map[f_id]):
                    val_found = True
                    print(f"DEBUG: Mapped {f_id} (exact) -> {values_map[f_id]}")
            # 1b. Case-insensitive fallback
            elif f_id and f_id.lower() in normalized_map:
                if safe_set_value(normalized_map[f_id.lower()]):
                    val_found = True
                    print(f"DEBUG: Mapped {f_id} (case-insensitive) -> {normalized_map[f_id.lower()]}")
            # 1c. Fuzzy fallback (strip _ and - and SPACES)
            elif f_id:
                clean_id = f_id.lower().replace("_", "").replace("-", "").replace(" ", "")
                # Try finding exact fuzzy match
                if clean_id in fuzzy_map:
                    if safe_set_value(fuzzy_map[clean_id]):
                        val_found = True
                        print(f"DEBUG: Mapped {f_id} (fuzzy) -> {fuzzy_map[clean_id]}")
                # Try relaxed heuristic: does any variable name contain this ID (or vice versa)?
                # Only if we haven't found a value yet
                elif not field.get("value"):
                    # Check if any fuzzy variable key is a substring of clean_id or vice versa
                    # This is expensive but might solve "Client Name" vs "client"
                    for fz_key, fz_val in fuzzy_map.items():
                         if len(fz_key) > 3 and (fz_key in clean_id or clean_id in fz_key):
                             field["value"] = fz_val
                             val_found = True
                             print(f"DEBUG: Mapped {f_id} (relaxed fuzzy) -> {fz_val} (match: {fz_key})")
                             break

            # --- CRITICAL: Ensure Value is in Options (for Dropdowns) ---
            # If the value is not in the options, it MUST be added for the dropdown to show it.
            # We strictly limit this to dropdown/select types to avoid polluting other fields.
            if field.get("type") in ["dropdown", "select", "radio-buttons"]:
                if field.get("options") and field.get("value"):
                    curr_val = str(field["value"]).strip().lower()
                    options_vals = [str(opt.get("name", "")).strip().lower() for opt in field["options"]]
                    options_ids = [str(opt.get("id", "")).strip().lower() for opt in field["options"]]
                    
                    if curr_val not in options_vals and curr_val not in options_ids:
                        print(f"DEBUG: Value '{field['value']}' not in options for {f_id}. Appending it.")
                        field["options"].append({"name": field["value"], "id": field["value"]})
            
            # Debug "Forward" field options source
            if "forward" in fid_lower or "outcome" in fid_lower:
                print(f"DEBUG: Field {f_id} (Forward?) Options: {field.get('options')}")
                print(f"DEBUG: Field {f_id} Value: {field.get('value')}")

            # ------------------------------------------------------------

            # 2. Expression Resolution (for ANY string content in value or name)
            # Check 'value' or 'name' for ${variable} pattern
            # Usually expression content is in 'value' for display fields
            
            # Identify content to process
            content_key = "value"
            if field.get("type") in ["expression", "header", "formatted-text"]:
                if not field.get("value") and field.get("name"):
                    content_key = "name"
            
            current_content = field.get(content_key)
            
            if current_content and isinstance(current_content, str) and "${" in current_content:
                # Regex to replace all ${varName}
                def replacer(match):
                    var_name = match.group(1)
                    # Try exact, then lower, then fuzzy
                    val = values_map.get(var_name)
                    if val is None:
                        val = normalized_map.get(var_name.lower())
                    if val is None:
                        clean_var = var_name.lower().replace("_", "").replace("-", "")
                        val = fuzzy_map.get(clean_var, "")
                    return str(val)
                
                try:
                    new_content = re.sub(r'\$\{(.+?)\}', replacer, current_content)
                    field[content_key] = new_content
                    # If we updated 'name' for an expression, also set 'value' so it renders
                    if content_key == "name":
                        field["value"] = new_content
                except Exception as e:
                    print(f"Error resolving expression for field {f_id}: {e}")

            # 3. Date Formatting
            # If it's a date field and we have a value, ensure it's YYYY-MM-DD
            # Check type case-insensitively
            f_type = field.get("type", "").lower()
            if f_type == "date" and field.get("value"):
                val = field["value"]
                # If it's a long timestamp (int or str), convert
                if val:
                    try:
                        # If string looks like int
                        if isinstance(val, str) and val.isdigit():
                            val = int(val)
                            
                        if isinstance(val, int):
                            dt = datetime.fromtimestamp(val / 1000.0)
                            field["value"] = dt.strftime("%Y-%m-%d")
                        elif isinstance(val, str):
                            # Handle "YYYY-MM-DD HH:MM:SS" (space separated)
                            if " " in val:
                                val = val.split(" ")[0]

                            # Try multiple string formats
                            # 1. ISO (2026-01-22T...)
                            if "T" in val:
                                field["value"] = val.split("T")[0]
                            # 2. DD-MM-YYYY or DD/MM/YYYY
                            elif "-" in val or "/" in val:
                                # Try common patterns
                                formatted = False
                                for fmt in ["%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y", "%Y-%m-%d"]:
                                    try:
                                        dt = datetime.strptime(val, fmt)
                                        field["value"] = dt.strftime("%Y-%m-%d")
                                        formatted = True
                                        break
                                    except:
                                        continue
                                
                                # If all failed, maybe it's already YYYY-MM-DD but strptime failed? 
                                # Or maybe it's some other format.
                                # If we stripped time above, we might be good.
                                if not formatted and " " in field["value"]:
                                     field["value"] = field["value"].split(" ")[0]
                    except Exception as e:
                        print(f"Error formatting date for field {f_id}: {e}")
                        pass
                
            # Handle nested layouts if any (e.g. Container)
            if "fields" in field:
                process_fields(field["fields"])
            
            if "rows" in field:
                for row in field["rows"]:
                    for col in row.get("cols", []):
                        if "fields" in col:
                            process_fields(col["fields"])
            
    # 1. Process top-level fields if they exist (sometimes used)
    if "fields" in model:
        process_fields(model["fields"])
        
    # 2. Process rows -> cols -> fields
    if "rows" in model:
        for row in model["rows"]:
            for col in row.get("cols", []):
                if "fields" in col:
                    process_fields(col["fields"])


def _claim_task(task_id: str, user_id: str) -> tuple[bool, str]:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return False, "Flowable base URL is not configured"
    
    # Use process-api
    url = f"{base}/process-api/runtime/tasks/{task_id}"
    payload = {
        "action": "claim",
        "assignee": user_id
    }
    
    try:
        r = requests.post(
            url,
            json=payload,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=30
        )
        if r.status_code != 200:
             # Try to get error message
             try:
                 err_msg = r.json().get("message", r.text)
             except:
                 err_msg = r.text
             return False, err_msg
             
        return True, ""
    except requests.RequestException as e:
        return False, str(e)


def _fetch_historic_form_instances_values(proc_inst_id: str) -> dict:
    """
    Fetches values from all historic form instances associated with the process instance.
    This is crucial for "Review" tasks where data was entered in a previous form (Start Form).
    """
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return {}
    
    values = {}
    try:
        # 1. Get list of form instances for this process
        url = f"{base}/form-api/form-history/form-instances"
        params = {
            "processInstanceId": proc_inst_id,
            "sort": "submittedDate",
            "order": "asc" # Process oldest first, so newer forms overwrite older ones
        }
        r = requests.get(url, params=params, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
        
        if r.status_code == 200:
            forms = r.json().get("data", [])
            for form in forms:
                form_id = form.get("id")
                if not form_id:
                    continue
                
                # 2. Fetch the actual field values for this form instance
                # The list endpoint might not have all values, so we fetch details
                # Actually, form-history/form-instances/{formId} returns the values in 'values' key?
                # Or we can query form-api/form-history/form-instances/{formId}/query/variables?
                # Let's try getting the full form instance details.
                
                # Check if 'values' is already in the list response (Flowable sometimes includes it)
                # If not, fetch individual
                form_values = form.get("values")
                if not form_values:
                     url_detail = f"{base}/form-api/form-history/form-instances/{form_id}"
                     r_d = requests.get(url_detail, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=10)
                     if r_d.status_code == 200:
                         form_values = r_d.json().get("values")
                
                if form_values:
                    # Merge into our master values map
                    # form_values is typically { "fieldId": "value", ... }
                    values.update(form_values)
                    
    except Exception as e:
        print(f"Error fetching historic form instances for process {proc_inst_id}: {e}")
        
    return values


def _fetch_task_variables_api(task_id: str) -> dict:
    """
    Fetches task variables using the Flowable REST API.
    Returns a dict {name: value}
    """
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return {}
        
    values = {}
    try:
        # Fetch variables (local and global)
        url = f"{base}/process-api/runtime/tasks/{task_id}/variables"
        r = requests.get(url, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
        
        if r.status_code == 200:
            # Returns list of variable objects
            vars_list = r.json()
            for v in vars_list:
                values[v["name"]] = v.get("value")
                
    except Exception as e:
        print(f"Error fetching REST API variables for task {task_id}: {e}")
        
    return values


def _fetch_process_variables_api(proc_inst_id: str) -> dict:
    """
    Fetches process instance variables using the Flowable REST API.
    This gets global variables that might not be attached to the specific task.
    """
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return {}
        
    values = {}
    try:
        url = f"{base}/process-api/runtime/process-instances/{proc_inst_id}/variables"
        r = requests.get(url, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
        
        if r.status_code == 200:
            vars_list = r.json()
            for v in vars_list:
                values[v["name"]] = v.get("value")
    except Exception as e:
        print(f"Error fetching process variables for {proc_inst_id}: {e}")
        
    return values


def _fetch_historic_variables(proc_inst_id: str) -> dict:
    """
    Fetches ALL historic variable instances for the process.
    This ensures we get variables even if they are no longer 'runtime' active.
    """
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return {}
        
    values = {}
    try:
        url = f"{base}/history/historic-variable-instances"
        params = {
            "processInstanceId": proc_inst_id,
            "size": 1000  # Get everything
        }
        r = requests.get(url, params=params, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
        
        if r.status_code == 200:
            data = r.json().get("data", [])
            for v in data:
                # Historic vars have "variable" structure
                name = v.get("variable", {}).get("name") or v.get("name")
                val = v.get("variable", {}).get("value") or v.get("value")
                
                if name:
                    values[name] = val
                    
        # Also try query API if above fails or returns partial
        if not values:
             url_query = f"{base}/query/historic-variable-instances"
             r = requests.post(url_query, json={"processInstanceId": proc_inst_id}, auth=(FLOWABLE_USER, FLOWABLE_PASS), timeout=30)
             if r.status_code == 200:
                 data = r.json().get("data", [])
                 for v in data:
                     name = v.get("variable", {}).get("name") or v.get("name")
                     val = v.get("variable", {}).get("value") or v.get("value")
                     if name:
                         values[name] = val

    except Exception as e:
        print(f"Error fetching historic variables for {proc_inst_id}: {e}")
        
    return values


@login_required
def claim_task_view(request: HttpRequest, task_id: str):
    if request.method != "POST":
         # Only POST allowed
         return HttpResponse("Method not allowed", status=405)
         
    flowable_user_id = request.user.username
    if hasattr(request.user, "profile") and request.user.profile.flowable_id:
        flowable_user_id = request.user.profile.flowable_id
    
    ok, err = _claim_task(task_id, flowable_user_id)
    if ok:
        return redirect("task_detail", task_id=task_id)
    else:
        # For now, render the detail page with error
        # But we need to load detail first to render the template
        # Simpler: redirect to detail with error param?
        # Or render error page.
        # Let's try to add error to session or context if we were using messages framework.
        # Since we are not sure about messages framework, let's just render a simple error or redirect with GET param.
        return redirect(f"/tasks/{task_id}/?error={quote(err)}")


@login_required
def task_detail_view(request: HttpRequest, task_id: str):
    flowable_user_id = request.user.username
    detail = _load_task_detail(flowable_user_id, task_id)
    if not detail:
        return render(
            request,
            "qed_utility/task_detail.html",
            {
                "error": request.GET.get("error", "Task not found or not assigned to you."),
            },
            status=404,
        )

    # Fetch flat form data (for values and basic fallback)
    if detail["status"] == "Completed":
        flat_form_data = _fetch_historic_task_form(task_id)
    else:
        flat_form_data = _fetch_task_form(task_id)

    # Try to fetch layout model
    form_def_id = None
    if flat_form_data:
        form_def_id = flat_form_data.get("formDefinitionId")
    form_layout = _fetch_form_model_layout(task_id, form_def_id=form_def_id)
    
    # Prepare the final form object
    form_data = flat_form_data
    
    # Map for values
    values_map = {}
    
    # 0. Populate from Historic Form Instances (Start Form data) - Highest priority for Review tasks
    # This ensures we get data entered in previous steps even if variables weren't perfectly synced
    if detail.get("proc_inst_id"):
        historic_form_values = _fetch_historic_form_instances_values(detail["proc_inst_id"])
        if historic_form_values:
            print(f"DEBUG: Found {len(historic_form_values)} historic form values")
            # Filter out empty values to avoid overwriting valid variables with empty strings from subsequent forms
            valid_hist_values = {k: v for k, v in historic_form_values.items() if v is not None and str(v).strip() != ""}
            values_map.update(valid_hist_values)
            
            # Log what we found
            print(f"DEBUG: Historic keys: {list(valid_hist_values.keys())}")

    # 1. Populate from variables (SQL-based)
    if detail.get("variables"):
         for v in detail["variables"]:
             values_map[v["name"]] = v["value"]
             
    # 1b. Populate from REST API variables (Runtime) - Can catch things SQL missed
    rest_vars = _fetch_task_variables_api(task_id)
    if rest_vars:
        print(f"DEBUG: Found {len(rest_vars)} REST API task variables")
        values_map.update(rest_vars)

    # 1c. Populate from Process Instance Variables (Global) - Highest coverage
    if detail.get("proc_inst_id"):
        # Runtime variables
        proc_vars = _fetch_process_variables_api(detail["proc_inst_id"])
        if proc_vars:
            print(f"DEBUG: Found {len(proc_vars)} REST API process variables (Runtime)")
            for k, v in proc_vars.items():
                if k not in values_map:
                    values_map[k] = v
        
        # Historic variables (Backup for transient/completed vars)
        hist_vars = _fetch_historic_variables(detail["proc_inst_id"])
        if hist_vars:
            print(f"DEBUG: Found {len(hist_vars)} REST API process variables (History)")
            for k, v in hist_vars.items():
                if k not in values_map:
                    values_map[k] = v

    # 2. Populate from flat form data (runtime values)
    # We re-enable this because for fields like binary Dates that failed to parse from variables,
    # the Form API (flat_form_data) is the ONLY source of the correct value.
    # To prevent "Option 1" defaults from overwriting valid variables, we filter for generic values.
    if flat_form_data and "fields" in flat_form_data:
        print(f"DEBUG: Processing {len(flat_form_data['fields'])} flat fields for values...")
        for f in flat_form_data["fields"]:
            f_id = f.get("id")
            val = f.get("value")
            f_type = f.get("type", "").lower()
            
            if f_id:
                # Debug specific date fields to confirm availability
                if "date" in f_id.lower() or "survey" in f_id.lower():
                     print(f"DEBUG: Flat Field {f_id} (Type: {f_type}) = {val}")

                # Check for generic "Option 1" or empty values
                if val is not None and str(val).strip() != "":
                    is_generic = "option 1" in str(val).lower()
                    if is_generic and f_type in ["dropdown", "select", "radio-buttons"]:
                        continue
                        
                    # Add to values_map if not already present or if it's a date (prioritize Form API for dates)
                    # For date fields, this is often the ONLY source of truth if binary vars failed.
                    if f_type == "date":
                        values_map[f_id] = val
                    elif f_id not in values_map:
                        values_map[f_id] = val

    # If we have a layout, use it
    if form_layout:
        form_data = form_layout
        form_data["use_layout"] = True
        
        # Ensure rows structure exists for rendering
        if "rows" not in form_data and "fields" in form_data:
            form_data["rows"] = [
                {
                    "cols": [
                        {
                            "width": 12,
                            "fields": form_data["fields"]
                        }
                    ]
                }
            ]
            
        if flat_form_data and "outcomes" in flat_form_data:
            form_data["outcomes"] = flat_form_data["outcomes"]
    elif flat_form_data and "fields" in flat_form_data:
        # Normalize flat fields to layout structure for consistent rendering
        form_data = flat_form_data.copy()
        form_data["rows"] = [
            {
                "cols": [
                    {
                        "width": 12,
                        "fields": flat_form_data["fields"]
                    }
                ]
            }
        ]
        form_data["use_layout"] = True
    
    # Also ensure outcomes are present
    if flat_form_data and "outcomes" in flat_form_data:
        form_data["outcomes"] = flat_form_data["outcomes"]

    # CRITICAL: Populate values (Force Fill, Expressions, etc.)
    # This must run AFTER form_data is prepared, so it works for both layout and flat modes.
    if form_data:
        _populate_model_values(form_data, values_map)

    # Fetch users for assignment recommendations
    flowable_users = get_flowable_users()
    
    submit_error = None
    submit_success = False
    if request.method == "POST":
        # Note: We use flat_form_data logic for submission because we need to iterate 
        # all possible fields to gather their values. The layout model is complex to iterate for submission.
        # But wait, if we used layout for display, the user sees layout fields.
        # We can reconstruct the fields list from layout or just use flat_form_data if available.
        # Ideally, flat_form_data contains all fields defined in the form.
        
        # 1. Handle File Uploads first
        upload_errors = []
        for key, file_obj in request.FILES.items():
            if key.startswith("upload_"):
                # Extract field ID from "upload_{field_id}"
                field_id = key[7:] 
                ok, err = _upload_content_item(task_id, field_id, file_obj)
                if not ok:
                    upload_errors.append(f"Error uploading {file_obj.name}: {err}")
        
        if upload_errors:
             submit_error = "; ".join(upload_errors)
        else:
            # 2. Submit Form Properties
            properties = []
            
            # Use flat fields for submission iteration if available, 
            # otherwise we'd need to flatten the layout.
            fields_source = flat_form_data.get("fields", []) if flat_form_data else []
            
            # If flat data is missing but we have layout, we must extract fields from layout
            if not fields_source and form_layout:
                 fields_source = []
                 
                 def collect_fields(f_list):
                     for f in f_list:
                         fields_source.append(f)
                         if "fields" in f:
                             collect_fields(f["fields"])

                 if "rows" in form_layout:
                     for row in form_layout["rows"]:
                         for col in row.get("cols", []):
                             if "fields" in col:
                                 collect_fields(col["fields"])

            for field in fields_source:
                field_id = field.get("id")
                if not field_id:
                    continue
                if field.get("readOnly", False):
                    continue
                
                # Handle boolean checkboxes
                if field.get("type") == "boolean":
                    val = request.POST.get(field_id)
                    value = "true" if val else "false"
                else:
                    value = request.POST.get(field_id, "")
                
                properties.append({"id": field_id, "value": value})
            
            outcome = request.POST.get("outcome")
            ok, err = _submit_task_form(task_id, properties, outcome=outcome)
            if ok:
                submit_success = True
                # Reload data
                detail = _load_task_detail(flowable_user_id, task_id)
                # Re-fetch form data/layout
                if detail["status"] == "Completed":
                    flat_form_data = _fetch_historic_task_form(task_id)
                else:
                    flat_form_data = _fetch_task_form(task_id)
                
                # Re-fetch layout
                form_layout = _fetch_form_model_layout(task_id)
                
                # Re-populate
                form_data = flat_form_data
                values_map = {}
                if detail.get("variables"):
                     for v in detail["variables"]:
                         values_map[v["name"]] = v["value"]
                if flat_form_data and "fields" in flat_form_data:
                    for f in flat_form_data["fields"]:
                        if f.get("id") and f.get("value") is not None:
                            values_map[f["id"]] = f["value"]
                
                if form_layout:
                    _populate_model_values(form_layout, values_map)
                    form_data = form_layout
                    form_data["use_layout"] = True
                    if flat_form_data and "outcomes" in flat_form_data:
                        form_data["outcomes"] = flat_form_data["outcomes"]
                elif flat_form_data and "fields" in flat_form_data:
                    # Normalize flat fields to layout structure for consistent rendering
                    form_data = flat_form_data.copy()
                    form_data["rows"] = [
                        {
                            "cols": [
                                {
                                    "width": 12,
                                    "fields": flat_form_data["fields"]
                                }
                            ]
                        }
                    ]
                    form_data["use_layout"] = True
                        
            else:
                submit_error = err
    
    flowable_base = FLOWABLE_BASE or ""
    flowable_link = None
    if flowable_base:
        base = flowable_base.rstrip("/")
        flowable_link = f"{base}/task-app/#/task/{task_id}"

    # Identify headlines (Logic applies to layout structure now)
    if form_data:
        _identify_headlines(form_data)

    # Fetch users for assignment autocomplete
    flowable_users = get_flowable_users()

    return render(
        request,
        "qed_utility/task_detail.html",
        {
            "task": detail,
            "flowable_link": flowable_link,
            "form": form_data,
            "submit_error": submit_error,
            "submit_success": submit_success,
            "flowable_users": flowable_users,
        },
    )


def _identify_headlines(data: list[dict] | dict):
    """
    Scans fields and identifies text fields that are serving as headlines/labels
    for subsequent boolean fields. Changes their type to 'header'.
    Recursive for layout models.
    """
    if isinstance(data, dict):
        # It's a model/layout
        if "fields" in data:
            _identify_headlines(data["fields"])
        if "rows" in data:
            for row in data["rows"]:
                for col in row.get("cols", []):
                    if "fields" in col:
                        _identify_headlines(col["fields"])
        return

    # It's a list of fields
    fields = data
    if not fields:
        return
    
    for i in range(len(fields)):
        f = fields[i]
        f_type = f.get('type', '').lower()
        f_name = (f.get('name') or "").lower()
        
        # If it's already a header type, skip
        if f_type in ('header', 'expression', 'formatted-text', 'container'):
            continue

        # 1. Special Case: "Reason for Non-FTR" is ALWAYS a header if it's a text field
        if "reason for non-ftr" in f_name and f_type in ('text', 'multi-line-text', 'plain-text'):
            f['type'] = 'header'
            continue
            
        # 2. General Case: Text field followed by a Boolean field
        if i < len(fields) - 1:
            next_f = fields[i+1]
            if next_f.get('type') == 'boolean':
                if f_type in ('text', 'multi-line-text', 'plain-text'):
                    # Exclude "Time Taken" specifically to avoid hiding its value
                    if "time taken" in f_name:
                        continue
                        
                    # 1. If it's ReadOnly, it's likely a header/label
                    if f.get('readOnly'):
                        f['type'] = 'header'
                    # 2. If it has no value, it's likely a label
                    elif not f.get('value'):
                        f['type'] = 'header'



def _upload_content_item(task_id: str, field_id: str, file_obj) -> tuple[bool, str]:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return False, "Flowable URL not configured"
    
    # Use content-service to upload
    url = f"{base}/content-api/content-service/content-items"
    
    try:
        # 'file' is the key expected by Flowable
        files = {'file': (file_obj.name, file_obj, file_obj.content_type)}
        data = {
            'taskId': task_id,
            'field': field_id
        }
        
        r = requests.post(
            url,
            data=data,
            files=files,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            timeout=60
        )
        r.raise_for_status()
        return True, ""
    except Exception as e:
        print(f"Error uploading content: {e}")
        return False, str(e)


@login_required
def download_content_view(request: HttpRequest, content_id: str):
    """
    Proxy to download content from Flowable.
    """
    return _proxy_content_request(content_id, inline=False)


@login_required
def stream_content_view(request: HttpRequest, content_id: str):
    """
    Proxy to stream content from Flowable (inline).
    """
    return _proxy_content_request(content_id, inline=True)


@login_required
def view_content_view(request: HttpRequest, content_id: str):
    """
    Renders a viewer page with the content embedded.
    """
    meta = _fetch_content_metadata(content_id)
    if not meta:
        # If metadata fetch fails, just redirect to stream (fallback)
        return stream_content_view(request, content_id)

    mime_type = meta.get("mimeType", "")
    name = meta.get("name", "Document")
    
    file_type = "unknown"
    if mime_type == "application/pdf":
        file_type = "pdf"
    elif mime_type.startswith("image/"):
        file_type = "image"
    elif mime_type.startswith("text/"):
        file_type = "text"

    is_viewable = file_type in ["pdf", "image", "text"]
    
    stream_url = f"/tasks/content/{content_id}/stream/"
    download_url = f"/tasks/content/{content_id}/"

    return render(
        request,
        "qed_utility/content_viewer.html",
        {
            "name": name,
            "mime_type": mime_type,
            "file_type": file_type,
            "is_viewable": is_viewable,
            "stream_url": stream_url,
            "download_url": download_url,
        },
    )


def _fetch_content_metadata(content_id: str) -> dict | None:
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        return None
        
    url = f"{base}/content-api/content-service/content-items/{content_id}"
    try:
        r = requests.get(
            url, 
            auth=(FLOWABLE_USER, FLOWABLE_PASS), 
            timeout=10
        )
        if r.status_code == 200:
            return r.json()
        
        # Fallback to process-api
        url = f"{base}/process-api/content-service/content-items/{content_id}"
        r = requests.get(
            url, 
            auth=(FLOWABLE_USER, FLOWABLE_PASS), 
            timeout=10
        )
        if r.status_code == 200:
            return r.json()

        # Fallback to app-api
        url = f"{base}/app-api/content-service/content-items/{content_id}"
        r = requests.get(
            url, 
            auth=(FLOWABLE_USER, FLOWABLE_PASS), 
            timeout=10
        )
        if r.status_code == 200:
            return r.json()
            
    except Exception as e:
        print(f"Error fetching metadata for {content_id}: {e}")
    
    return None


def _proxy_content_request(content_id: str, inline: bool = False):
    base = FLOWABLE_BASE.rstrip("/")
    if not base:
        raise Http404("Flowable not configured")
    
    # Try the content service API first
    # URL structure: {base}/content-api/content-service/content-items/{id}/data
    url = f"{base}/content-api/content-service/content-items/{content_id}/data"
    
    try:
        r = requests.get(
            url,
            auth=(FLOWABLE_USER, FLOWABLE_PASS),
            stream=True,
            timeout=60,
        )
        if r.status_code == 404:
            # Fallback: try process-api if content-api is not separate or mapped differently
            # Some setups map content endpoints under process-api too
            url = f"{base}/process-api/content-service/content-items/{content_id}/data"
            r = requests.get(
                url,
                auth=(FLOWABLE_USER, FLOWABLE_PASS),
                stream=True,
                timeout=60,
            )
            
        if r.status_code != 200:
             # Try one more fallback: app-api
             url = f"{base}/app-api/content-service/content-items/{content_id}/data"
             r = requests.get(
                url,
                auth=(FLOWABLE_USER, FLOWABLE_PASS),
                stream=True,
                timeout=60,
             )

        if r.status_code == 200:
            response = StreamingHttpResponse(
                r.iter_content(chunk_size=8192), 
                content_type=r.headers.get("Content-Type")
            )
            disposition_type = "inline" if inline else "attachment"
            # Keep the filename from the original header if possible
            original_disposition = r.headers.get("Content-Disposition", "")
            if "filename=" in original_disposition:
                 filename_part = original_disposition.split("filename=")[1]
                 response["Content-Disposition"] = f'{disposition_type}; filename={filename_part}'
            else:
                 response["Content-Disposition"] = disposition_type
            return response
        else:
             print(f"Failed to fetch content {content_id}: {r.status_code} {r.text}")
             raise Http404("Content not found")
             
    except requests.RequestException as e:
        print(f"Error downloading content {content_id}: {e}")
        raise Http404("Error downloading content")


@login_required
def process_start_view(request: HttpRequest, process_definition_id: str):
    form_data = _fetch_start_form(process_definition_id)
    process_def = _fetch_process_definition_details(process_definition_id)
    submit_error = None
    
    # Get user ID from Flowable logic
    flowable_user_id = request.user.username
    if hasattr(request.user, "profile") and request.user.profile.flowable_id:
        flowable_user_id = request.user.profile.flowable_id
    
    if request.method == "POST":
        properties = []
        if form_data:
            for field in form_data.get("fields", []):
                field_id = field.get("id")
                if not field_id:
                    continue
                if field.get("readOnly", False):
                    continue
                value = request.POST.get(field_id, "")
                properties.append({"id": field_id, "value": value})
            outcome = request.POST.get("outcome")
            ok, err, pid = _submit_start_form(
                process_definition_id, 
                properties, 
                outcome=outcome, 
                user_id=flowable_user_id
            )
        else:
            # No form, just start
            ok, err, pid = _submit_start_form(
                process_definition_id, 
                None, 
                user_id=flowable_user_id
            )

        if ok:
             return redirect("tasks")
        else:
             submit_error = err
    
    return render(
        request,
        "qed_utility/process_start.html",
        {
            "process_definition_id": process_definition_id,
            "process_def": process_def,
            "form": form_data,
            "submit_error": submit_error,
        },
    )

