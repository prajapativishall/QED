import mysql.connector
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from qed_utility.access import role_required
from qed_utility.views.dashboard import DB_CONFIG
from qed_utility.views.export import fetch_export_data


@role_required("designcoordinator")
def process_data_view(request):
    return render(request, "qed_utility/process_data.html")


@role_required("designcoordinator")
@require_GET
def process_filter_values(request):
    values = {"qacajobid": [], "siteid": [], "circle": [], "activitytype": []}
    with mysql.connector.connect(**DB_CONFIG) as conn:
        cursor = conn.cursor()
        for name in values.keys():
            cursor.execute(
                "SELECT DISTINCT TEXT_ FROM ACT_HI_VARINST WHERE NAME_ = %s AND TEXT_ IS NOT NULL ORDER BY TEXT_",
                (name,),
            )
            values[name] = [row[0] for row in cursor.fetchall()]
    return JsonResponse(values)


def _instance_ids_for_filters(filters):
    names = {
        "qacajobid": "qacajobid",
        "siteid": "siteid",
        "circle": "circle",
        "activitytype": "activitytype",
    }
    sets = []
    with mysql.connector.connect(**DB_CONFIG) as conn:
        cursor = conn.cursor()
        for key, var_name in names.items():
            value = filters.get(key)
            if value:
                cursor.execute(
                    "SELECT DISTINCT PROC_INST_ID_ FROM ACT_HI_VARINST WHERE NAME_ = %s AND TEXT_ = %s",
                    (var_name, value),
                )
                sets.append({row[0] for row in cursor.fetchall()})
    if not sets:
        return []
    ids = sets[0]
    for s in sets[1:]:
        ids &= s
    return list(ids)


@role_required("designcoordinator")
@require_GET
def process_data_api(request):
    filters = {
        "qacajobid": request.GET.get("qacajobid") or "",
        "siteid": request.GET.get("siteid") or "",
        "circle": request.GET.get("circle") or "",
        "activitytype": request.GET.get("activitytype") or "",
    }
    instance_ids = _instance_ids_for_filters(filters)
    if not instance_ids:
        return JsonResponse({"rows": []})
    limit = int(request.GET.get("limit", "100"))
    offset = int(request.GET.get("offset", "0"))
    df = fetch_export_data(instance_ids)
    if df.empty:
        return JsonResponse({"rows": []})
    sliced = df.iloc[offset : offset + limit]
    return JsonResponse({"rows": sliced.to_dict(orient="records")})
