from django.contrib import admin
from django.urls import path

from qed_utility.views.auth import QedLoginView, QedLogoutView
from qed_utility.views.bulk_delete import bulk_delete, bulk_delete_execute
from qed_utility.views.bulk_upload import (
    bulk_upload_view,
    validate_excel_view,
    bulk_start_view,
)
from qed_utility.views.dashboard import (
    dashboard_view,
    api_ch_summary,
    api_dt_summary,
    api_flowable_users,
    api_flowable_groups,
    api_user_activity_sites,
    api_activity_types,
    api_user_tasks,
    api_site_ids,
)
from qed_utility.views.export import export_view
from qed_utility.views.process_data import (
    process_data_view,
    process_filter_values,
    process_data_api,
)
from qed_utility.views.tasks import (
    my_tasks_view,
    my_tasks_api,
    task_detail_api,
    task_detail_view,
    claim_task_view,
    process_start_view,
    process_definitions_api,
    download_content_view,
    view_content_view,
    stream_content_view,
)

urlpatterns = [
    path("admin/", admin.site.urls),
    path("login/", QedLoginView.as_view(), name="login"),
    path("logout/", QedLogoutView.as_view(), name="logout"),
    path("", dashboard_view, name="dashboard"),
    path("tasks/", my_tasks_view, name="tasks"),
    path("tasks/<str:task_id>/", task_detail_view, name="task_detail"),
    path("tasks/<str:task_id>/claim/", claim_task_view, name="claim_task"),
    path("tasks/content/<str:content_id>/", download_content_view, name="download_content"),
    path("tasks/content/<str:content_id>/view/", view_content_view, name="view_content"),
    path("tasks/content/<str:content_id>/stream/", stream_content_view, name="stream_content"),
    path("tasks/start/<str:process_definition_id>/", process_start_view, name="process_start"),
    path("api/tasks", my_tasks_api, name="tasks_api"),
    path("api/tasks/<str:task_id>", task_detail_api, name="task_detail_api"),
    path("api/process-definitions/", process_definitions_api, name="process_definitions_api"),
    path("api/ch_summary", api_ch_summary),
    path("api/dt_summary", api_dt_summary),
    path("api/flowable-users", api_flowable_users),
    path("api/flowable-groups", api_flowable_groups),
    path("api/user-activity-sites", api_user_activity_sites),
    path("api/activity-types", api_activity_types),
    path("api/site-ids", api_site_ids),
    path("api/user-tasks", api_user_tasks),
    path("api/process-filters", process_filter_values, name="process_filters"),
    path("api/process-data", process_data_api, name="process_data_api"),
    path("upload/", bulk_upload_view, name="bulk_upload"),
    path("upload/validate/", validate_excel_view, name="validate_excel"),
    path("upload/start/", bulk_start_view, name="bulk_start"),
    path("delete/", bulk_delete, name="bulk_delete"),
    path("delete/execute/", bulk_delete_execute, name="bulk_delete_execute"),
    path("export/", export_view, name="export"),
    path("process-data/", process_data_view, name="process_data"),
]
