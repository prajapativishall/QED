from django.contrib import admin
from django.urls import path

from qed_utility.views.auth import QedLoginView, QedLogoutView
from qed_utility.views.bulk_delete import bulk_delete, bulk_delete_execute
from qed_utility.views.bulk_upload import (
    bulk_upload_view,
    validate_excel_view,
    bulk_start_view
)
from qed_utility.views.dashboard import (
    dashboard_view,
    api_ch_summary,
    api_dt_summary,
    api_flowable_users,
    api_user_activity_sites,
    api_activity_types,
    api_user_tasks,
    api_site_ids
)
from qed_utility.views.export import export_view
from qed_utility.views.process_data import (
    process_data_view,
    process_filter_values,
    process_data_api,
)

urlpatterns = [
    path("admin/", admin.site.urls),
    path("login/", QedLoginView.as_view(), name="login"),
    path("logout/", QedLogoutView.as_view(), name="logout"),
    path("", dashboard_view, name="dashboard"),
    
    # API endpoints
    path("api/ch_summary", api_ch_summary),
    path("api/dt_summary", api_dt_summary),
    path("api/flowable-users", api_flowable_users),
    path("api/user-activity-sites", api_user_activity_sites),
    path("api/activity-types", api_activity_types),
    path("api/site-ids", api_site_ids),
    path("api/user-tasks", api_user_tasks),
    path("api/process-filters", process_filter_values, name="process_filters"),
    path("api/process-data", process_data_api, name="process_data_api"),
    
    # Feature views
    path("upload/", bulk_upload_view, name="bulk_upload"),
    path("upload/validate/", validate_excel_view, name="validate_excel"),
    path("upload/start/", bulk_start_view, name="bulk_start"),
    
    path("delete/", bulk_delete, name="bulk_delete"),
    path("delete/execute/", bulk_delete_execute, name="bulk_delete_execute"),
    path("export/", export_view, name="export"),
    path("process-data/", process_data_view, name="process_data"),
]
