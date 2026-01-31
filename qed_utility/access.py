from functools import wraps
from typing import Any, List, Callable

from django.contrib.auth.views import redirect_to_login
from django.http import HttpRequest, HttpResponse, JsonResponse

from qed_utility.roles import ALL_ROLES


def _user_in_any_role(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return user.groups.filter(name__in=ALL_ROLES).exists()


def _user_has_any_role(user, roles: List[str]) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return user.groups.filter(name__in=roles).exists()


def role_required(*roles: str):
    roles_list = list(roles)

    def decorator(view_func: Callable[..., HttpResponse]):
        @wraps(view_func)
        def wrapper(request: HttpRequest, *args: Any, **kwargs: Any):
            is_ajax = request.headers.get('x-requested-with') == 'XMLHttpRequest' or 'application/json' in request.headers.get('accept', '')

            if not request.user.is_authenticated:
                if is_ajax:
                    return JsonResponse({"error": "Authentication required"}, status=401)
                return redirect_to_login(request.get_full_path())
            
            if roles_list:
                if not _user_has_any_role(request.user, roles_list):
                    if is_ajax:
                        return JsonResponse({"error": "Forbidden: You do not have permission to perform this action."}, status=403)
                    return HttpResponse("Forbidden", status=403)
            else:
                if not _user_in_any_role(request.user):
                    if is_ajax:
                        return JsonResponse({"error": "Forbidden: You do not have permission to perform this action."}, status=403)
                    return HttpResponse("Forbidden", status=403)
            return view_func(request, *args, **kwargs)

        return wrapper

    return decorator

