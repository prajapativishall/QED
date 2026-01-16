from collections.abc import Callable
from functools import wraps
from typing import Any

from django.contrib.auth.views import redirect_to_login
from django.http import HttpRequest, HttpResponse

from qed_utility.roles import ALL_ROLES


def _user_in_any_role(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return user.groups.filter(name__in=ALL_ROLES).exists()


def _user_has_any_role(user, roles: list[str]) -> bool:
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
            if not request.user.is_authenticated:
                return redirect_to_login(request.get_full_path())
            if roles_list:
                if not _user_has_any_role(request.user, roles_list):
                    return HttpResponse("Forbidden", status=403)
            else:
                if not _user_in_any_role(request.user):
                    return HttpResponse("Forbidden", status=403)
            return view_func(request, *args, **kwargs)

        return wrapper

    return decorator

