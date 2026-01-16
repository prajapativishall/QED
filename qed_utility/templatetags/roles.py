from django import template


register = template.Library()


@register.filter
def has_role(user, role_name: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    return user.groups.filter(name=role_name).exists()


@register.filter
def has_any_role(user, role_names: str) -> bool:
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if getattr(user, "is_superuser", False):
        return True
    names = [n.strip() for n in (role_names or "").split(",") if n.strip()]
    if not names:
        return False
    return user.groups.filter(name__in=names).exists()

