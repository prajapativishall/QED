from django.contrib.auth.models import Group
from django.core.management.base import BaseCommand

from qed_utility.roles import ALL_ROLES


class Command(BaseCommand):
    help = "Create default role groups used by QED Utility."

    def handle(self, *args, **options):
        created = 0
        for role in ALL_ROLES:
            _, was_created = Group.objects.get_or_create(name=role)
            if was_created:
                created += 1
        self.stdout.write(self.style.SUCCESS(f"Roles ensured. Created: {created}"))

