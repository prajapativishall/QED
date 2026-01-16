from django.contrib.auth.models import Group, User
from django.test import TestCase


class RoleAccessTests(TestCase):
    def setUp(self):
        for name in ["admin", "circleHead", "viewer"]:
            Group.objects.get_or_create(name=name)

    def _create_user(self, username: str, role: str):
        user = User.objects.create_user(username=username, password="pass12345")
        user.groups.add(Group.objects.get(name=role))
        return user

    def test_viewer_access(self):
        user = self._create_user("viewer1", "viewer")
        self.client.force_login(user)
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/upload/").status_code, 403)
        self.assertEqual(self.client.get("/delete/").status_code, 403)
        self.assertEqual(self.client.get("/export/").status_code, 403)

    def test_circle_head_access(self):
        user = self._create_user("ch1", "circleHead")
        self.client.force_login(user)
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/upload/").status_code, 200)
        self.assertEqual(self.client.get("/delete/").status_code, 403)
        self.assertEqual(self.client.get("/export/").status_code, 403)

    def test_admin_access(self):
        user = self._create_user("admin1", "admin")
        self.client.force_login(user)
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/upload/").status_code, 200)
        self.assertEqual(self.client.get("/delete/").status_code, 200)
        self.assertEqual(self.client.get("/export/").status_code, 200)

    def test_unauthenticated_redirects_to_login(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp["Location"].startswith("/login/"))

