import os
import unittest

from jinja2 import Environment, FileSystemLoader


class TemplateLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        template_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "app", "templates"
        )
        cls.env = Environment(loader=FileSystemLoader(template_dir))
        cls.env.globals["app_version"] = "1.2.3"
        cls.user = type("User", (), {"username": "admin", "is_admin": True})()

    def test_context_bar_requires_page_heading(self):
        dashboard = self.env.from_string(
            '{% extends "base.html" %}{% block content %}Dashboard{% endblock %}'
        ).render(current_user=self.user)
        page = self.env.from_string(
            """
            {% extends "base.html" %}
            {% block page_heading %}Providers{% endblock %}
            {% block page_description %}Manage sources.{% endblock %}
            {% block page_actions %}<button id="action">Add</button>{% endblock %}
            {% block content %}Content{% endblock %}
            """
        ).render(current_user=self.user)

        self.assertNotIn('class="context-bar"', dashboard)
        self.assertNotIn("<h1", dashboard)
        self.assertEqual(dashboard.count('class="nav-user"'), 1)
        self.assertEqual(dashboard.count('class="nav-version"'), 1)
        self.assertIn('class="nav-version">v1.2.3</span>', dashboard)
        self.assertNotIn("Welcome back", dashboard)
        self.assertEqual(page.count('class="context-bar"'), 1)
        self.assertIn('id="context-bar-title">Providers</h1>', page)
        self.assertEqual(page.count('id="action"'), 1)

    def test_migrated_actions_are_unique(self):
        checks = {
            "providers/index.html": "add-provider-btn",
            "filters/index.html": "open-add-btn",
            "library/downloads.html": "btn-clear-failed",
            "admin/logs.html": "log-refresh",
        }
        for template_name, action_id in checks.items():
            source, _, _ = self.env.loader.get_source(self.env, template_name)
            self.assertEqual(source.count(f'id="{action_id}"'), 1)
            self.assertNotIn("<h1", source)

        database, _, _ = self.env.loader.get_source(
            self.env, "admin/library.html"
        )
        self.assertEqual(database.count("/admin/database/clear/streams"), 1)
        self.assertEqual(database.count("/admin/database/clear/entries"), 1)
        self.assertNotIn("<h1", database)

        logs, _, _ = self.env.loader.get_source(self.env, "admin/logs.html")
        for level in ("debug", "info", "warning", "error", "critical"):
            self.assertIn(f".log-row--{level}", logs)


if __name__ == "__main__":
    unittest.main()