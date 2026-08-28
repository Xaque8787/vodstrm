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

    def test_login_has_remember_me_checkbox(self):
        source, _, _ = self.env.loader.get_source(self.env, "login.html")

        self.assertIn('type="checkbox"', source)
        self.assertIn('id="remember"', source)
        self.assertIn('name="remember"', source)
        self.assertIn("Remember me", source)

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
        self.assertEqual(logs.count('id="log-cleanup"'), 1)
        self.assertEqual(logs.count('id="log-cleanup-modal"'), 1)
        self.assertIn("This action cannot be undone", logs)
        self.assertIn("/admin/logs/cleanup", logs)

    def test_library_entry_download_updates_only_clicked_button(self):
        source, _, _ = self.env.loader.get_source(
            self.env, "library/index.html"
        )
        helper_start = source.index("async function queueEntryDownload")
        helper_end = source.index("// ── Render helpers", helper_start)
        helper = source[helper_start:helper_end]

        self.assertEqual(source.count("await queueEntryDownload(btn, entryId);"), 2)
        self.assertIn("queueSeriesEpisodeDownload", source)
        self.assertIn(
            "await queueSeriesEpisodeDownload(btn, seriesTitle, seasonNum, episodeNum);",
            source,
        )
        self.assertIn("data-episode=", source)
        self.assertIn("markDownloadQueued(button);", helper)
        self.assertNotIn("refreshAfterAction", helper)
        self.assertNotIn("refreshAfterTvVodAction", helper)
        self.assertIn("e.preventDefault();", source)
        self.assertIn(".btn-download { flex: 0 0 auto; min-width: 72px; }", source)
        self.assertIn(".ep-btn-download.is-downloading {", source)
        self.assertIn(
            'class="btn-action btn-download is-downloading" disabled>Queued',
            source,
        )
        self.assertIn(
            'class="btn-action btn-download" data-action="download-entry">Retry',
            source,
        )

    def test_series_overlay_scrolls_and_expands_one_season(self):
        source, _, _ = self.env.loader.get_source(
            self.env, "library/index.html"
        )

        self.assertIn(".series-panel {", source)
        self.assertIn("height: min(860px, calc(100dvh - 80px));", source)
        self.assertIn("flex: 1 1 auto;", source)
        self.assertIn("min-height: 0;", source)
        self.assertIn("flex: 0 0 auto;", source)
        self.assertIn(".season-list::-webkit-scrollbar", source)
        self.assertIn("panel.className = 'expand-panel series-panel';", source)
        self.assertIn(
            "document.querySelectorAll('#season-list .episode-list.open')",
            source,
        )
        self.assertNotIn("Episodes load when opened", source)
        self.assertNotIn("On demand", source)
        self.assertNotIn("badge-on-demand", source)
        self.assertNotIn("badge-owned", source)


if __name__ == "__main__":
    unittest.main()