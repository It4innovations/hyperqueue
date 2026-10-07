"""HTTP regression for comparison reports and path traversal protection."""

from pathlib import Path
import sys
import tempfile
import unittest
from urllib.parse import quote

from tornado import web
from tornado.testing import AsyncHTTPTestCase

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from src.postprocessing.comparison import ComparisonHandler  # noqa: E402


class ComparisonServerTest(AsyncHTTPTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.reports = self.root / "custom-output" / "comparisons"
        self.reports.mkdir(parents=True)
        (self.reports / "run-a_run-b.html").write_text("<html>comparison</html>")
        (self.root / "secret.html").write_text("private contents")
        super().setUp()

    def get_app(self):
        return web.Application([(r"/comparisons/(.*)", ComparisonHandler, {"directory": self.reports})])

    def assert_not_served(self, name):
        response = self.fetch("/comparisons/" + name)
        self.assertEqual(response.code, 404)
        self.assertNotIn(b"private contents", response.body)

    def test_comparison_from_configured_directory(self):
        response = self.fetch("/comparisons/run-a_run-b.html")
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b"<html>comparison</html>")
        self.assertEqual(response.headers["Content-Type"], "text/html; charset=UTF-8")

    def test_comparison_created_after_server_start(self):
        name = "new comparison_(1).html"
        (self.reports / name).write_text("new report")
        response = self.fetch("/comparisons/" + quote(name))
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b"new report")

    def test_missing_non_html_and_nested_reports(self):
        (self.reports / "secret.txt").write_text("private contents")
        (self.reports / "nested").mkdir()
        (self.reports / "nested" / "secret.html").write_text("private contents")
        (self.reports / "directory.html").mkdir()
        for name in ["missing.html", "secret.txt", "nested/secret.html", "directory.html"]:
            with self.subTest(name=name):
                self.assert_not_served(name)

    def test_traversal_and_absolute_paths(self):
        for name in [
            "../../secret.html",
            "%2e%2e%2f%2e%2e%2fsecret.html",
            quote(str(self.root / "secret.html"), safe=""),
            "..%5c..%5csecret.html",
        ]:
            with self.subTest(name=name):
                self.assert_not_served(name)

    def test_symlink_outside_comparison_directory(self):
        (self.reports / "link.html").symlink_to(self.root / "secret.html")
        self.assert_not_served("link.html")

    def test_symlink_inside_comparison_directory(self):
        (self.reports / "link.html").symlink_to(self.reports / "run-a_run-b.html")
        response = self.fetch("/comparisons/link.html")
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b"<html>comparison</html>")

    def test_long_invalid_name(self):
        self.assert_not_served("-" * 4000)


if __name__ == "__main__":
    unittest.main()
