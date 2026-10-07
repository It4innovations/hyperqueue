from pathlib import Path

from tornado import web


class ComparisonHandler(web.RequestHandler):
    def initialize(self, directory: Path):
        self.directory = directory.resolve()

    def get(self, key: str):
        # Select a local report by name; never construct a path from the URL.
        for report in self.directory.glob("*.html"):
            if report.name != key:
                continue
            resolved = report.resolve()
            if not resolved.is_relative_to(self.directory) or not resolved.is_file():
                raise web.HTTPError(404)
            self.set_header("Content-Type", "text/html; charset=UTF-8")
            self.write(resolved.read_text(encoding="utf-8"))
            return
        raise web.HTTPError(404)
