import logging

from pathlib import Path
from bokeh.embed import file_html
from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler
from bokeh.server.server import Server
from bokeh.resources import CDN
from tornado import ioloop, web
from tornado.ioloop import IOLoop

from .monitor import create_page
from .overview import pregenerate_entries, create_summary_page, create_comparer_page
from ..benchmark.database import Database
from .report import ClusterReport


def serve_cluster_report(report: ClusterReport, port: int):
    class Handler(web.RequestHandler):
        def get(self):
            page = create_page(report)
            self.write(file_html(page, CDN, "Cluster report"))

    app = web.Application(
        [
            (r"/", Handler),
        ]
    )
    app.listen(port)

    logging.info(f"Serving report at http://0.0.0.0:{port}")
    ioloop.IOLoop.current().start()


def serve_summary_html(database: Database, directory: Path, port: int):
    entries = pregenerate_entries(database, directory)
    io_loop = IOLoop.current()

    class SummaryHandler(web.RequestHandler):
        def get(self):
            page = create_summary_page(database, directory)
            self.write(page)

    class ClusterHandler(web.RequestHandler):
        def get(self, key: str):
            report = entries[str(Path(key).stem)].report
            page = create_page(report)
            self.write(file_html(page, CDN, "Cluster report"))

    class ComparisonHandler(web.RequestHandler):
        def get(self, key: str):
            html_file = open(
                Path("summary/comparisons").joinpath(key), "r", encoding="utf-8"
            )
            source_code = html_file.read()
            self.write(source_code)

    app = web.Application(
        [
            (r"/comparisons/(.*)", ComparisonHandler),
            (r"/monitoring/(.*)", ClusterHandler),
            (r"/", SummaryHandler),
        ]
    )

    app.listen(port)
    logging.info(f"Serving report at http://localhost:{port}")
    logging.info(f"Serving comparator at http://localhost:5006")

    def comparer(doc):
        page = create_comparer_page(database, directory, f"http://localhost:{port}")
        doc.add_root(page)

    Server(
        {
            "/": Application(FunctionHandler(comparer)),
        },
        io_loop=io_loop,
    )

    io_loop.start()
