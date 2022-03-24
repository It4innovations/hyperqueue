import logging
from abc import ABC

from pathlib import Path
from bokeh.embed import file_html
from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler
from bokeh.server.server import Server
from bokeh.resources import CDN
from tornado import ioloop, web
from tornado.ioloop import IOLoop

from .monitor import create_page
from .overview import create_summary_page, create_comparer_page
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
    io_loop = IOLoop.current()

    # Create custom app that will host all created comparisons and summary
    class SummaryHandler(web.RequestHandler):
        def get(self):
            page = create_summary_page(database, directory)
            self.write(file_html(page, CDN, "Summary"))

    app = web.Application([(r"/", SummaryHandler)])
    app.listen(port)
    logging.info(f"Serving report at http://0.0.0.0:{port}")

    # Create bokeh server that will host dynamic comparer
    def update_comparer(doc):
        page = create_comparer_page(database, directory, app, f"http://0.0.0.0:{port}")
        doc.add_root(page)

    server = Server(
        {"/": Application(FunctionHandler(update_comparer))},
        io_loop=io_loop,
    )

    io_loop.add_callback(server.show, "/")
    io_loop.start()
