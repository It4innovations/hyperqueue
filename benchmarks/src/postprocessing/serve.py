import logging

from pathlib import Path
from bokeh.embed import file_html
from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler
from bokeh.server.server import Server
from bokeh.resources import CDN
from tornado import ioloop, web
from tornado.ioloop import IOLoop
import seaborn as sns
from .monitor import create_page
from .overview import pregenerate_entries, create_summary_page, create_comparer_page, render
from ..benchmark.database import Database
from .report import ClusterReport
from .common import create_database_df, groupby_workload
import os
import matplotlib.pyplot as plt
from PIL import Image
import numpy as np
import io
import json


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

    class CompareOverview(web.RequestHandler):
        def get(self, key: str):
            df = create_database_df(database)
            key = key.split("(")[1].split(")")[0]
            df = df[df['workload-params'] == key]
            max_index = df['index'].max()
            samples = [df[df['index'] == i] for i in range(max_index + 1)]
            tables = [i['duration'].describe().to_frame() for i in samples]
            compare1 = tables[0] > tables[1]
            compare0 = tables[1] > tables[0]
            color_0 = lambda x: ["background-color:red" if i[1][0] else "" for i in compare0.iterrows()]
            color_1 = lambda x: ["background-color:red" if i[1][0] else "" for i in compare1.iterrows()]
            # tables[1].style.apply_index(color_b)
            output = ""
            tables[0].style.set_uuid("table0")
            tables[1].style.set_uuid("table1")
            output += tables[0].style.apply(color_0).set_uuid("table0").to_html()
            output += tables[1].style.apply(color_1).set_uuid("table1").to_html()

            plt.scatter(df['index'], df["duration"], s=10)
            plt.xticks(list(range(max_index + 1)))
            plt.ylabel("Duration [ms]")
            plt.savefig("img.png")
            plt.clf()
            with open(os.path.join(os.path.dirname(__file__), "templates/compare_table.html")) as fp:
                file = fp.read()

                self.write(render(file, tables=output))

    class ImgHandler(web.RequestHandler):
        def get(self):
            img = Image.open('img.png')
            s = io.BytesIO()
            img.save(s, format="PNG")
            o = s.getvalue()
            self.write(o)
            self.set_header("Content-type", "image/png")

    class OverViewHandler(web.RequestHandler):
        KEY_GROUPS = ["Grouped by workload:", "Grouped by environment:", "Grouped by benchmark:"]

        def summary_reader(self):
            output = {}
            i = 0

            with open("benchmark/zw/summary.txt") as fp:
                key = ""
                entries = []
                while True:
                    line = fp.readline()

                    line_parsed = line.split("\n")

                    if line_parsed[0] in self.KEY_GROUPS:
                        if len(entries) > 0 and key != "":
                            output[key] = entries
                        key = line_parsed[0]
                        entries = []
                        continue

                    entries.append(line_parsed[0]) if line_parsed[0] != "" else None
                    i += 1
                    if line == '':
                        if len(entries) > 0 and key != "":
                            output[key] = entries
                        break

            return output

        def get(self):
            data = self.summary_reader()
            df = create_database_df(database)
            grouped = df.groupby(["workload", "workload-params", "env", "env-params","index"])[
                "duration"
            ]
            pairs = []
            for pair in list(grouped):
                pairs.append([pair[0], pair[1].describe().to_frame().T.to_html()])
            data["Grouped by benchmark:"] = pairs
            for i in range(len(data["Grouped by benchmark:"])):
                formated_str = "-".join([str(z) for z in data["Grouped by benchmark:"][i][0]])
                formated_str = formated_str.replace(",","_")
                data["Grouped by benchmark:"][i][0] = formated_str
            with open(os.path.join(os.path.dirname(__file__), "templates/summary.html")) as fp:
                file = fp.read()
                self.write(render(file, data=data, keys=list(data.keys())[:2]))
            # self.write(data)



    app = web.Application(
        [
            (r"/comparisons/(.*)", ComparisonHandler),
            (r"/monitoring/(.*)", ClusterHandler),
            (r"/compare/(.*)", CompareOverview),
            (r"/img", ImgHandler),
            (r"/", SummaryHandler),
            (r"/overview", OverViewHandler)
        ]
    )

    app.listen(port)
    logging.info(f"Serving report at http://localhost:{port}")
    logging.info("Serving comparator at http://localhost:5006")

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
