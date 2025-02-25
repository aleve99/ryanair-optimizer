import pandas as pd
from plotly.graph_objects import Figure
from plotly.graph_objects import Table
from pathlib import Path
from http.server import SimpleHTTPRequestHandler, HTTPServer

from ryanair.logger import logging

logger = logging.getLogger("ryanair")

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=kwargs.pop('directory'), **kwargs)

    def log_message(self, format, *args):
        pass

def make_clickable(val):
    return f'<a href="{val}">link</a>'

def run_server(port: int, dir: Path = Path(".")):
    server_address = ('', port)
    handler = lambda *args, **kwargs: Handler(directory=dir, *args, **kwargs)
    httpd = HTTPServer(server_address, handler)
    httpd.serve_forever()

def serve_table(df: pd.DataFrame, dir: Path):
    fig = Figure(
        data=[Table(
            header=dict(
                values=list(df.columns),
                fill_color='midnightblue',
                font=dict(color='lightgray'),
                align='left'
            ),
            cells=dict(
                values=[df[col] for col in df.columns],
                fill_color=[['lightsteelblue' if i % 2 == 0 else 'aliceblue' for i in range(len(df))] * len(df.columns)],
                align='left'
            )
        )]
    )


    fig.update_layout(
        title="Ryanair fares ✈️",
        title_x=0.5,
        title_font_size=24
    )

    fig.write_html(dir / "fares.html")

    logger.info(f"Fares saved to {dir.absolute()}")

    logger.info(f"Serving fares at http://localhost:8080/fares.html")
    logger.info("Press Ctrl+C to stop")
    try:
        run_server(8080, dir)
    except KeyboardInterrupt:
        logger.info("Server stopped")