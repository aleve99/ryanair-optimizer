import logging
import pandas as pd
from typing import List

from pathlib import Path
from datetime import date

from ryanair.ryanair import Ryanair  
from ryanair.utils.config import parse_toml, parse_proxies
from http.server import SimpleHTTPRequestHandler, HTTPServer


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

def optimizer_1w(
        origin: str,
        from_date: date,
        to_date: date,
        dests: List[str],
        config_path: Path,
        proxy_path: Path,
        use_usd: bool,
        no_proxy: bool
    ) -> pd.DataFrame:

    logger.info(f"Using config path: {config_path.absolute()}")
    logger.info(f"Using proxies path: {proxy_path.absolute()}")

    config = parse_toml(config_path)

    ryanair = Ryanair(
        rid=config['cookies']['rid'],
        ridsig=config["cookies"]["rid.sig"],
        origin=origin,
        USD=use_usd
    )

    if no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)

    ryanair.sm.pool_size = config['network']['pool_size']
    ryanair.sm.timeout = config['network']['timeout']

    fares = ryanair.search_one_way_fares(
            from_date=from_date,
            to_date=to_date,
            destinations=dests
        )

    df = pd.DataFrame(fares)
    
    if not df.empty:        
        df = df.sort_values(
            by="outbound_fare", ascending=True
        ).reset_index(drop=True)
        
        df['link'] = df.apply(
            lambda row: ryanair.get_one_way_link(
                from_date=row['outbound_dep_time'].date(),
                destination=row['destination']
            ),
            axis=1
        )

        df['link'] = df['link'].apply(make_clickable)
    
    return df

def optimizer_rt(
        origin: str,
        from_date: date,
        to_date: date,
        min_nights: int,
        max_nights: int,
        dests: List[str],
        config_path: Path,
        proxy_path: Path,
        use_usd: bool,
        no_proxy: bool
    ) -> pd.DataFrame:

    logger.info(f"Using config path: {config_path.absolute()}")
    logger.info(f"Using proxies path: {proxy_path.absolute()}")

    config = parse_toml(config_path)

    ryanair = Ryanair(
        rid=config['cookies']['rid'],
        ridsig=config["cookies"]["rid.sig"],
        origin=origin,
        USD=use_usd
    )

    if no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)

    ryanair.sm.pool_size = config['network']['pool_size']
    ryanair.sm.timeout = config['network']['timeout']

    fares = ryanair.search_round_trip_fares(
        min_nights=min_nights,
        max_nights=max_nights,
        from_date=from_date,
        to_date=to_date,
        destinations=dests
    )

    df = pd.DataFrame(fares)

    if not df.empty:
        df['round_trip_fare'] = (df["outbound_fare"] + df["return_fare"]).round(
            decimals=2
        )
        
        df = df.sort_values(
            by="round_trip_fare", ascending=True
        ).reset_index(drop=True)

        columns = df.columns.to_list()
        df = df[columns[:-2] + [df.columns[-1], df.columns[-2]]]
        
        df['link'] = df.apply(
            lambda row: ryanair.get_round_trip_link(
                from_date=row['outbound_dep_time'].date(),
                to_date=row['return_dep_time'].date(),
                destination=row['destination']
            ),
            axis=1
        )

        df['link'] = df['link'].apply(make_clickable)
    
    return df
