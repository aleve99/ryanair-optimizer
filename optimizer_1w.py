import logging
import pandas as pd

from pathlib import Path
from datetime import date
from argparse import ArgumentError, ArgumentParser

from ryanair.ryanair import Ryanair  
from ryanair.utils.config import parse_toml, parse_proxies
from ryanair.utils.args_check import check_paths, check_positive

_CONFIG_DEFAULT_PATH_ = "config/config.toml"
_PROXYLIST_DEFAULT_PATH_ = "config/proxy_list.txt"

logger = logging.getLogger("ryanair")

def main():
    parser = ArgumentParser(
        prog="Ryanair fares finder",
        description="Find the cheapest fares from an airport origin"
    )

    parser.add_argument(
        "--origin",
        required=True,
        type=str,
        help="The origin airport (IATA code)"
    )
    parser.add_argument(
        "--dests",
        default=None,
        type=str,
        help="The destination airports (IATA code). If missing search across all destinations. Optional multiple ...|...|..."
    )
    parser.add_argument(
        "--from-date",
        default=date.today(),
        type=date.fromisoformat,
        help="The first date (yyyy-mm-dd) to search for flights, default is today"
    )
    parser.add_argument(
        "--to-date",
        default=None,
        type=date.fromisoformat,
        help="The last date (yyyy-mm-dd) to search for flights, default is ryanair max range"
    )
    parser.add_argument(
        "--config-path",
        default=Path(_CONFIG_DEFAULT_PATH_),
        type=Path,
        help="Toml file for the proxy and other configurations"
    )
    parser.add_argument(
        "--proxy-path",
        default=Path(_PROXYLIST_DEFAULT_PATH_),
        type=Path,
        help=".txt file with a proxy domain for each line"
    )
    parser.add_argument(
        "--no-proxy",
        default=False,
        action='store_true',
        help="Use the proxy specified in the proxies file"
    )

    parser.add_argument(
        "--use-usd",
        default=False,
        action='store_true',
        help='Display prices in USD, otherwise the origin airport currency is used'
    )
    
    args = parser.parse_args()

    check_paths(
        ((args.config_path, "toml"), (args.proxy_path, "txt"))
    )

    logger.info(f"Using config path: {args.config_path.absolute()}")
    logger.info(f"Using proxies path: {args.proxy_path.absolute()}")

    config = parse_toml(args.config_path)

    ryanair = Ryanair(
        rid=config['cookies']['rid'],
        ridsig=config["cookies"]["rid.sig"],
        origin=args.origin,
        USD=args.use_usd
    )

    if args.no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(args.proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)

    ryanair.sm.pool_size = config['network']['pool_size']
    ryanair.sm.timeout = config['network']['timeout']

    fares = ryanair.search_one_way_fares(
            from_date=args.from_date,
            to_date=args.to_date,
            destinations=args.dests.split("|") if args.dests else []
        )

    df = pd.DataFrame(fares)
    if not df.empty:        
        df = df.sort_values(
            by="outbound_fare", ascending=True
        ).reset_index(drop=True)
    
    print(df)

    df.to_csv(
        f"fares_{args.origin}_one_way.csv",
        index=False
    )

if __name__ == "__main__":
    main()