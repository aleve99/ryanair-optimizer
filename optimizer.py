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
        "--max-nights",
        required=True,
        type=check_positive,
        help="The maximum nights of vacation, must be >= 0"
    )
    parser.add_argument(
        "--dests",
        default=None,
        type=str,
        help="The destination airports (IATA code). If missing search across all destinations. Optional multiple ...|...|..."
    )
    parser.add_argument(
        "--currency",
        default=None,
        type=str,
        help=f"The currency symbol to display fares, default is using local currency. Supported currencies "
    )
    ref_arg_min_nights = parser.add_argument(
        "--min-nights",
        default=1,
        type=check_positive,
        help="The minimum nights of vacation, must be >= 0"
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

    config = parse_toml(args.config_path)

    ryanair = Ryanair(
        rid=config['cookies']['rid'],
        ridsig=config["cookies"]["rid.sig"],
        origin=args.origin,
        USD=args.use_usd
    )

    if args.min_nights > args.max_nights:
        raise ArgumentError(ref_arg_min_nights, "must be less than --max-nights")

    if args.no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(args.proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)

    ryanair.sm.pool_size = config['network']['pool_size']

    fares = ryanair.search_fares(
        min_nights=args.min_nights,
        max_nights=args.max_nights,
        from_date=args.from_date,
        to_date=args.to_date,
        destinations=args.dests.split("|")
    )

    df = pd.DataFrame(fares)
    if not df.empty:
        df['round_trip_fare'] = (df["outbound_fare"] + df["return_fare"]).round(decimals=2)
        df = df.sort_values(by="round_trip_fare", ascending=True).reset_index(drop=True)
        columns = df.columns.to_list()
        df = df[columns[:-3] + [df.columns[-1], df.columns[-2]]]
    
    print(df)

    df.to_csv("fares.csv", index=False)

if __name__ == "__main__":
    main()