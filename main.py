import logging
from argparse import ArgumentParser, ArgumentError
from datetime import date
from pathlib import Path

from funcs.optimizers import optimizer_1w, optimizer_rt, optimizer_multi_trip
from funcs.args_check import check_positive, check_paths
from funcs.server import serve_table

_CONFIG_DEFAULT_PATH_ = Path("config/config.toml")
_PROXYLIST_DEFAULT_PATH_ = Path("config/proxy_list.txt")

_FARES_DIR_ = Path("fares")
_FARES_DIR_.mkdir(exist_ok=True)

logger = logging.getLogger("ryanair")

def main():
    epilog = """
    For round trip fares, specify the minimum and maximum number of nights.
    The fares are saved in the fares/ directory
    """

    parser = ArgumentParser(
        prog="Ryanair fares finder",
        description="Find the cheapest fares from an origin airport.",
        epilog=epilog
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

    ref_max_days = parser.add_argument(
        "--max-days",
        default=None,
        type=check_positive,
        help="The maximum days of vacation, must be >= 0"
    )

    ref_min_days = parser.add_argument(
        "--min-days",
        default=None,
        type=check_positive,
        help=f"The minimum days of vacation, must be >= 0 and less than {ref_max_days.option_strings[0]}"
    )

    parser.add_argument(
        "--serve-html",
        default=False,
        action='store_true',
        help="Serve the fares in a web server"
    )

    parser.add_argument(
        "--multi-trip",
        default=False,
        action='store_true',
        help="Use multi-trip optimizer"
    )

    parser.add_argument(
        "--cutoff",
        default=4,
        type=check_positive,
        help="The maximum number of flights in a multi-trip"
    )

    parser.add_argument(
        "--max-price",
        default=30,
        type=check_positive,
        help="The maximum price for a flight, must be >= 0"
    )

    args = parser.parse_args()

    check_paths(
        ((args.config_path, "toml"), (args.proxy_path, "txt"))
    )

    if args.min_days and not args.max_days:
        raise ArgumentError(
            argument=ref_max_days,
            message=f"must be provided when {ref_min_days.option_strings[0]} is provided"
        )
    elif not args.min_days and args.max_days:
        raise ArgumentError(
            argument=ref_min_days,
            message=f"must be provided when {ref_max_days.option_strings[0]} is provided"
        )
    elif args.min_days and args.max_days and args.min_days > args.max_days:
        raise ArgumentError(
            argument=ref_min_days,
            message=f"must be less than {ref_max_days.option_strings[0]}"
        )
    elif args.min_days and args.max_days:
        one_way = False
    else:
        one_way = True
    
    dests = args.dests.split("|") if args.dests else []

    if one_way and not args.multi_trip:
        df = optimizer_1w(
            origin=args.origin,
            from_date=args.from_date,
            to_date=args.to_date,
            dests=dests,
            config_path=args.config_path,
            proxy_path=args.proxy_path,
            use_usd=args.use_usd,
            no_proxy=args.no_proxy
        )
    elif not one_way and not args.multi_trip:
        df = optimizer_rt(
            origin=args.origin,
            from_date=args.from_date,
            to_date=args.to_date,
            min_days=args.min_days,
            max_days=args.max_days,
            dests=dests,
            config_path=args.config_path,
            proxy_path=args.proxy_path,
            use_usd=args.use_usd,
            no_proxy=args.no_proxy
        )
    else:
        optimizer_multi_trip(
            origin=args.origin,
            from_date=args.from_date,
            to_date=args.to_date,
            min_days=args.min_days,
            max_days=args.max_days,
            dests=dests,
            config_path=args.config_path,
            proxy_path=args.proxy_path,
            use_usd=args.use_usd,
            no_proxy=args.no_proxy,
            cutoff=args.cutoff,
            max_price=args.max_price
        )
        
        return
    
    if df.empty:
        logger.info("No valid trips found")
    else:
        filename = f"fares_{args.origin}_" \
                   f"{'_'.join(dests) if dests else 'ALL'}_" + \
                    ('one_way' if one_way else 'round_trip')

        dir = _FARES_DIR_ / filename
        dir.mkdir(exist_ok=True)

        df.to_csv(
            dir / "fares.csv",
            index=False
        )

        if args.serve_html:
            serve_table(df, dir)

if __name__ == "__main__":
    main()