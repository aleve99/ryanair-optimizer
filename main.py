import logging
from argparse import ArgumentParser, ArgumentError
from datetime import date
from pathlib import Path
from plotly.graph_objects import Figure, Table

from optimizers import optimizer_1w, optimizer_rt, run_server
from ryanair.utils.args_check import check_positive, check_paths

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

    ref_max_nights = parser.add_argument(
        "--max-nights",
        default=None,
        type=check_positive,
        help="The maximum nights of vacation, must be >= 0"
    )

    ref_min_nights = parser.add_argument(
        "--min-nights",
        default=None,
        type=check_positive,
        help=f"The minimum nights of vacation, must be >= 0 and less than {ref_max_nights.option_strings[0]}"
    )

    parser.add_argument(
        "--serve-html",
        default=False,
        action='store_true',
        help="Serve the fares in a web server"
    )
    args = parser.parse_args()

    check_paths(
        ((args.config_path, "toml"), (args.proxy_path, "txt"))
    )

    if args.min_nights and not args.max_nights:
        raise ArgumentError(
            argument=ref_max_nights,
            message=f"must be provided when {ref_min_nights.option_strings[0]} is provided"
        )
    elif not args.min_nights and args.max_nights:
        raise ArgumentError(
            argument=ref_min_nights,
            message=f"must be provided when {ref_max_nights.option_strings[0]} is provided"
        )
    elif args.min_nights and args.max_nights and args.min_nights > args.max_nights:
        raise ArgumentError(
            argument=ref_min_nights,
            message=f"must be less than {ref_max_nights.option_strings[0]}"
        )
    elif args.min_nights and args.max_nights:
        one_way = False
    else:
        one_way = True
    
    dests = args.dests.split("|") if args.dests else []

    if one_way:
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
    else:
        df = optimizer_rt(
            origin=args.origin,
            from_date=args.from_date,
            to_date=args.to_date,
            min_nights=args.min_nights,
            max_nights=args.max_nights,
            dests=dests,
            config_path=args.config_path,
            proxy_path=args.proxy_path,
            use_usd=args.use_usd,
            no_proxy=args.no_proxy
        )
    
    
    dir = _FARES_DIR_ / f"fares_{args.origin}_{'_'.join(dests) if dests else 'ALL'}_{'one_way' if one_way else 'round_trip'}"
    dir.mkdir(exist_ok=True)

    df.to_csv(
        dir / "fares.csv",
        index=False
    )

    fig = Figure(
        data=[Table(
            header=dict(
                values=list(df.columns),
                fill_color='midnightblue',
                font=dict(color='lightgray'),
                align='left'),
            cells=dict(
                values=[df[col] for col in df.columns],
                fill_color=[['lightsteelblue' if i % 2 == 0 else 'aliceblue' for i in range(len(df))] * len(df.columns)],
                align='left'
            )
        )]
    )
    fig.write_html(dir / "fares.html")

    logger.info(f"Fares saved to {dir.absolute()}")

    if args.serve_html:
        logger.info(f"Serving fares at http://localhost:8080/fares.html")
        logger.info("Press Ctrl+C to stop")
        try:
            run_server(8080, dir)
        except KeyboardInterrupt:
            logger.info("Server stopped")

if __name__ == "__main__":
    main()