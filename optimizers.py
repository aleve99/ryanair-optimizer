import logging
import pandas as pd
from typing import List

from pathlib import Path
from datetime import date

from ryanair.ryanair import Ryanair
from ryanair.utils.path_processor import PathProcessor
from ryanair.utils.config import parse_toml
from ryanair.utils.config import parse_proxies
from ryanair.utils.server import make_clickable
from ryanair.utils.multitrip import get_reachable_graph, get_reachable_fares, \
                                    preprocess_graph, find_multi_city_trips, \
                                    find_closed_paths, get_adjacency_list, \
                                    preprocess_fares, get_destinations, \
                                    find_multi_city_trips_v2, get_ryanair_graph, \
                                    get_valid_paths, dump_valid_paths, read_valid_paths

from ryanair.utils.loaders import load_adjacency_list, save_adjacency_list, \
                                load_reachable_fares, save_reachable_fares, \
                                load_trips, save_trips, save_airports, \
                                load_ryanair_graph, save_ryanair_graph

from ryanair.utils.timer import Timer
logger = logging.getLogger("ryanair")

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
        config=config,
        USD=use_usd
    )

    if no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)

    airport = ryanair.get_airport(origin)

    fares = ryanair.search_one_way_fares_v2(
        origin=airport.IATA_code,
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
                origin=row['origin'],
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
        config=config,
        USD=use_usd
    )

    if no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)

    airport = ryanair.get_airport(origin)

    fares = ryanair.search_round_trip_fares(
        origin=airport.IATA_code,
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
                origin=row['origin'],
                destination=row['destination']
            ),
            axis=1
        )

        df['link'] = df['link'].apply(make_clickable)
    
    return df

def optimizer_multi_trip(
        origin: str,
        from_date: date,
        to_date: date,
        dests: List[str],
        config_path: Path,
        proxy_path: Path,
        use_usd: bool,
        no_proxy: bool,
        cutoff: int,
        max_price: float,
        min_nights: int = 0,
        max_nights: int = 7,
        data_path: Path = Path("data")
    ) -> None:
    
    if not data_path.exists():
        data_path.mkdir(parents=True)

    if min_nights is None or max_nights is None:
        raise ValueError("min_nights and max_nights must be provided")

    logger.info(f"Using config path: {config_path.absolute()}")
    logger.info(f"Using proxies path: {proxy_path.absolute()}")

    config = parse_toml(config_path)

    ryanair = Ryanair(
        config=config,
        USD=use_usd
    )

    if no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)
    
    logger.info("Starting reachable fares scraping...")

    timer = Timer(start=True)

    if (data_path / "adjacency.json").exists():
        logger.info("Loading adjacency list from JSON")
        adjacency_list = load_adjacency_list(data_path)
    else:
        logger.info("Getting adjacency list...")
        adjacency_list = get_adjacency_list(ryanair, origin, dests)
        logger.info(f"Found max {len(adjacency_list)} reachable nodes from {origin}")
        
        logger.info("Saving adjacency list to JSON")
        save_adjacency_list(adjacency_list, data_path)


    logger.info("Getting closed paths...")

    closed_paths = find_closed_paths(adjacency_list, origin, cutoff)

    logger.info(f"Found {len(closed_paths)} closed paths")

    destinations = get_destinations(closed_paths)
    logger.info(f"Found {len(destinations)} reachable nodes from {origin} in a trip with {cutoff} flights")

    if (data_path / "fares.csv").exists():
        logger.info("Loading fares from CSV")
        fares_node_map = load_reachable_fares(data_path)
    else:
        fares_node_map = get_reachable_fares(
            ryanair, destinations, from_date, to_date
        )
    
        logger.info("Saving fares to CSV")
        save_reachable_fares(fares_node_map, data_path)

    fares_count = sum(sum(len(fares) for fares in fares_by_dest.values()) for fares_by_dest in fares_node_map.values())
    fares_node_map = preprocess_fares(fares_node_map, max_price)
    
    diff = fares_count - sum(sum(len(fares) for fares in fares_by_dest.values()) for fares_by_dest in fares_node_map.values())
    logger.info(f"Removed {diff} fares with price greater than {max_price}")

    if {"trips.csv", "stays.csv", "summary.csv"}.issubset(file.name for file in Path(data_path).iterdir()):
        logger.info("Loading trips from CSV")
        trips = load_trips(data_path)
        logger.info(f"Loaded {len(trips)} trips")
    else:
        USE_V2 = True

        if not USE_V2:
            logger.info("Getting reachable graph")
            reachable_graph = get_reachable_graph(origin, fares_node_map)
            
            logger.info("Preprocessing graph")
            reachable_graph = preprocess_graph(reachable_graph, max_price)

            logger.info("Finding multi-city trips")
            trips = find_multi_city_trips(
                reachable_graph,
                origin,
                min_nights,
                max_nights,
                cutoff
            )
        else:
            trips = find_multi_city_trips_v2(
                closed_paths, fares_node_map, min_nights, max_nights
            )

        logger.info("Sorting trips per total cost")
        trips.sort(key=lambda trip: trip.total_cost)

        logger.info("Saving trips to CSV")
        save_trips(trips, data_path)
    
    
    if not (data_path / "airports.csv").exists():
        logger.info("Saving airports to CSV")
        save_airports(ryanair, trips, data_path)
    
    timer.stop()
    logger.info(f"Process completed in {timer.seconds_elapsed} seconds")
    logger.info("All done! Data is ready to be used.")
    logger.info(f"Data is saved in {data_path.absolute()}")

def optimizer_multi_trip_complete(
        from_date: date,
        to_date: date,
        config_path: Path,
        proxy_path: Path,
        use_usd: bool,
        no_proxy: bool,
        cutoff: int,
        max_price: float,
        min_nights: int = 0,
        max_nights: int = 7,
        data_path: Path = Path("data")
    ) -> None:
    
    if not data_path.exists():
        data_path.mkdir(parents=True)
    
    if min_nights is None or max_nights is None:
        raise ValueError("min_nights and max_nights must be provided")

    # Set up signal handlers at the start
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal. Cleaning up...")
        raise KeyboardInterrupt("User requested shutdown")

    import signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info(f"Using config path: {config_path.absolute()}")
    logger.info(f"Using proxies path: {proxy_path.absolute()}")

    config = parse_toml(config_path)

    try:
        ryanair = Ryanair(
            config=config,
            USD=use_usd
        )

        if no_proxy:
            proxies = ({},)
        else:
            proxies = parse_proxies(proxy_path)
            ryanair.sm.extend_proxies_pool(proxies)

        if (data_path / "adjacency.pickle").exists():
            logger.info("Loading adjacency list")
            adjacency_list = load_adjacency_list(data_path)
        else:
            logger.info("Getting adjacency list")
            timer = Timer(start=True)
            adjacency_list = get_adjacency_list(ryanair)
            timer.stop()
            logger.info(f"Got adjacency list in {timer.seconds_elapsed} seconds")

            logger.info("Saving adjacency list")
            save_adjacency_list(adjacency_list, data_path)  

        logger.info(f"Adjacency list has {len(adjacency_list)} nodes")

        if not (data_path / "ryanair.json").exists():
            logger.info("Getting Ryanair graph")
            timer = Timer(start=True)
            ryanair_graph = get_ryanair_graph(
                ryanair, adjacency_list, from_date, to_date
            )
            timer.stop()
            logger.info(f"Got Ryanair graph in {timer.seconds_elapsed} seconds")

            logger.info("Saving Ryanair graph")
            save_ryanair_graph(ryanair_graph, data_path)
    
            logger.info(f"Ryanair graph has {ryanair_graph.number_of_nodes()} nodes and {ryanair_graph.number_of_edges()} edges")

        timer = Timer(start=True)
        origin = "TRS"

        # Database configuration with timeout
        db_config = config['postgres']

        logger.info("Creating PathProcessor...")
        # Create processor with correct parameters
        processor = PathProcessor(
            db_config=db_config,
            min_nights=min_nights,
            max_nights=max_nights,
            cutoff=cutoff,  # Use cutoff for max_flights
            num_threads=8
        )

        logger.info("Starting path processing...")
        # Process graph
        processor.process_paths(str(data_path / "ryanair.json"), origin)

        timer.stop()
        logger.info(f"Paths update process completed in {timer.seconds_elapsed} seconds")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        raise
    except Exception as e:
        logger.error(f"Error in optimizer: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    optimizer_multi_trip_complete(
        from_date=date(2025, 2, 15),
        to_date=date(2025, 8, 15),
        config_path=Path("config") / "config.toml",
        proxy_path=Path("config") / "proxy_list.txt",
        use_usd=True,
        no_proxy=False,
        cutoff=3,
        max_price=100,
        min_nights=0,
        max_nights=7,
    )