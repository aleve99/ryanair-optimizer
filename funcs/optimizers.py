import logging
import pandas as pd
from typing import List

from pathlib import Path
from datetime import date

from ryanair.ryanair import Ryanair
from ryanair.utils.config import parse_toml
from ryanair.utils.config import parse_proxies
from funcs.server import make_clickable
from funcs.multitrip import get_reachable_graph, get_reachable_fares, \
                             preprocess_graph, find_multi_city_trips, \
                             find_closed_paths, get_adjacency_list, \
                             preprocess_fares, get_destinations, \
                             find_multi_city_trips_v2

from funcs.loaders import load_adjacency_list, save_adjacency_list, \
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
        timeout=config['network']['timeout'],
        pool_size=config['network']['pool_size'],
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
            by="fare", ascending=True
        ).reset_index(drop=True)
        
        df['link'] = df.apply(
            lambda row: ryanair.get_one_way_link(
                from_date=row['dep_time'].date(),
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
        min_days: int,
        max_days: int,
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
        timeout=config['network']['timeout'],
        pool_size=config['network']['pool_size'],
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
        min_nights=min_days,
        max_nights=max_days,
        from_date=from_date,
        to_date=to_date,
        destinations=dests
    )
    print(fares)
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
        min_days: int = 0,
        max_days: int = 7,
        data_path: Path = Path("data")
    ) -> None:
    
    if not data_path.exists():
        data_path.mkdir(parents=True)

    if min_days is None or max_days is None:
        raise ValueError("min_days and max_days must be provided")

    logger.info(f"Using config path: {config_path.absolute()}")
    logger.info(f"Using proxies path: {proxy_path.absolute()}")

    config = parse_toml(config_path)

    ryanair = Ryanair(
        timeout=config['network']['timeout'],
        pool_size=config['network']['pool_size'],
        USD=use_usd
    )

    if no_proxy:
        proxies = ({},)
    else:
        proxies = parse_proxies(proxy_path)
        ryanair.sm.extend_proxies_pool(proxies)
    
    logger.info("Starting reachable fares scraping...")

    timer = Timer(start=True)

    if (data_path / "adjacency.pkl").exists():
        logger.info("Loading adjacency list from pickle")
        adjacency_list = load_adjacency_list(data_path)
    else:
        logger.info("Getting adjacency list...")
        adjacency_list = get_adjacency_list(ryanair, origin, dests)
        logger.info(f"Found max {len(adjacency_list)} reachable nodes from {origin}")
        
        logger.info("Saving adjacency list to pickle")
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
        USE_V2 = False

        logger.info("Getting reachable graph")
        reachable_graph = get_reachable_graph(ryanair, fares_node_map)

        logger.info("Preprocessing graph")
        reachable_graph = preprocess_graph(reachable_graph, max_price)

        logger.info("Saving reachable graph")
        save_ryanair_graph(reachable_graph, data_path)

        if not USE_V2:
            logger.info("Finding multi-city trips using old algorithm")
            trips = find_multi_city_trips(
                reachable_graph,
                origin,
                min_days,
                max_days,
                cutoff
            )
        else:
            logger.info("Finding multi-city trips using new algorithm")
            trips = find_multi_city_trips_v2(
                closed_paths, fares_node_map, min_days, max_days
            )

        logger.info("Sorting trips per total cost")
        trips.sort(key=lambda trip: trip.total_cost)

        logger.info("Saving trips to CSV")
        save_trips(trips, data_path)
    
    
    if not (data_path / "airports.csv").exists():
        logger.info("Saving airports to CSV")
        save_airports(reachable_graph, data_path)
    
    timer.stop()
    logger.info(f"Process completed in {timer.seconds_elapsed} seconds")

    logger.info("All done! Data is ready to be used.")
    logger.info(f"Data is saved in {data_path.absolute()}")