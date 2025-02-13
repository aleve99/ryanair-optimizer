import logging
import pandas as pd
from typing import List

from pathlib import Path
from datetime import date

from ryanair.ryanair import Ryanair
from ryanair.utils.config import parse_proxies
from ryanair.utils.server import make_clickable
from ryanair.utils.multitrip import get_reachable_graph, get_reachable_fares, \
                                    preprocess_graph, find_multi_city_trips, \
                                    find_closed_paths, get_adjacency_list, \
                                    preprocess_fares, get_destinations, \
                                    find_multi_city_trips_v2

from ryanair.utils.loaders import load_adjacency_list, save_adjacency_list, \
                                load_reachable_fares, save_reachable_fares, \
                                load_trips, save_trips, save_airports, \
                                load_closed_paths, save_closed_paths

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

    ryanair = Ryanair(
        config_path=config_path,
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

    ryanair = Ryanair(
        config_path=config_path,
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

    ryanair = Ryanair(
        config_path=config_path,
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

    if (data_path / "closed_paths.json").exists():
        logger.info("Loading closed paths from JSON")
        closed_paths = load_closed_paths(data_path)
    else:
        logger.info("Getting closed paths...")
        closed_paths = find_closed_paths(
            adjacency_list, origin, cutoff
        )   
        logger.info(f"Found {len(closed_paths)} closed paths")

        logger.info("Saving closed paths to CSV")
        save_closed_paths(closed_paths, data_path)

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
