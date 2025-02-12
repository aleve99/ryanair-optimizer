import networkx as nx
import multiprocessing as mp

from datetime import date, timedelta
from typing import List, Dict, Set, Tuple, Generator

from .timer import Timer
from ..logger import logging
from ..ryanair import Ryanair
from ..types import OneWayFare, Trip, Stay

logger = logging.getLogger("ryanair")

PARALLEL_FACTOR = 3


def preprocess_fares(fares_node_map: Dict[str, Dict[str, List[OneWayFare]]], max_price: float):
    return {
        origin: {
            dest: [fare for fare in fares if fare.fare <= max_price]
            for dest, fares in fares_by_dest.items()
        }
        for origin, fares_by_dest in fares_node_map.items()
    }

def preprocess_graph(graph: nx.MultiDiGraph, max_price: float):
    edges_to_remove = [(u, v, k) for u, v, k, data in graph.edges(data=True, keys=True) if data['weight'] > max_price]
    graph.remove_edges_from(edges_to_remove)
    logger.info(f"Removed {len(edges_to_remove)} edges with price greater than {max_price}")
    return graph

def find_closed_paths(
        adjacency: Dict[str, List[str]],
        origin: str,
        cutoff: int
    ) -> List[List[str]]:
    """Find all closed paths using dfs from origin to origin with no repeated nodes within cutoff length."""
    closed_paths: List[List[str]] = []

    def dfs_cycles(current: str, path: List[str], visited: Set[str]):
        if len(path) > cutoff:
            return

        # If we can return to origin and path is longer than 2 nodes, we found a cycle
        if len(path) > 2 and origin in adjacency.get(current, []):
            # Create a complete cycle by adding origin at the end
            closed_paths.append(path + [origin])
            return

        # Continue DFS
        for next_node in adjacency.get(current, []):
            if next_node not in visited:
                dfs_cycles(next_node, path + [next_node], visited | {next_node})

    # Start DFS from origin
    dfs_cycles(origin, [origin], {origin})
    return closed_paths

def get_adjacency_list(
        ryanair: Ryanair,
        origin: str,
        allowed_dests: List[str] = None
    ) -> Dict[str, List[str]]:

    if allowed_dests:
        airports = {airport.IATA_code for airport in ryanair.active_airports if airport.IATA_code in allowed_dests}
        airports = {origin} | airports
    else:
        airports = {airport.IATA_code for airport in ryanair.active_airports}
    
    processes = min(
        int(mp.cpu_count() * PARALLEL_FACTOR),
        len(airports)
    )

    logger.info(f"Using {processes} processes to get all destinations")

    with mp.Pool(processes) as pool:
        destinations_by_node = pool.map(
            ryanair.get_destination_codes,
            airports
        )

    # Build adjacency list for faster DFS
    adjacency = {}
    for code, dests in zip(airports, destinations_by_node):
        adjacency[code] = []
        
        if allowed_dests:
            adjacency[code] = [d for d in dests if d in allowed_dests]
        elif dests:
            adjacency[code] = dests
    
    return adjacency

def get_destinations(closed_paths: List[List[str]]) -> Dict[str, Set[str]]:
    """Compute the destinations reachable from each node given the closed paths."""
    
    destinations: Dict[str, Set[str]] = {}
    for path in closed_paths:
        for i in range(len(path) - 1):
            curr_node = path[i]
            next_node = path[i + 1]
            
            if curr_node not in destinations:
                destinations[curr_node] = {next_node}
            else:
                destinations[curr_node].add(next_node)
            
            if next_node not in destinations:
                destinations[next_node] = {curr_node}
            else:
                destinations[next_node].add(curr_node)
    
    return destinations

def get_reachable_fares(
        ryanair: Ryanair,
        destinations: Dict[str, Set[str]],
        from_date: date,
        to_date: date,
    ) -> Dict[str, Dict[str, List[OneWayFare]]]:
    """Return fares for each node and destination."""
    
    processes = min(
        int(mp.cpu_count() * PARALLEL_FACTOR),
        len(destinations.keys())
    )

    logger.info(f"Using {processes} processes to get fares")
    with mp.Pool(processes) as pool:
        fares = pool.starmap(
            ryanair.search_one_way_fares_v2,
            (
                (node, from_date, to_date, list(destinations[node]))
                for node in destinations.keys()
            )
        )

    fares = [set(fare) for fare in fares] #TODO: Better check for duplicates ahead

    return {
        node: {dest: list(filter(lambda fare: fare.destination == dest, fares[i])) for dest in destinations[node]}
        for i, node in enumerate(destinations.keys())
    }

def get_reachable_graph(
        origin: str,
        fares_node_map: Dict[str, Dict[str, List[OneWayFare]]]
    ) -> nx.MultiDiGraph:

    reachable_graph = nx.MultiDiGraph()
    reachable_graph.add_nodes_from(fares_node_map.keys())

    for fares_by_dest in fares_node_map.values():
        for fares in fares_by_dest.values():
            for fare in fares:
                reachable_graph.add_edge(
                    fare.origin,
                    fare.destination,
                    key=Ryanair.get_flight_key(fare),
                    dep_time=fare.dep_time,
                    arr_time=fare.arr_time,
                    weight=fare.fare,
                    left=fare.left,
                    currency=fare.currency
                )
    
    logger.info(f"Reachable graph for {origin} has {len(reachable_graph.nodes)} nodes and {len(reachable_graph.edges)} edges")
    return reachable_graph

def _depth_first_search_from_edge(
        graph: nx.MultiDiGraph,
        origin: str,
        start_edge: tuple,
        min_nights: int,
        max_nights: int,
        cutoff: int
    ) -> List[List[tuple]]:
    """Process paths starting with a specific edge."""
    valid_paths = []
    
    edge_cache = {}
    for u, v, k in graph.edges(keys=True):
        edge_cache[(u, v, k)] = graph.get_edge_data(u, v)[k]
    
    def find_paths(current_path: List[tuple], visited: Set[str]):        
        start_node = current_path[-1][1]
        prev_arrival = edge_cache[current_path[-1]]['arr_time']
        
        for successor in graph.successors(start_node):
            if successor != origin and successor in visited:
                continue
                
            for edge_key in graph[start_node][successor]:
                curr_departure = edge_cache[(start_node, successor, edge_key)]['dep_time']
                
                if curr_departure <= prev_arrival:
                    continue
                
                time_diff: timedelta = curr_departure - prev_arrival
                
                if min_nights == 0:
                    if time_diff.total_seconds() < 7200:  # 2 hours minimum
                        continue
                elif not (min_nights <= time_diff.days <= max_nights):
                    continue
                
                new_path = current_path + [(start_node, successor, edge_key)]

                if successor == origin:
                    if len(new_path) == 2:
                        continue
                    else:
                        valid_paths.append(new_path)    
                        continue
                
                if len(new_path) >= cutoff:
                    continue

                find_paths(new_path, visited | {successor})

    find_paths([start_edge], {start_edge[0], start_edge[1]})

    logger.info(f"Found {len(valid_paths)} valid paths with first flight from {start_edge[0]} to {start_edge[1]}")
    return valid_paths

def get_valid_paths(
        graph: nx.MultiDiGraph,
        origin: str,
        min_nights: int,
        max_nights: int,
        cutoff: int,
    ) -> List[List[str]]:
    """Find all valid closed paths from origin respecting length and time constraints.""" 
    
    # Get all initial edges from origin
    initial_edges: List[Tuple[str, str, str]] = [
        (origin, succ, edge_key)
        for succ in graph.successors(origin)
        for edge_key in graph.get_edge_data(origin, succ)
    ]
    
    logger.info(f"Found {len(initial_edges)} possible first flights from {origin}")
    
    # Use multiprocessing to process different starting edges in parallel
    num_processes = min(mp.cpu_count(), len(initial_edges))
    
    logger.info(f"Using {num_processes} processes for parallel path finding")
    
    # Prepare arguments for multiprocessing
    process_args = [
        (graph, origin, edge, min_nights, max_nights, cutoff)
        for edge in initial_edges
    ]
    
    logger.info(f"Starting depth first search from {len(initial_edges)} edges")
    with mp.Pool(num_processes) as pool:
        all_paths = pool.starmap(_depth_first_search_from_edge, process_args)
    
    # Flatten the list of paths
    valid_paths = [path for sublist in all_paths for path in sublist]
    
    return valid_paths

def path_to_trips(graph: nx.MultiDiGraph, paths: List[List[tuple]]) -> List[Trip]:
    """Convert a path to a list of trips with details on times and costs."""
    trips: List[Trip] = []

    edge_data_cache = {}
    for path in paths:
        flights: List[OneWayFare] = []
        total_cost: float = 0
        total_duration: timedelta = None
        stays: List[dict] = []

        for i, edge in enumerate(path):
            edge_data = edge_data_cache.get(edge, None)
            
            if edge_data is None:
                edge_data = graph.get_edge_data(*edge)
                edge_data_cache[edge] = edge_data

            flight = OneWayFare(
                dep_time=edge_data['dep_time'],
                arr_time=edge_data['arr_time'],
                origin=edge[0],
                destination=edge[1],
                fare=edge_data['weight'],
                left=edge_data['left'],
                currency=edge_data['currency']  
            )

            flights.append(flight)
            total_cost += edge_data['weight']

            # Calculate stay duration at each destination
            if i < len(path) - 1:
                next_edge = path[i+1]
                if next_edge not in edge_data_cache:
                    edge_data_cache[next_edge] = graph.get_edge_data(*next_edge)

                next_edge_data = edge_data_cache[next_edge]
                stay_duration = next_edge_data['dep_time'] - edge_data['arr_time']
                stays.append(Stay(
                    location=edge[1],
                    duration=stay_duration
                ))

        if flights:
            total_duration = (
                flights[-1].arr_time - 
                flights[0].dep_time
            )

        trip = Trip(
            flights=tuple(flights),
            total_cost=total_cost,
            total_duration=total_duration,
            stays=tuple(stays)
        )

        trips.append(trip)
    
    return trips

def find_multi_city_trips(
        graph: nx.MultiDiGraph,
        origin: str,
        min_nights: int,
        max_nights: int,
        cutoff: int,
    ) -> List[Trip]:
    """Find all valid multi-city flights and return them as detailed trips."""
    logger.info("Starting multi-city trip search")
    
    timer = Timer(start=True)
    valid_paths = get_valid_paths(
        graph, 
        origin, 
        min_nights, 
        max_nights, 
        cutoff
    )
    
    timer.stop()
    
    logger.info(f"Path search completed in {timer.seconds_elapsed} seconds")
    logger.info(f"Found {len(valid_paths)} valid paths")
    logger.info("Converting paths to itineraries")

    timer.start()
    
    trips = path_to_trips(graph, valid_paths)
    
    timer.stop()
    logger.info(f"Generated {len(trips)} trips in {timer.seconds_elapsed} seconds")
    
    if trips:
        min_cost = min(trip.total_cost for trip in trips)
        max_cost = max(trip.total_cost for trip in trips)
        logger.info(f"Cost range: {min_cost:.2f} - {max_cost:.2f}")
    
    return trips