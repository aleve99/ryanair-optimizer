import csv
import json
import pickle
import pandas as pd
import networkx as nx
from pathlib import Path
from typing import List, Dict
from datetime import datetime

from ..ryanair import Ryanair
from ..types import Trip, OneWayFare, Stay

def save_airports(
        ryanair: Ryanair,
        trips: List[Trip],
        path: Path,
        filename: str = "airports.csv"
    ):

    codes = set()
    for trip in trips:
        for flight in trip.flights:
            codes.add(flight.origin)

    with open(path / filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['code', 'location', 'lng', 'lat'])
        writer.writeheader()
        for code in codes:
            airport = ryanair.get_airport(code)
            writer.writerow({
                'code': airport.IATA_code,
                'location': airport.location,
                'lng': airport.lng,
                'lat': airport.lat
            })

def load_trips(
        path: Path,
        filename_trips: str = "trips.csv",
        filename_summary: str = "summary.csv",
        filename_stays: str = "stays.csv",
        filename_fares: str = "fares.csv"
    ) -> List[Trip]:

    # Read all CSVs at once with optimized dtypes
    df_summary = pd.read_csv(path / filename_summary, dtype={
        'total_cost': float,
        'total_duration': str,
        'num_flights': int
    })
    df_trips = pd.read_csv(path / filename_trips, dtype={'trip_id': int, 'position': int, 'key': str})
    df_stays = pd.read_csv(path / filename_stays, dtype={'trip_id': int, 'position': int, 'location': str})
    df_fares = pd.read_csv(path / filename_fares, dtype={
        'fare': float,
        'left': int,
        'currency': str,
        'origin': str,
        'destination': str,
        'key': str
    })

    # Pre-process fares DataFrame to avoid repeated datetime conversions
    df_fares['dep_time'] = pd.to_datetime(df_fares['dep_time'])
    df_fares['arr_time'] = pd.to_datetime(df_fares['arr_time'])
    
    # Create fare_info dictionary using vectorized operations
    fare_info = {
        row['key']: OneWayFare(
            dep_time=row['dep_time'],
            arr_time=row['arr_time'],
            origin=row['origin'],
            destination=row['destination'],
            fare=row['fare'],
            left=row['left'],
            currency=row['currency']
        )
        for _, row in df_fares.iterrows()
    }

    trips = []
    # Group dataframes by trip_id for faster access
    trips_grouped = df_trips.groupby('trip_id')
    stays_grouped = df_stays.groupby('trip_id')

    for i, summary_row in df_summary.iterrows():
        # Get all flights and stays for this trip at once
        trip_flights = trips_grouped.get_group(i) if i in trips_grouped.groups else pd.DataFrame()
        trip_stays = stays_grouped.get_group(i) if i in stays_grouped.groups else pd.DataFrame()
        
        # Convert stays using vectorized operations
        stays = [
            Stay(location=row['location'], duration=pd.to_timedelta(row['duration']))
            for _, row in trip_stays.iterrows()
        ]
        
        # Convert flights using list comprehension
        flights = [fare_info[row['key']] for _, row in trip_flights.iterrows()]

        trips.append(Trip(
            flights=tuple(flights),
            total_cost=summary_row['total_cost'],
            total_duration=summary_row['total_duration'],
            stays=tuple(stays)
        ))

    return trips

def save_trips(
        trips: List[Trip],
        path: Path,
        filename_trips: str = "trips.csv",
        filename_summary: str = "summary.csv",
        filename_stays: str = "stays.csv"
    ):

    with open(path / filename_summary, mode='w', newline='') as file_summary:
        writer = csv.DictWriter(file_summary, fieldnames=[
            "trip_id", "total_cost", "total_duration", "num_flights",
            "departure_time", "return_time", "route"
        ])
        writer.writeheader()
        for i, trip in enumerate(trips):
            writer.writerow({
                "trip_id": i,
                "total_cost": round(trip.total_cost, 2),
                "total_duration": trip.total_duration,
                "num_flights": len(trip.flights),
                "departure_time": trip.flights[0].dep_time,
                "return_time": trip.flights[-1].arr_time,
                "route": '-'.join(flight.origin for flight in trip.flights) + f'-{trip.flights[-1].destination}',
            })

    with open(path / filename_stays, mode='w', newline='') as file_stays:
        writer = csv.DictWriter(file_stays, fieldnames=[
            "trip_id", "position", "location", "duration"
        ])
        writer.writeheader()
        for i, trip in enumerate(trips):
            for j, stay in enumerate(trip.stays):
                writer.writerow({
                    "trip_id": i,
                    "position": j,
                    "location": stay.location,
                    "duration": stay.duration
                })

    with open(path / filename_trips, mode='w', newline='') as file_trips:
        writer = csv.DictWriter(file_trips, fieldnames=[
            "trip_id", "position", "key"
        ])
        writer.writeheader()
        for i, trip in enumerate(trips):
            for j, flight in enumerate(trip.flights):
                writer.writerow({
                    "trip_id": i,
                    "position": j,
                    "key": Ryanair.get_flight_key(flight),
                })

def load_reachable_fares(
        path: Path,
        filename: str = "fares.csv"
    ) -> Dict[str, Dict[str, List[OneWayFare]]]:
    
    df = pd.read_csv(path / filename)
    fares_node_map = {}

    for _, row in df.iterrows():
        element = OneWayFare(
            dep_time=datetime.fromisoformat(row['dep_time']),
            arr_time=datetime.fromisoformat(row['arr_time']),
            origin=row['origin'],
            destination=row['destination'],
            fare=float(row['fare']),
            left=int(row['left']),
            currency=row['currency']
        )
        
        if element.origin in fares_node_map:
            if element.destination in fares_node_map[element.origin]:
                fares_node_map[element.origin][element.destination].append(element)
            else:
                fares_node_map[element.origin][element.destination] = [element]
        else:
            fares_node_map[element.origin] = {element.destination: [element]}

    return fares_node_map

def save_reachable_fares(
        fares_node_map: Dict[str, Dict[str, List[OneWayFare]]],
        path: Path,
        filename: str = "fares.csv"
    ):

    with open(path / filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "dep_time", "arr_time", "origin", "destination", "fare", "left", "currency", "key"
        ])
        writer.writeheader()
        for fares_by_dest in fares_node_map.values():
            for fares in fares_by_dest.values():
                for fare in fares:
                    row = fare.to_dict()
                    row['key'] = Ryanair.get_flight_key(fare)
                    writer.writerow(row)

def save_adjacency_list(
        adjacency: Dict[str, List[str]],
        path: Path,
        filename: str = "adjacency.pickle"
    ):

    with open(path / filename, mode='wb') as file:
        pickle.dump(adjacency, file)

def load_adjacency_list(
        path: Path,
        filename: str = "adjacency.pickle"
    ) -> Dict[str, List[str]]:

    with open(path / filename, mode='rb') as file:
        adjacency = pickle.load(file)
    
    return adjacency

def save_closed_paths(
        closed_paths: List[List[str]],
        path: Path,
        filename: str = "closed_paths.json"
    ):

    with open(path / filename, mode='w') as file:
        json.dump(tuple('-'.join(path) for path in closed_paths), file)

def load_closed_paths(
        path: Path,
        filename: str = "closed_paths.json"
    ) -> List[List[str]]:

    with open(path / filename, mode='r') as file:
        return [path.split('-') for path in json.load(file)]
    
def save_ryanair_graph(
        graph: nx.MultiDiGraph,
        path: Path,
        filename: str = "ryanair.json"
    ):
    graph_data = {}
    for u, v, k, data in graph.edges(keys=True, data=True):
        if u not in graph_data:
            graph_data[u] = []
        
        edge_data = {
            'to': v,
            'key': k,
            'departure': int(data['dep_time'].timestamp()),
            'arrival': int(data['arr_time'].timestamp()),
            'weight': float(data['weight']),
            'currency': data['currency']
        }
        graph_data[u].append(edge_data)
    
    with open(path / filename, 'w') as f:
        json.dump(graph_data, f)
    
def load_ryanair_graph(
        path: Path,
        filename: str = "ryanair.json"
    ) -> nx.MultiDiGraph:

    with open(path / filename, mode='r') as file:
        graph_data = json.load(file)
    
    graph = nx.MultiDiGraph()
    for u, edges in graph_data.items():
        for edge in edges:
            graph.add_edge(
                u, 
                edge['to'], 
                key=edge['key'], 
                dep_time=datetime.fromtimestamp(edge['departure']),
                arr_time=datetime.fromtimestamp(edge['arrival']),
                weight=edge['weight'],
                currency=edge['currency']
            )
    
    return graph