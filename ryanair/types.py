from dataclasses import dataclass, asdict
from typing import Optional, List
from datetime import datetime, timedelta


@dataclass(frozen=True, eq=True)
class Airport:
    IATA_code: str
    lat: Optional[float]
    lng: Optional[float]
    location: Optional[str]

@dataclass(eq=True, frozen=True)
class RoundTripFare:
    outbound_dep_time: datetime
    outbound_arr_time: datetime
    return_dep_time: datetime
    return_arr_time: datetime
    origin: str
    destination: str
    outbound_fare: float
    outbound_left: int
    return_fare: float
    return_left: int
    currency: str

@dataclass(eq=True, frozen=True)
class OneWayFare:
    dep_time: datetime
    arr_time: datetime
    origin: str
    destination: str
    fare: float
    left: int
    currency: str

    def to_dict(self):
        return asdict(self)

@dataclass(eq=True, frozen=True)
class Schedule:
    origin: str
    destination: str
    departure_time: datetime
    arrival_time: datetime
    flight_number: str

@dataclass(eq=True, frozen=True)
class Stay:
    location: str
    duration: timedelta

@dataclass(eq=True, frozen=True)
class Trip:
    flights: List[OneWayFare]
    total_cost: float
    total_duration: timedelta
    stays: List[Stay]