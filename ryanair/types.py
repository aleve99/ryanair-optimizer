from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class Airport:
    IATA_code: str
    lat: Optional[float]
    lng: Optional[float]
    location: Optional[str]

@dataclass
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

@dataclass
class OneWayFare:
    outbound_dep_time: datetime
    outbound_arr_time: datetime
    origin: str
    destination: str
    outbound_fare: float
    outbound_left: int
    currency: str