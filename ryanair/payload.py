from datetime import date, time
from dataclasses import dataclass
from typing import Optional

@dataclass
class AvailabilityPayload:
    Origin: str
    Destination: str
    DateOut: date
    DateIn: date
    ADT: int = 1
    TEEN: int = 0
    CHD: int = 0
    INF: int = 0
    promoCode: str = "" 
    IncludeConnectingFlights: bool = False
    FlexDaysBeforeOut: int = 0
    FlexDaysOut: int = 0
    FlexDaysBeforeIn: int = 0
    FlexDaysIn: int = 0
    RoundTrip: bool = True
    ToUs: str = "AGREED"

    def to_dict(self):
        return vars(self)


@dataclass
class FarfndOneWayPayload:
    departureAirportIataCode: str = ""
    outboundDepartureDateFrom: date = None
    outboundDepartureDateTo: date = None
    arrivalAirportIataCodes: Optional[list] = None
    outboundDepartureTimeFrom: Optional[time] = time(0,0)
    outboundDepartureTimeTo: Optional[time] = time(23,59)
    outboundDepartureDaysOfWeek: Optional[str] = None
    adultPaxCount: Optional[int] = 1
    market: Optional[str] = "it-it"
    promoCode: Optional[str] = "undefined"

    def to_dict(self) -> dict:
        return vars(self)

@dataclass
class FarfndRoundTripPayload:
    departureAirportIataCode: str = ""
    outboundDepartureDateFrom: date = None
    outboundDepartureDateTo: date = None
    inboundDepartureDateFrom: date = None
    inboundDepartureDateTo: date = None
    arrivalAirportIataCodes: Optional[list] = None
    durationFrom: int = None
    durationTo: int = None
    outboundDepartureTimeFrom: Optional[time] = time(0,0)
    outboundDepartureTimeTo: Optional[time] = time(23,59)
    inboundDepartureTimeFrom: Optional[time] = time(0,0)
    inboundDepartureTimeTo: Optional[time] = time(23,59)
    outboundDepartureDaysOfWeek: str = None
    inboundDepartureDaysOfWeek: str = None
    adultPaxCount: Optional[int] = 1
    market: Optional[str] = "it-it"
    promoCode: Optional[str] = "undefined"

    def to_dict(self) -> dict:
        return vars(self)
    

def get_availabilty_payload(
        origin: str,
        destination: str,
        date_out: date,
        date_in: date,
        flex_days: int = 0,
        round_trip: bool = True
    ) -> AvailabilityPayload:

    return AvailabilityPayload(
        Origin=origin,
        Destination=destination,
        DateOut=date_out,
        DateIn=date_in,
        FlexDaysIn=flex_days,
        FlexDaysOut=flex_days,
        RoundTrip=round_trip
    )