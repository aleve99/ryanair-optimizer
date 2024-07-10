import requests
import pandas as pd
from typing import Optional
from dataclasses import dataclass
from datetime import date, time, datetime
from time import perf_counter_ns

import ryanair.utils as utils

config = utils.parse_toml("src/config.toml")

one_way_fares = "https://services-api.ryanair.com/farfnd/v4/oneWayFares"
round_trip_fares = "https://services-api.ryanair.com/farfnd/v4/roundTripFares"

@dataclass
class oneWayPayload:
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
class roundTripPayload:
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

payload = roundTripPayload(
    departureAirportIataCode = "TRS",
    outboundDepartureDateFrom = date(2024,7,3),
    outboundDepartureDateTo = date(2024,7,7),
    inboundDepartureDateFrom = date(2024,7,3),
    inboundDepartureDateTo = date(2024,7,7),
    durationFrom=1,
    durationTo=1
)


#res = requests.get(round_trip_fares, params=payload.to_dict())
#res.raise_for_status()
#
#fares = res.json()['fares']
#
#for fare in fares:
#    dt_outdep = datetime.fromisoformat(fare['outbound']['departureDate'])
#    dt_indep = datetime.fromisoformat(fare['inbound']['departureDate'])
#    print(fare['outbound']['departureAirport']['iataCode'], fare['outbound']['arrivalAirport']['iataCode'], dt_outdep, dt_outdep.weekday(), dt_indep, dt_indep.weekday(), fare['summary']['price']['value'])

payload = utils.get_payload("MXP", "DUB", date(2024,7,5), date(2024,12,31))

days = 6

payload.FlexDaysBeforeIn = 0
payload.FlexDaysBeforeOut = 0

payload.FlexDaysIn = days
payload.FlexDaysOut = days

res = requests.get(utils.AVAILABILITY("en", "us"), params=payload.to_dict(), headers=utils.headers, cookies=config['cookies'])

print(res.json(), end="\n\n")

for trip in res.json()['trips']:
    print(trip['origin'], trip['destination'], end=" ")
    for d in trip['dates']:
        print(d['dateOut'], end=" ")
    print()
print()