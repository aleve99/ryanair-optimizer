import sys
import logging
from pathlib import Path
from json import load
from datetime import date
from dataclasses import dataclass

if sys.version_info.major < 3 or (sys.version_info.major >= 3 and sys.version_info.minor < 11):
    import tomli as tomllib
else:
    import tomllib

with open("src/currency_map.json", "r") as file:
    CURRENCIES = load(file)

logging.basicConfig(format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s', level=logging.INFO)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.ryanair.com/api"

AVAILABILITY = lambda lang, country: f"{BASE_URL}/booking/v4/{lang}-{country}/availability"
ACTIVE_AIRPORTS = f"{BASE_URL}/views/locate/5/airports/en/active"
AVAILABLE_DATES = lambda origin, destination: f"{BASE_URL}/farfnd/v4/oneWayFares/{origin}/{destination}/availabilities"
DESTINATIONS = lambda origin: f"{BASE_URL}/views/locate/searchWidget/routes/en/airport/{origin}"

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

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

def get_payload(origin: str, destination: str, date_out: date, date_in: date, flex_days: int = 0): 
    return AvailabilityPayload(origin, destination, date_out, date_in, FlexDaysIn=flex_days, FlexDaysOut=flex_days)

def parse_toml(path: Path) -> dict:
    with open(path, "rb") as file:
        toml = tomllib.load(file)
    return toml

def hook(response, *args, **kwargs):
    response.raise_for_status()
    logger.debug(kwargs.get('proxies').get('https'))