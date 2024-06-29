import requests
import locale
from argparse import ArgumentParser
from datetime import date, timedelta
from tqdm import tqdm
from time import sleep

loc = locale.getlocale()[0].split("_")
loc = f"{loc[0].lower()}-{loc[1].lower()}"

BASE_URL = "https://www.ryanair.com/api"

AVAILABILITY = f"{BASE_URL}/booking/v4/{loc}/availability"
ACTIVE_AIRPORTS = f"{BASE_URL}/views/locate/5/airports/en/active"
AVAILABLE_DATES = lambda origin, destination: f"{BASE_URL}/farfnd/v4/oneWayFares/{origin}/{destination}/availabilities"
DESTINATIONS = lambda origin: f"{BASE_URL}/views/locate/searchWidget/routes/en/airport/{origin}"

payload = lambda origin, destination, date_out, date_in: {
    "ADT": 1,
    "TEEN": 0,
    "CHD": 0,
    "INF": 0,
    "Origin": origin,
    "Destination": destination,
    "promoCode": "",
    "IncludeConnectingFlights": False,
    "DateOut": date_out.strftime("%Y-%m-%d"),
    "DateIn": date_in.strftime("%Y-%m-%d"),
    "FlexDaysBeforeOut": 0,
    "FlexDaysOut": 0,
    "FlexDaysBeforeIn": 0,
    "FlexDaysIn": 0,
    "RoundTrip": True,
    "ToUs": "AGREED",
}

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"}

cookies = {
    'rid': '6dad7e8c-425f-4294-9de3-bbd8e6bbdc1b',
    'rid.sig': 'jyna6R42wntYgoTpqvxHMK7H+KyM6xLed+9I3KsvYZaVt7P36AL6zp9dGFPu5uVxaIiFpNXrszr+LfNCdY3IT3oCSYLeNv/ujtjsDqOzkY5JmUFsCdAEz3kpPbhCUwiAt/vQmRn3hREI7zVdoZEQRlIxg+JQgZr7xof7l5bqUoYk5r1E2GfQ5gCk3SrXFOOL1I22oV1G8pkY/xDePp76SSOTrB/47p9I+9RJ2RiQfQkI1kkawfj1UbQq7ntmFQC4vphbXob9Z8EQJ58VTNmd/OHNBtkyfOMXCzCpgrsMYkaRaIvq2yqlEi/Ug77EHxYCcJDyK3vfQ7liPd7BFeLw6r4ZnCRS0ED0vsy2gPCCYLgjiobVgem+ByKWBxO61uH2qYOmKIhVJx7bEkc3BXH6/DxrIdOkKtj46VWKcDxkI8groRKy2oKCkBxKXA8U2XSdpvfEvzT5DY3gMjhVpWcOETjuTHNuJcZN3zopQu9IvmSa5YfIlyPm/TcrHK4QUWNzcVxTaZMQh3C8QrNc7NIDesurdbrIIwAzTeGwzqxPeMtm67Esgapl8iprq9axS1uXK2M1KJhwPJtQWFFyz9u93vtLIEysXf7fTkntQ9xGnNU+bbRDS4Px4blQCWOL3dOdG0C0YZI20kODggGoeAseQz+j2G9dWQkqQe31zX9/DiCVqxB6osuc9gyksmJaXvrWcYhyzI88I31iRwVUvawTAnUnelgGlOc0GDYu98NwXwI/F78D567AnN+YPoxMGbbQiU6TZZQbRUNJBR5VxRi/92AbGdW2adIzdIiU6RkOubU8SoHqln3rNu64ZmB56z0WQJDL29m8QO9Vp+JVUH+X6Bjxr70PtdsXQbdLyDwyQ5PFpuTyW3ti1hU0tcoXYrVMqbE/68p3yyUUGSYvPbmqIJkcHAdYRd+lDxrlV+xbEJ6OQFkcPPIDBKJYsD6Nw4yBktancAm/TQjoFGpFHT6l9uSm3WLnUHOEtKDaSAm6oqA=',
}

def main():
    parser = ArgumentParser("Ryanair fares finder", description="Find the cheapest fares from an airport origin")

    parser.add_argument("--origin", required=True, type=str, help="The origin airport (airport IATA code)")
    parser.add_argument("--max_days", required=True, type=int, help="The maximum days of vacation")
    parser.add_argument("--min_days", default=1, type=int, help="The minimum days of vacation")
    parser.add_argument("--from_date", default=date.today() + timedelta(days=1), type=date, help="The start date (yyyy-mm-dd) where to search, default is tomorrow")

    args = parser.parse_args()

    if res := requests.get(DESTINATIONS(args.origin)):
        destinations = res.json()
    else:
        res.raise_for_status()

    destination_codes = [dest['arrivalAirport']['code'] for dest in destinations]
    fares = []

    for destination in destination_codes:
        print(f"Scraping fares from {args.origin} to {destination}...")
        if res := requests.get(AVAILABLE_DATES(args.origin, destination)):
            available_dates = list(filter(lambda d: d >= args.from_date, [date.fromisoformat(d) for d in res.json()]))
        else:
            res.raise_for_status()

        for date_out in tqdm(available_dates):
            for days in range(args.min_days, args.max_days):
                date_in = date_out + timedelta(days=days)
                
                if date_in not in available_dates:
                    continue
                
                params = payload(args.origin, destination, date_out, date_in)

                if res := requests.get(AVAILABILITY, params, headers=headers, cookies=cookies):
                    trips = res.json()['trips']
                    fares.append({
                        'date_out': date_out.strftime("%Y-%m-%d"),
                        'date_in': date_in.strftime("%Y-%m-%d"),
                        'origin': args.origin,
                        'destination': destination,
                        'outbound_fare': trips[0]['dates'][0]['flights'][0]['regularFare']['fares'][0]['amount'],
                        'outbound_left': trips[0]['dates'][0]['flights'][0]['faresLeft'],
                        'return_fare': trips[1]['dates'][0]['flights'][0]['regularFare']['fares'][0]['amount'],
                        'return_left': trips[1]['dates'][0]['flights'][0]['faresLeft']
                    })
                else:
                    res.raise_for_status()
                
                sleep(0.1)

if __name__ == "__main__":
    main()