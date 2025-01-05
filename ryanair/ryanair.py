import logging
import grequests

from typing import Optional, Tuple, List, Iterable, Dict, Union 
from datetime import date, timedelta, datetime
from requests import Response

from .session_manager import SessionManager
from .payload import AvailabilityPayload, get_availabilty_payload
from .types import Airport, OneWayFare, RoundTripFare
from .utils.timer import Timer
from .utils.args_check import check_destinations

logger = logging.getLogger("ryanair")

class Ryanair:
    BASE_API_URL = "https://www.ryanair.com/api/"
    SERVICES_API_URL = "https://services-api.ryanair.com/"
    FLIGHT_PAGE_URL = "https://www.ryanair.com/en/us/trip/flights/select"
    FLEX_DAYS = 6

    def __init__(
            self,
            rid: str,
            ridsig: str,
            origin: str,
            USD: Optional[bool] = False
        ) -> None:
        
        if USD:
            self._currency_str = "en-us/"
        else:
            self._currency_str = ""
                
        self.sm = SessionManager(
            rid=rid,
            ridsig=ridsig
        )

        self.active_airports = self.get_active_airports()
        self.origin = origin

    @property
    def origin(self) -> Airport:
        return self._origin

    @origin.setter
    def origin(self, airport: Union[str, Airport]):
        if isinstance(airport, str):
            iata_code = airport
        elif isinstance(airport, Airport):
            iata_code = airport.IATA_code
        else:
            raise TypeError(f"{type(airport)} not <str> or <Airport>")
        
        if not any(el.IATA_code == iata_code for el in self.active_airports):
            raise ValueError(f"IATA code {iata_code} not valid")
        
        if isinstance(airport, str):
            self._origin = self.get_airport(iata_code)
        else:
            self._origin = airport
        self.destinations = self.get_destination_codes()

    def get(self, url: str, **kwargs) -> Response:
        res = self.sm.session.get(
            url, 
            **kwargs,
            proxies={},
            timeout=self.sm.timeout)
        return res

    def get_airport(self, iata_code: str) -> Airport:
        res = self.get(self._airport_info_url(iata_code)).json()
        return Airport(
            iata_code,
            res['coordinates']['latitude'],
            res['coordinates']['longitude'],
            res['name']
        )

    def get_available_dates(self, destination: str) -> Tuple[str]:
        trip = f"{self.origin.IATA_code}-{destination}"
        logger.info(f"Getting available dates for {trip}")
        res = self.get(
            self._available_dates_url(self.origin.IATA_code, destination)
        )

        return res.json()
    
    def get_active_airports(self) -> Tuple[Airport, ...]:
        logger.info(f"Getting Ryanair active airports")
        res = self.get(self._active_airports_url())

        return tuple(
            Airport(
                airport['code'],
                airport['coordinates']['latitude'],
                airport['coordinates']['longitude'],
                airport['name']
            ) for airport in res.json()
        )

    def get_round_trip_link(
            self,
            from_date: date,
            to_date: date,
            destination: str
        ) -> str:
        return self.FLIGHT_PAGE_URL + f"?" + \
            "&".join([
                "adults=1",
                "teens=0",
                "children=0",
                "infants=0",
                f"dateOut={from_date}",
                f"dateIn={to_date}",
                "isConnectedFlight=false",
                "discount=0",
                "promoCode=",
                "isReturn=true",
                f"originIata={self.origin.IATA_code}",
                f"destinationIata={destination}"
            ])
    
    def get_one_way_link(
            self,
            from_date: date,
            destination: str
        ) -> str:
        return self.FLIGHT_PAGE_URL + "?" + \
            "&".join([
                "adults=1",
                "teens=0",
                "children=0",
                "infants=0",
                f"dateOut={from_date}",
                "isConnectedFlight=false",
                "discount=0",
                "promoCode=",
                "isReturn=false",
                f"originIata={self.origin.IATA_code}",
                f"destinationIata={destination}"
            ])

    def get_availability(self, payload: AvailabilityPayload) -> dict:
        res = self.get(
            self._availabilty_url(),
            params=payload.to_dict()
        )

        return res.json()
    
    def get_destination_codes(self) -> Tuple[str, ...]:
        logger.info(f"Getting destinations for {self.origin.IATA_code}")
        res = self.get(
            self._destinations_url(self.origin.IATA_code)
        )

        return tuple(
            dest['arrivalAirport']['code'] for dest in res.json()
        )

    def search_round_trip_fares(
            self,
            min_nights: int,
            max_nights: int,
            from_date: date,
            to_date: date = None,
            destinations: Iterable[str] = []
        ) -> List[RoundTripFare]:
        
        destinations = check_destinations(destinations, self.destinations)

        timer = Timer(start=True)

        fares = self._execute_and_compute(
            code_requests_map=self._prepare_search_requests(
                from_date=from_date,
                to_date=to_date,
                destinations=destinations
            ),
            min_nights=min_nights,
            max_nights=max_nights
        )

        timer.stop()

        logger.info(f"Scraped round-trip fares in {timer.seconds_elapsed()}s")

        return fares
    
    def search_one_way_fares(
            self,
            from_date: date,
            to_date: date = None,
            destinations: Iterable[str] = []
        ) -> List[OneWayFare]:
        
        destinations = check_destinations(destinations, self.destinations)
        
        timer = Timer(start=True)

        code_requests_map = self._prepare_search_requests(
            from_date=from_date,
            to_date=to_date,
            destinations=destinations,
            round_trip=False
        )
        
        fares = []
        for dest, requests in code_requests_map.items():
            reponses = self._execute_search_requests(requests)

            for res in reponses:
                json_res = res.json()
                currency = json_res['currency']

                for date in json_res['trips'][0]['dates']:
                    for flight in filter(
                        lambda fl: fl['faresLeft'] != 0, date['flights']
                    ):
                        fares.append(
                            OneWayFare(
                                datetime.fromisoformat(flight['time'][0]),
                                datetime.fromisoformat(flight['time'][1]),
                                self.origin.IATA_code,
                                dest,
                                flight['regularFare']['fares'][0]['amount'],
                                flight['faresLeft'],
                                currency
                            )
                        )

        timer.stop()

        logger.info(f"Scraped one-way fares in {timer.seconds_elapsed()}s")

        return fares

    def _prepare_search_requests(
            self,
            from_date: date,
            to_date: 'date | None',
            destinations: Iterable[str],
            round_trip: bool = True
        ) -> Dict[str, List[grequests.AsyncRequest]]:
        
        requests = dict()
        for code in destinations:
            reqs = list()
            
            if to_date is None:
                str_dates = self.get_available_dates(code)
                _to_date = date.fromisoformat(str_dates[-1])
            else:
                _to_date = to_date

            _from_date = from_date
            
            while _from_date <= _to_date:
                if (_to_date - _from_date).days >= self.FLEX_DAYS:
                    flex_days = self.FLEX_DAYS
                else:
                    flex_days = (_to_date - _from_date).days
                
                params = get_availabilty_payload(
                    origin=self.origin.IATA_code,
                    destination=code,
                    date_out=_from_date,
                    date_in=_from_date,
                    flex_days=flex_days,
                    round_trip=round_trip
                )

                reqs.append(
                    grequests.get(
                        url=self._availabilty_url(),
                        params=params.to_dict(),
                        session=self.sm.session,
                        timeout=self.sm.timeout
                    )
                )

                _from_date += timedelta(days=flex_days + 1)
                self.sm.set_next_proxy()
            
            requests[code] = reqs
        
        return requests

    def _search_exec_handler(
            self,
            request: grequests.AsyncRequest,
            exception: Exception
        ) -> Optional[Response]:
        
        logger.warning(f"Request failed. Exception type = {type(exception)}")
        for arg in exception.args:
            logger.warning(arg)
        
        logger.info("Retrying with next proxy")
        
        self.sm.set_next_proxy()
        response = self.get(
            url=request.url,
            params=request.kwargs['params'],
        )

        logger.info(f"Response code <{response.status_code}>")

        return response

    def _execute_and_compute(
            self,
            code_requests_map: Dict[str, List[grequests.AsyncRequest]],
            min_nights: int,
            max_nights: int
        ) -> List[RoundTripFare]:

        fares, timer = list(), Timer()

        for code, requests in code_requests_map.items():
            timer.start()
            responses = self._execute_search_requests(requests)

            if responses:
                fares.extend(
                    self._compute_responses(
                        responses=responses,
                        min_nights=min_nights,
                        max_nights=max_nights
                    )
                )

            timer.stop()

            trip = f"{self.origin.IATA_code}-{code}"
            logger.info(
                f"{trip} scraped in {timer.seconds_elapsed()}s"
            )

        return fares

    def _execute_search_requests(
            self,
            requests: List[grequests.AsyncRequest]
        ) -> List[Response]:

        return grequests.map(
            requests=requests, 
            size=self.sm.pool_size,
            exception_handler=self._search_exec_handler
        )
    
    def _compute_responses(
            self,
            responses: List[Response],
            min_nights: int,
            max_nights: int
        ) -> List[RoundTripFare]:

        for req_num, res in enumerate(responses):
            if res and req_num == 0:
                json_res = res.json()
                trips = json_res['trips']
                currency = json_res['currency']
            elif res and req_num != 0:
                json_res = res.json()
                for j in range(2):
                    trips[j]['dates'].extend(
                        json_res['trips'][j]['dates']
                    )
            else:
                logger.warning(f"Request {res} is None")
                continue

        fares = []
        for trip_date_out in trips[0]['dates']:
            date_out = date.fromisoformat(trip_date_out['dateOut'][:10])

            for outbound_flight in trip_date_out['flights']:

                if outbound_flight['faresLeft'] != 0:
                    for i in range(len(trips[1]['dates'])):
                        trip_date_in = trips[1]['dates'][i]
                        
                        date_in = date.fromisoformat(
                            trip_date_in['dateOut'][:10]
                        )

                        if date_in < date_out:
                            i = (date_out - date_in).days
                            continue
                        elif min_nights <= (date_in - date_out).days <= max_nights:
                            for return_flight in trip_date_in['flights']:

                                if return_flight['faresLeft'] != 0:
                                    fares.append(RoundTripFare(
                                        datetime.fromisoformat(
                                            outbound_flight['time'][0]
                                        ),
                                        datetime.fromisoformat(
                                            outbound_flight['time'][1]
                                        ),
                                        datetime.fromisoformat(
                                            return_flight['time'][0]
                                        ),
                                        datetime.fromisoformat(
                                            return_flight['time'][1]
                                        ),
                                        trips[0]['origin'],
                                        trips[0]['destination'],
                                        outbound_flight['regularFare']['fares'][0]['amount'],
                                        outbound_flight['faresLeft'],
                                        return_flight['regularFare']['fares'][0]['amount'],
                                        return_flight['faresLeft'],
                                        currency
                                    ))
                        elif date_in != date_out:
                            break
        
        return fares
    
    @classmethod
    def _airport_info_url(cls, iata_code: str) -> str:
        return cls.BASE_API_URL + f'views/locate/5/airports/en/{iata_code}'

    @classmethod
    def _available_dates_url(cls, origin: str, destination: str) -> str:
        return cls.BASE_API_URL + \
            f"farfnd/v4/oneWayFares/{origin}/{destination}/availabilities"
    
    @classmethod
    def _active_airports_url(cls) -> str:
        return cls.BASE_API_URL + "views/locate/5/airports/en/active"
    
    @classmethod
    def _destinations_url(cls, origin: str) -> str:
        return cls.BASE_API_URL + \
            f"views/locate/searchWidget/routes/en/airport/{origin}"
    
    @classmethod
    def _one_way_fares_url(cls) -> str:
        return cls.SERVICES_API_URL + "farfnd/v4/oneWayFares"

    @classmethod
    def _round_trip_fares_url(cls) -> str:
        return cls.SERVICES_API_URL + "farfnd/v4/roundTripFares"

    def _availabilty_url(self) -> str:
        return self.BASE_API_URL + \
            f"booking/v4/{self._currency_str}availability"
