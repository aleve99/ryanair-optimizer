import logging
import locale
import grequests

from typing import Optional, Tuple, List, Iterable
from datetime import date, timedelta, datetime
from requests import Response

from .session_manager import SessionManager
from .payload import AvailabilityPayload, get_availabilty_payload
from .types import Airport, Fare
from .utils.timer import Timer

logger = logging.getLogger("ryanair")

class Ryanair:
    BASE_API_URL = "https://www.ryanair.com/api/"
    SERVICES_API_URL = "https://services-api.ryanair.com/"
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
        self.destinations = self.get_destination_codes()

    @property
    def origin(self) -> Airport:
        return self._origin

    @origin.setter
    def origin(self, iata_code: str):
        if not any(el.IATA_code == iata_code for el in self.active_airports):
            raise ValueError(f"IATA code {iata_code} not valid")
    
        self._origin = self.get_airport(iata_code)
        
    def get(self, url: str, **kwargs) -> Response:
        res = self.sm.session.get(url, **kwargs, proxies={})
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
        res = self.get(
            self._available_dates_url(self.origin.IATA_code, destination)
        )

        return res.json()
    
    def get_active_airports(self) -> Tuple[Airport, ...]:
        res = self.get(self._active_airports_url())

        return tuple(
            Airport(
                airport['code'],
                airport['coordinates']['latitude'],
                airport['coordinates']['longitude'],
                airport['name']
            ) for airport in res.json()
        )

    def get_availability(self, payload: AvailabilityPayload) -> dict:
        res = self.get(
            self._availabilty_url(),
            params=payload.to_dict()
        )

        return res.json()
    
    def get_destination_codes(self) -> Tuple[str, ...]:
        res = self.get(
            self._destinations_url(self.origin.IATA_code)
        )

        return tuple(
            dest['arrivalAirport']['code'] for dest in res.json()
        )

    def search_fares(
            self,
            min_nights: int,
            max_nights: int,
            from_date: date,
            to_date: date = None,
            destinations: Iterable[str] = []
        ) -> List[Fare]:
        
        if not destinations:
            destinations = self.destinations
        else:
            not_valid = tuple(
                filter(
                    lambda dest: dest not in self.destinations,
                    destinations
                )
            )
            if not_valid:
                raise ValueError(f"Destinations {not_valid} not valid")

        timer = Timer()
        timer.start()
        requests = self._prepare_search_requests(
            from_date,
            to_date,
            destinations
        )
        responses = self._execute_search_requests(requests)

        fares = self._compute_responses(responses, min_nights, max_nights)
        timer.stop()

        logger.info(f"Scraped fares in {timer.seconds_elapsed()}s")
        return fares
    
    def _prepare_search_requests(
            self,
            from_date: date,
            to_date: date,
            destinations: Iterable[str]
        ) -> List[grequests.AsyncRequest]:
        
        reqs = list()
        for code in destinations:
            str_dates = self.get_available_dates(code)
            
            if not to_date:
                to_date = date.fromisoformat(str_dates[-1])

            dynamic_date = from_date
            
            while dynamic_date <= to_date:
                if (to_date - dynamic_date).days >= self.FLEX_DAYS:
                    flex_days = self.FLEX_DAYS
                else:
                    flex_days = (to_date - dynamic_date).days
                
                params = get_availabilty_payload(
                    self.origin.IATA_code,
                    code,
                    dynamic_date,
                    dynamic_date,
                    flex_days
                ).to_dict()

                reqs.append(
                    grequests.get(
                        url=self._availabilty_url(),
                        params=params,
                        session=self.sm.session
                    )
                )

                dynamic_date += timedelta(days=flex_days + 1)
                self.sm.set_next_proxy()
        
        return reqs

    @staticmethod
    def _search_exec_handler(
            request: grequests.AsyncRequest,
            exception: Exception
        ) -> Optional[Response]:

        for arg in exception.args:
            logger.warning(arg)

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
        ) -> List[Fare]:

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
                        date_in = date.fromisoformat(trip_date_in['dateOut'][:10])

                        if date_in < date_out:
                            i = (date_out - date_in).days
                            continue
                        elif min_nights <= (date_in - date_out).days <= max_nights:
                            for return_flight in trip_date_in['flights']:
                                if return_flight['faresLeft'] != 0:
                                    fares.append(Fare(
                                        datetime.fromisoformat(outbound_flight['time'][0]),
                                        datetime.fromisoformat(outbound_flight['time'][1]),
                                        datetime.fromisoformat(return_flight['time'][0]),
                                        datetime.fromisoformat(return_flight['time'][1]),
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
