import logging
import grequests

from copy import deepcopy
from typing import Optional, Tuple, List, Iterable, Dict, Any
from datetime import date, timedelta, datetime, time
from requests import Response, get
from pathlib import Path

from .utils.config import parse_toml
from .session_manager import SessionManager
from .payload import AvailabilityPayload, get_availabilty_payload, get_farfnd_one_way_payload
from .types import Airport, OneWayFare, RoundTripFare, Schedule
from .utils.timer import Timer

logger = logging.getLogger("ryanair")

class Ryanair:
    BASE_API_URL = "https://www.ryanair.com/api/"
    SERVICES_API_URL = "https://services-api.ryanair.com/"
    FLIGHT_PAGE_URL = "https://www.ryanair.com/en/us/trip/flights/select"
    FLEX_DAYS = 6

    def __init__(
            self,
            config: Dict[str, Any],
            USD: Optional[bool] = False
        ) -> None:
        
        if USD:
            self._currency_str = "en-us/"
            self._market = "en-us"
        else:
            self._currency_str = ""
            self._market = "it-it"

        self.sm = SessionManager(
            timeout=config['network']['timeout'],
            pool_size=config['network']['pool_size']
        )

        self.active_airports = self.get_active_airports()

    def get(self, url: str, **kwargs) -> Response:
        self.sm.set_next_proxy()
        while True:
            try:
                res = self.sm.session.get(
                    url=url,
                    timeout=self.sm.timeout,
                    **kwargs
                )
                break
            except Exception as e:
                logger.warning(f"Request failed. Exception type = {type(e)}")
                logger.warning(f"Retrying with next proxy")
                self.sm.set_next_proxy()
                
        return res

    def get_airport(self, iata_code: str) -> Optional[Airport]:
        for airport in self.active_airports:
            if airport.IATA_code == iata_code:
                return airport
        
        return None

    def get_available_dates(self, origin: str, destination: str) -> Tuple[str]:
        trip = f"{origin}-{destination}"
        logger.info(f"Getting available dates for {trip}")
        res = self.get(
            self._available_dates_url(origin, destination),
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

    def get_schedules(
            self,
            origin: str,
            destination: str,
            year: int = None,
            month: int = None,
            response: Response = None
        ) -> List[Schedule]:

        schedules: List[Schedule] = []
        
        if response is None:
            if year is None or month is None:
                raise ValueError("Year and month must be provided if response is not provided")
            res = self.get(self._schedules_url(origin, destination, year, month))
        else:
            res = response
            url_split = response.url.split('/')
            year, month = int(url_split[-3]), int(url_split[-1])

        for day in res.json()['days']:
            day_date = date(year, month, day['day'])
            for flight in day['flights']:
                dep_time = time.fromisoformat(flight['departureTime'])
                arr_time = time.fromisoformat(flight['arrivalTime'])
                dep_datetime = datetime.combine(day_date, dep_time)

                if arr_time >= time(0, 0):
                    arr_datetime = datetime.combine(day_date + timedelta(days=1), arr_time)
                else:
                    arr_datetime = datetime.combine(day_date, arr_time)
                
                schedule = Schedule(
                    origin=origin,
                    destination=destination,
                    arrival_time=arr_datetime,
                    departure_time=dep_datetime,
                    flight_number=flight['carrierCode'] + flight['number']
                )

                schedules.append(schedule)

        return schedules

    def get_round_trip_link(
            self,
            from_date: date,
            to_date: date,
            origin: str,
            destination: str
        ) -> str:
        if not self.get_airport(origin):
            raise ValueError(f"IATA code {origin} not valid")
        
        if not self.get_airport(destination):
            raise ValueError(f"IATA code {destination} not valid")
        
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
                f"originIata={origin}",
                f"destinationIata={destination}"
            ])
    
    def get_one_way_link(
            self,
            from_date: date,
            origin: str,
            destination: str
        ) -> str:
        if not self.get_airport(origin):
            raise ValueError(f"IATA code {origin} not valid")
        
        if not self.get_airport(destination):
            raise ValueError(f"IATA code {destination} not valid")
        
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
                f"originIata={origin}",
                f"destinationIata={destination}"
            ])

    def get_availability(self, payload: AvailabilityPayload) -> dict:
        res = self.get(
            self._availabilty_url(),
            params=payload.to_dict()
        )

        return res.json()
    
    def get_destination_codes(self, origin: str) -> Tuple[str, ...]:
        logger.info(f"Getting destinations for {origin}")
        res = self.get(
            self._destinations_url(origin)
        )

        return tuple(
            dest['arrivalAirport']['code'] for dest in res.json()
        )

    def search_round_trip_fares(
            self,
            origin: str,
            min_nights: int,
            max_nights: int,
            from_date: date,
            to_date: date = None,
            destinations: Iterable[str] = []
        ) -> List[RoundTripFare]:
        
        if not destinations:
            destinations = self.get_destination_codes(origin)

        timer = Timer(start=True)

        fares = self._execute_and_compute_availability(
            origin=origin,
            code_requests_map=self._prepare_availability_requests(
                origin=origin,
                from_date=from_date,
                to_date=to_date,
                destinations=destinations
            ),
            min_nights=min_nights,
            max_nights=max_nights
        )

        timer.stop()

        logger.info(f"Scraped round-trip fares in {timer.seconds_elapsed}s")

        return fares
    
    def search_one_way_fares(
            self,
            origin: str,
            from_date: date,
            to_date: date = None,
            destinations: Iterable[str] = []
        ) -> List[OneWayFare]:
        
        if not destinations:
            destinations = self.get_destination_codes(origin)
        
        timer = Timer(start=True)

        code_requests_map = self._prepare_availability_requests(
            origin=origin,
            from_date=from_date,
            to_date=to_date,
            destinations=destinations,
            round_trip=False
        )
        
        fares = []
        for dest, requests in code_requests_map.items():
            reponses = self._execute_requests(requests)

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
                                origin,
                                dest,
                                flight['regularFare']['fares'][0]['amount'],
                                flight['faresLeft'],
                                currency
                            )
                        )

        timer.stop()

        logger.info(f"Scraped one-way fares in {timer.seconds_elapsed}s")

        return fares

    def search_one_way_fares_v2(
            self,
            origin: str,
            from_date: date,
            to_date: date = None,
            destinations: Iterable[str] = []
        ) -> List[OneWayFare]:
        
        if not destinations:
            destinations = self.get_destination_codes(origin)

        timer = Timer(start=True)

        requests = self._prepare_availability_requests_v2(
            origin=origin,
            from_date=from_date,
            to_date=to_date,
            destinations=destinations,
            round_trip=False
        )

        responses = self._execute_requests(requests)
        timer.stop()

        logger.info(f"Scraped {origin} one-way fares in {timer.seconds_elapsed}s")

        fares = []

        for res in responses:
            json_res = res.json()
            if json_res['nextPage'] is not None:
                print(json_res)
                raise ValueError("Next page is not None")

            for flight in json_res['fares']:
                info = flight['outbound']

                fares.append(
                    OneWayFare(
                        datetime.fromisoformat(info['departureDate']),
                        datetime.fromisoformat(info['arrivalDate']),
                        origin,
                        info['arrivalAirport']['iataCode'],
                        info['price']['value'],
                        -1,
                        info['price']['currencyCode']
                    )
                )

        return fares

    def _prepare_availability_requests(
            self,
            origin: str,
            from_date: date,
            to_date: 'date | None',
            destinations: Iterable[str],
            round_trip: bool = True
        ) -> Dict[str, List[grequests.AsyncRequest]]:
        
        requests = dict()
        for code in destinations:
            reqs = list()
            
            if to_date is None:
                str_dates = self.get_available_dates(origin, code)
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
                    origin=origin,
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

    def _prepare_availability_requests_v2(
            self,
            origin: str,
            from_date: date,
            to_date: date,
            destinations: Iterable[str],
            round_trip: bool = True
        ) -> Dict[str, List[grequests.AsyncRequest]]:

        requests = []

        schedules_by_code = self._execute_and_compute_schedules(
            origin=origin,
            code_requests_map=self._prepare_schedules_requests(
                origin=origin,
                destinations=destinations,
                from_date=from_date,
                to_date=to_date
            ),
            destinations=destinations,
            from_date=from_date,
            to_date=to_date
        )

        for days in range(0, (to_date - from_date).days + 1):
            date_from = from_date + timedelta(days=days)
            date_to = date_from

            day_schedules: List[Schedule] = []
            for schedules in schedules_by_code.values():
                for schedule in schedules:
                    if schedule.departure_time.date() == date_from:
                        day_schedules.append(schedule)

            if not day_schedules:
                continue

            day_schedules.sort(key=lambda s: s.departure_time)
            time_ranges = []
            
            time_from = day_schedules[0].departure_time.time()
            time_to = time_from

            counts, i = {code: 0 for code in destinations}, 0
            while i < len(day_schedules):
                schedule = day_schedules[i]
                
                if counts[schedule.destination] == 1:
                    time_to = (datetime.combine(
                        date=datetime.today(),
                        time=schedule.departure_time.time()
                    ) - timedelta(minutes=1)).time()

                    time_ranges.append((time_from, time_to))
                    time_from = schedule.departure_time.time()
                    time_to = time_from
                    same_time = tuple(filter(
                        lambda s: s.departure_time.time() == schedule.departure_time.time(),
                        day_schedules
                    ))
                    counts = {code: 0 for code in destinations}

                    for s in same_time:
                        counts[s.destination] += 1

                    i = day_schedules.index(same_time[-1])
                    
                else:
                    counts[schedule.destination] += 1
                    time_to = schedule.departure_time.time()
                
                i += 1

            time_ranges.append((time_from, time_to))

            for time_from, time_to in time_ranges:
                params = get_farfnd_one_way_payload(
                    origin=origin,
                    destinations=destinations,
                    date_from=date_from,
                    date_to=date_to,
                    time_from=time_from,
                    time_to=time_to,
                    market=self._market
                )

                requests.append(
                    grequests.get(
                        url=self._one_way_fares_url(),
                        params=params.to_dict(),
                        session=self.sm.session,
                        timeout=self.sm.timeout
                    )
                )

        return requests

    def _prepare_schedules_requests(
            self,
            origin: str,
            destinations: Iterable[str],
            from_date: date,
            to_date: date
        ) -> Dict[str, List[grequests.AsyncRequest]]:

        requests = dict()

        for destination in destinations:
            reqs = list()
            for year in range(from_date.year, to_date.year + 1):
                if year == from_date.year and from_date.year != to_date.year:
                    month_range = range(from_date.month, 13)
                elif year == to_date.year and from_date.year != to_date.year:
                    month_range = range(1, to_date.month + 1)
                elif year == from_date.year and from_date.year == to_date.year:
                    month_range = range(from_date.month, to_date.month + 1)
                else:
                    month_range = range(1, 13)

                for month in month_range:
                    url = self._schedules_url(
                        origin=origin,
                        destination=destination,
                        year=year,
                        month=month
                    )

                    reqs.append(
                        grequests.get(
                            url=url,
                            session=self.sm.session,
                            timeout=self.sm.timeout
                        )
                    )

            requests[destination] = reqs

        return requests

    def _exec_handler(
            self,
            request: grequests.AsyncRequest,
            exception: Exception
        ) -> Optional[Response]:
        
        logger.warning(f"Request failed. Exception type = {type(exception)}")
        if request.response:
            logger.warning(f"Request URL: {request.response.url}")
            logger.warning(f"{request.response.text}")
        
        for arg in exception.args:
            logger.warning(arg)
        
        logger.info("Retrying with next proxy")
        
        for _ in range(10):
            self.sm.set_next_proxy()
            params = request.kwargs.get('params')
            try:
                response = self.get(
                    url=request.url,
                    params=params,
                )
            except Exception as e:
                logger.warning(f"Request failed again. Exception type = {type(e)}")
                continue
            break
        else:
            logger.info("Tried 10 times, trying without proxy")
            response = get(
                url=request.url,
                params=params,
                cookies=self.sm.session.cookies,
                timeout=self.sm.timeout,
                headers=self.sm.session.headers
            )

        logger.info(f"Response code <{response.status_code}>")

        return response

    def _execute_and_compute_availability(
            self,
            origin: str,
            code_requests_map: Dict[str, List[grequests.AsyncRequest]],
            min_nights: int,
            max_nights: int
        ) -> List[RoundTripFare]:

        fares, timer = list(), Timer()

        for code, requests in code_requests_map.items():
            timer.start()
            responses = self._execute_requests(requests)

            if responses:
                fares.extend(
                    self._compute_responses(
                        responses=responses,
                        min_nights=min_nights,
                        max_nights=max_nights
                    )
                )

            timer.stop()

            trip = f"{origin}-{code}"
            logger.info(
                f"{trip} scraped in {timer.seconds_elapsed}s"
            )

        return fares

    def _execute_and_compute_schedules(
            self,
            origin: str,
            code_requests_map: Dict[str, List[grequests.AsyncRequest]],
            destinations: Iterable[str],
            from_date: date,
            to_date: date
        ) -> Dict[str, List[Schedule]]:

        schedules: Dict[str, List[Schedule]] = dict()
        timer = Timer()
        
        for destination in destinations:
            timer.start()
            responses = self._execute_requests(
                code_requests_map[destination]
            )

            schedules[destination] = []
            for response in responses:
                scheds = self.get_schedules(
                    origin=origin,
                    destination=destination,
                    response=response
                )
                for sched in scheds:
                    if from_date <= sched.departure_time.date() <= to_date:
                        schedules[destination].append(sched)

            timer.stop()

            trip = f"{origin}-{destination}"
            logger.info(
                f"{trip} schedules scraped in {timer.seconds_elapsed}s"
            )

        return schedules

    def _execute_requests(
            self,
            requests: List[grequests.AsyncRequest]
        ) -> List[Response]:

        return grequests.map(
            requests=requests,
            size=self.sm.pool_size,
            exception_handler=self._exec_handler
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
    
    @staticmethod
    def get_flight_key(flight: OneWayFare) -> str:
        return f"{flight.origin}({flight.dep_time}):{flight.destination}({flight.arr_time})"

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
    
    @classmethod
    def _schedules_url(cls, origin: str, destination: str, year: int, month: int) -> str:
        return cls.SERVICES_API_URL + \
            f"timtbl/3/schedules/{origin}/{destination}/years/{year}/months/{month}"
    
    def _availabilty_url(self) -> str:
        return self.BASE_API_URL + \
            f"booking/v4/{self._currency_str}availability"
    

    def __deepcopy__(self, memo):
        cls = self.__class__
        id_self = id(self)
        _copy = memo.get(id_self)
        if _copy is None:
            _copy = cls.__new__(cls)
            memo[id_self] = _copy
            for k, v in self.__dict__.items():
                if k == 'sm':
                    setattr(_copy, k, self.sm)  # Ensure the same session manager is used
                else:
                    setattr(_copy, k, deepcopy(v, memo))
        return _copy