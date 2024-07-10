import grequests

from itertools import cycle
from typing import List, Dict
from requests import Response

_POOL_SIZE_ = 5
_TIMEOUT_ = 60

class SessionManager:
    BASE_SITE_FOR_SESSION_URL = "https://www.ryanair.com/ie/en"
    SESSION_HEADERS = {
        "User-Agent": 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) " +
            "Chrome/126.0.0.0 Safari/537.36"
    }

    def __init__(self, rid: str, ridsig: str):
        self._session = grequests.Session()
        self._rid = rid
        self._ridsig = ridsig
        self._session.headers = self.SESSION_HEADERS
        self._update_session_cookies()

        self._create_proxies_pool()
        self.session.hooks = {"response": self._hook}
        self.pool_size = _POOL_SIZE_
        self.timeout = _TIMEOUT_

    def _update_session_cookies(self):
        self.session.get(
            self.BASE_SITE_FOR_SESSION_URL
        )

        self.session.cookies.set(
            "rid", self._rid, domain=".ryanair.com"
        )

        self.session.cookies.set(
            "rid.sig", self._ridsig, domain=".ryanair.com"
        )

    @property
    def session(self):
        return self._session
    
    @property
    def pool_size(self) -> int:
        return self._pool_size
    
    @pool_size.setter
    def pool_size(self, size: int) -> None:
        self._pool_size = size

    @property
    def timeout(self) -> int:
        return self._timeout
    
    @timeout.setter
    def timeout(self, timeout: int) -> None:
        self._timeout = timeout

    def _create_proxies_pool(self) -> None:
        self._proxies_pool = [{}]
        self._proxies_loop = cycle(self._proxies_pool)

    def extend_proxies_pool(self, proxies: List[Dict[str, str]]):
        if self._proxies_pool[0] == {}:
            self._proxies_pool.remove({})
            self.session.proxies = proxies[0]

        self._proxies_pool.extend(proxies)

    def set_next_proxy(self) -> None:
        self.session.proxies = next(self._proxies_loop)

    @staticmethod
    def _hook(response: Response, *args, **kwargs) -> None:
        response.raise_for_status()