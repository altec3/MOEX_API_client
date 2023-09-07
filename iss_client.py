import logging
from typing import Type

import requests
from requests.cookies import RequestsCookieJar

logger = logging.getLogger('basic')

# TODO: Перейти на asyncio


class Config(object):
    def __init__(self, user: str = '', password: str = '', proxy_url: str = ''):
        """ Container for all the configuration options """

        self.proxies: dict = {'http': proxy_url} if proxy_url else {}
        self.user: str = user
        self.password: str = password
        self.auth_url: str = 'https://passport.moex.com/authenticate'


class MicexAuth(object):
    """ User authentication data and functions """

    def __init__(self, config: Config):
        self._config: Config = config
        self._cookie_jar: RequestsCookieJar | None = None
        self._auth()

    def _auth(self):
        """ One attempt to authenticate """

        response: requests.Response = requests.get(
            url=self._config.auth_url,
            auth=(self._config.user, self._config.password),
            proxies=self._config.proxies if self._config.proxies else None
        )
        self._cookie_jar = response.cookies

        self._passport = None
        for cookie in self._cookie_jar:
            if cookie.name == 'MicexPassportCert':
                self._passport = cookie
                break
        if self._passport is None:
            print('Cookie not found!')

    def is_real_time(self):
        """ Repeat auth request if failed last time or cookie expired """

        if not self._passport or (self._passport and self._passport.is_expired()):
            self._auth()
        if self._passport and not self._passport.is_expired():
            return True

        return False

    @property
    def cookie_jar(self) -> RequestsCookieJar | None:
        return self._cookie_jar


class MicexISSDataHandler(object):
    """ Data handler which will be called. """

    def __init__(self, container):
        self._container = container()

    def add_data(self, *args):
        """ This handler method should be overridden to perform
        the processing of data returned by the server.
        """
        pass

    @property
    def container(self):
        return self._container


class MicexISSClient(object):
    """ Methods for interacting with the MICEX ISS server. """

    BASE_URL = 'https://iss.moex.com/iss/{method}.json'
    METHODS = {
        'sec_history': 'history/engines/{engine}/markets/{market}/boards/{board}/securities/{security}',
        'sec_bondization': 'securities/{secid}/bondization',
        'bonds': 'engines/stock/markets/bonds/boardgroups/{boardgroup}/securities'
    }

    def __init__(self, config: Config, auth: MicexAuth, handler: Type[MicexISSDataHandler], container):
        """ Create connection with authorization cookie. """

        self._session = requests.Session()
        self._session.proxies = config.proxies
        self._session.cookies = auth.cookie_jar
        self._handler = handler(container)

    def get_securities_history(self, engine: str, market: str, board: str, *secids: str, **params) -> bool:
        """ Get and parse historical data on all the securities at the given engine, market, board. """

        data = {}
        if secids:
            for secid in secids:
                url = self.BASE_URL.format(method=self.METHODS['sec_history'].format(engine=engine,
                                                                                     market=market,
                                                                                     board=board,
                                                                                     security=secid,
                                                                                     ))
                secid_data: dict[str:list] = self._get_data(url, **params)
                for blockname in secid_data:
                    data[blockname] = data.get(blockname, []) + secid_data[blockname]
        else:
            logger.debug(f'[get_securities_history]. Отсутствуют SECID')

        self._handler.add_data(data)
        return True

    def get_bonds_bondization(self, *secids: str, **params) -> bool:
        """ Получает данные по облигациям. """

        data = {}
        if secids:
            for secid in secids:
                url: str = self.BASE_URL.format(method=self.METHODS['sec_bondization'].format(secid=secid))
                secid_data: dict[str:list] = self._get_data(url, **params)
                for blockname in secid_data:
                    data[blockname] = data.get(blockname, []) + secid_data[blockname]
        else:
            logger.debug(f'[get_bonds_bondization]. Отсутствуют SECID')

        self._handler.add_data(data)
        return True

    def get_available_bonds(self, *boardgroups: int, **params) -> bool:
        """ Получает список доступных облигаций. """

        boardgroups = (58,) if not boardgroups else boardgroups
        params['limit'] = 'unlimited'

        data = {}
        for boardgroup in boardgroups:
            url: str = self.BASE_URL.format(method=self.METHODS['bonds'].format(boardgroup=str(boardgroup)))
            boardgroup_data: dict[str:list] = self._get_data(url, **params)
            for blockname in boardgroup_data:
                data[blockname] = data.get(blockname, []) + boardgroup_data[blockname]

        self._handler.add_data(data)
        return True

    def _get_data(self, url: str, **params) -> dict[str:list]:

        def flatten(iss_data: dict, blockname: str) -> list[dict]:
            """ Преобразует данные, полученные от API, в вид пригодный для передачи в pandas"""

            columns: list | None = iss_data[blockname].get('columns', None)
            data: list | None = iss_data[blockname].get('data', None)

            if columns and data:
                security_data = []

                for item in iss_data[blockname]['data']:
                    item_data = {column: item[index] for index, column in enumerate(iss_data[blockname]['columns'])}
                    security_data.append(item_data)

                return security_data

            return []

        limit: str | None = params.get('limit', None)
        data: dict[str:list] = {}

        if limit == 'unlimited':
            response: dict = self._send_request(url, **params)
            for blockname in response.keys():
                block_data: list[dict] = flatten(response, blockname)
                data[blockname] = block_data
        else:
            start: int = 0
            count: int = 1
            while count > 0:
                params['start'] = str(start)

                response: dict = self._send_request(url, **params)
                for blockname in response.keys():
                    block_data: list[dict] = flatten(response, blockname)
                    data[blockname] = data.get(blockname, []) + block_data
                    count: int = len(block_data)

                start += count

        return data

    def _send_request(self, url: str, **params) -> dict:
        """ Отправляет запрос к API. """

        response: requests.Response = self._session.get(url=url,
                                                        params={**params},
                                                        timeout=5,
                                                        )
        logger.debug(f'{response.url}')

        try:
            response: dict = response.json()
            return response
        except requests.JSONDecodeError:
            return {}

    @property
    def handler(self):
        return self._handler


if __name__ == '__main__':
    pass
