import asyncio
from asyncio import Task
import logging

import aiohttp
from aiohttp import ClientSession
from http.cookies import SimpleCookie
from typing import Type, Any

from models import Blocks

logger = logging.getLogger('basic')


class Config(object):
    def __init__(self, user: str = '', password: str = '', proxy_url: str = ''):
        """ Container for all the configuration options """

        self.proxies: dict = {'http': proxy_url} if proxy_url else {}
        self.user: str = user
        self.password: str = password
        self.auth_url: str = 'https://passport.moex.com/authenticate'


class MicexAuth(object):
    """ User authentication data and functions. """

    def __init__(self, config: Config):
        self._config: Config = config
        self._cookies: SimpleCookie | None = None
        self._session = ClientSession()

    async def __aenter__(self) -> 'MicexAuth':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._close()

    async def auth(self, session: ClientSession = None):
        """ One attempt to authenticate """

        session = self._session if session is None else session
        async with session.get(
                url=self._config.auth_url,
                auth=aiohttp.BasicAuth(self._config.user, self._config.password),
                proxy=self._config.proxies if self._config.proxies else None) as response:

            self._cookies: SimpleCookie = response.cookies

        self._passport = None
        for value in self._cookies.values():
            if value.key == 'MicexPassportCert':
                self._passport = value
                break
        if self._passport is None:
            print('Cookie not found!')

    def is_authorized(self) -> bool:
        return bool(self._passport)

    async def _close(self) -> None:
        return await self._session.close()

    @property
    def cookies(self) -> SimpleCookie | None:
        return self._cookies


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

        self._session = aiohttp.ClientSession()
        self._session.proxies = config.proxies
        self._session.cookies = auth.cookies
        self._handler = handler(container)

    async def __aenter__(self) -> 'MicexISSClient':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._close()

    async def _close(self) -> None:
        return await self._session.close()

    @staticmethod
    def _merge_data(target: dict, source: dict) -> None:
        """ Merges data from two dictionaries. """

        for blockname in source:
            if isinstance(source[blockname], list):
                target[blockname] = target.get(blockname, []) + source[blockname]

            if isinstance(source[blockname], dict):
                if data := source[blockname].get('data', []):
                    if target.get(blockname):
                        target[blockname]['data'] = target[blockname].get('data', []) + data
                    else:
                        target[blockname] = source[blockname]

    async def get_available_bonds(self, *boardgroups: int, **params) -> bool:
        """Gets a list of available bonds.

        :param boardgroups: Number indicating trading mode.
                            See: https://iss.moex.com/iss/engines/stock/markets/bonds/boardgroups
        :param params: Additional parameters that need to be added to the URL's query string.
        :return: True for success, False otherwise.
        """

        boardgroups = (58,) if not boardgroups else boardgroups
        params['limit'] = 'unlimited'

        data = {}

        async with asyncio.TaskGroup() as tg:
            tasks: list[Task] = [tg.create_task(
                self._get_data(self.BASE_URL.format(method=self.METHODS['bonds'].format(boardgroup=str(boardgroup))),
                               **params
                               )) for boardgroup in boardgroups]

        for task in tasks:
            self._merge_data(data, task.result())

        self._handler.add_data(data)
        return True

    async def get_securities_history(self, engine: str, market: str, board: str, *secids: str, **params) -> bool:
        """Get and parse historical data on all the securities at the given engine, market, board.

        :param engine: Engine. See: https://iss.moex.com/iss/engines/
        :param market: Market. See: https://iss.moex.com/iss/engines/stock/markets/
        :param board: Trading mode identifier. See: https://iss.moex.com/iss/engines/stock/markets/bonds/boards
        :param secids: Financial instrument identifier.
        :param params: Additional parameters that need to be added to the URL's query string.
        :return: True for success, False otherwise.
        """

        data: dict = {}

        if secids:
            async with asyncio.TaskGroup() as tg:
                tasks: list[Task] = [tg.create_task(
                    self._get_data(self.BASE_URL.format(method=self.METHODS['sec_history'].format(engine=engine,
                                                                                                  market=market,
                                                                                                  board=board,
                                                                                                  security=secid,
                                                                                                  )),
                                   **params
                                   )) for secid in secids]

            for task in tasks:
                self._merge_data(data, task.result())
        else:
            logger.debug(f'[get_securities_history]. Отсутствуют SECID')

        self._handler.add_data(data)
        return True

    async def get_bonds_bondization(self, *secids: str, **params) -> bool:
        """Retrieves detailed data on bonds.

        :param secids: Financial instrument identifier.
        :param params: Additional parameters that need to be added to the URL's query string.
        :return: True for success, False otherwise.
        """

        data: dict = {}

        if secids:
            async with asyncio.TaskGroup() as tg:
                tasks: list[Task] = [tg.create_task(
                    self._get_data(self.BASE_URL.format(method=self.METHODS['sec_bondization'].format(secid=secid)),
                                   **params
                                   )) for secid in secids]

            for task in tasks:
                self._merge_data(data, task.result())
        else:
            logger.debug(f'[get_bonds_bondization]. Отсутствуют SECID')

        self._handler.add_data(data)
        return True

    async def _get_data(self, url: str, **params) -> dict[str:list]:

        def flatten_response(blocks: dict, blockname: str) -> list[dict]:
            """ Converts data received from the API into a form suitable for transmission to pandas. """

            columns: list | None = blocks[blockname].get('columns', None)
            data: list | None = blocks[blockname].get('data', None)

            if columns and data:
                security_data = []

                for item in blocks[blockname]['data']:
                    item_data = {column: item[index] for index, column in enumerate(blocks[blockname]['columns'])}
                    security_data.append(item_data)

                return security_data

            return []

        limit: str | None = params.get('limit', None)
        data: dict[str:list] = {}

        if limit == 'unlimited':
            response: dict[str:Any] = await self._send_request(url, **params)
            for blockname in response.keys():
                block_data: list[dict] = flatten_response(response, blockname)
                data[blockname] = block_data
        else:
            start: int = 0
            count: int = 1

            while count > 0:
                params['start'] = str(start)

                response: dict[str:Any] = await self._send_request(url, **params)
                for blockname in response.keys():
                    block_data: list[dict] = flatten_response(response, blockname)
                    data[blockname] = data.get(blockname, []) + block_data

                    count: int = len(block_data)
                start += count

        return data

    async def _send_request(self, url: str, **params) -> dict[str:Any]:
        """ Sends a request to the API. """

        try:
            async with self._session.get(url=url, params={**params}) as response:
                logger.debug(f'{response.url}')
                return Blocks(**await response.json(encoding='utf-8')).model_dump(exclude_none=True)
        except aiohttp.ContentTypeError:
            return {}

    @property
    def handler(self):
        return self._handler


if __name__ == '__main__':
    pass
