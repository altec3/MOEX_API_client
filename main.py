import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from config.config import PASSWORD, USER, FILTER_CRITERIA
from iss_client import Config
from iss_client import MicexAuth
from iss_client import MicexISSClient
from iss_client import MicexISSDataHandler
from logger import create_logger

logger = logging.getLogger('basic')


class MyData(object):
    """ Container that will be used by the handler to store data. """

    def __init__(self):
        self._data: Any = None

    def print_data(self):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 0)

        result = pd.DataFrame(self._data['profit'][0])
        print(result.sort_values(by=['PROFIT'], ascending=False))

    @property
    def data(self) -> list:
        return self._data

    @data.setter
    def data(self, value: list[str]) -> None:
        self._data = value


class MyDataHandler(MicexISSDataHandler):
    """ This handler will be receiving pieces of data from the ISS client. """

    def add_data(self, market_data: dict[str:list]):
        """ Adds the received data to the storage, pre-filtering. """

        data: dict[str:list] = self._container.data if self._container.data else {}

        for blockname in market_data:
            data[blockname] = data.get(blockname, []) + market_data[blockname]
        self._container.data = self._filter_data(data, **FILTER_CRITERIA)

    @staticmethod
    def _filter_data(data: dict[str:list], **criteries) -> dict[str:list]:
        """ Filters security data according to specified criteria. """

        price_max: float = criteries.get('price_max', 101.0)  #: Максимальная цена, %
        duration_max: int = criteries.get('duration_max', 365)  #: Максимальная дюрация, дней
        trades_min: int = criteries.get('trades_min', 300)  #: Минимальное количество заключенных сделок, шт
        faceunits: list = criteries.get('faceunits', ['SUR'])  #: Валюта номинала

        def get_index_by_secid(secid: str, rows: list[dict]) -> int | None:
            for index, row in enumerate(rows):
                row_secid: str = row.get('SECID', '')
                if row_secid == secid:
                    return index

        def securities_filter(row: dict[str:list]) -> bool:
            if row.get('PREVLEGALCLOSEPRICE'):
                if row['FACEUNIT'] in faceunits and Decimal(float(row['PREVLEGALCLOSEPRICE'])) <= Decimal(price_max):
                    return True
            return False

        securities_rows: list[dict] = data.pop('securities', [])
        marketdata_rows: list[dict] = data.pop('marketdata', [])
        history_rows: list[dict] = data.pop('history', [])

        logger.debug(f'Количество записей до фильтра: {len(securities_rows)}')

        if securities_rows:
            securities_rows = list(filter(securities_filter, securities_rows))

            if marketdata_rows:
                for row in marketdata_rows:
                    secid = row['SECID']
                    if row.get('DURATION'):
                        if 0 < int(row['DURATION']) > duration_max:
                            index = get_index_by_secid(secid, securities_rows)
                            if index:
                                del securities_rows[index]
                    else:
                        index = get_index_by_secid(secid, securities_rows)
                        if index:
                            del securities_rows[index]

            if history_rows:

                aggregated_history = MyDataHandler._aggregate_data(history_rows, 'SECID', 'NUMTRADES')

                for secid, numtrades in aggregated_history.items():
                    for daytrades in numtrades:
                        if daytrades < trades_min:
                            index = get_index_by_secid(secid, securities_rows)
                            if index:
                                del securities_rows[index]

            data['securities'] = securities_rows

        logger.debug(f'Количество записей после фильтра: {len(securities_rows)}')

        return data

    def get_secids_list(self) -> list[str]:
        """ Returns a security SECID. """

        data: dict[str:list] = self._container.data if self._container.data else {}

        securities_rows: list[dict] = [data[blockname] for blockname in data if blockname == 'securities'][0]
        secids_list: list[str] = list(map(lambda row: row.get('SECID', ''), securities_rows))

        logger.debug(f'Вего SECID: {len(secids_list)}')

        return secids_list

    def get_boards_with_secids(self) -> dict[str:list]:
        """ Returns a dictionary of security groups with their SECID. """

        data: dict[str:list] = self._container.data if self._container.data else {}

        securities_rows: list[dict] = [data[blockname] for blockname in data if blockname == 'securities'][0]
        boards = self._aggregate_data(securities_rows, 'BOARDID', 'SECID')

        logger.debug(f'Вего BOARDID: {len(boards)}')

        return boards

    @staticmethod
    def get_start_date(days: int = 14) -> str:
        """ Returns a date that was [days] days ago. """

        ordinal: int = datetime.toordinal(datetime.now()) - days
        return datetime.fromordinal(ordinal).strftime('%Y-%m-%d')

    @staticmethod
    def _aggregate_data(array: list[dict], group_key: str, values_key: str) -> dict[str:list]:
        data: dict = {}
        for item in array:
            data[item[group_key]] = data.get(item.get(group_key), []) + [item.get(values_key)]
        return data

    def calculate_profit(self, *secids: str) -> None:
        """ Calculates the total profit for each bond. """

        data: dict[str:list] = self._container.data if self._container.data else {}
        securities: list[dict] = data.get('securities', [])
        coupons: list[dict] = data.get('coupons', [])

        result: dict[str:list] = {}
        if coupons and securities:
            for secid in secids:
                security: dict = list(filter(lambda row: bool(row.get('SECID') == secid), securities))[0]
                coupons_count: int = len(list(filter(lambda row: bool(row.get('SECID') == secid), coupons)))
                accruedint: Decimal = Decimal(security.get('ACCRUEDINT'))
                couponvalue: Decimal = Decimal(security.get('COUPONVALUE'))
                facevalue: Decimal = Decimal(security.get('FACEVALUE'))
                prevlegalcloseprice: Decimal = Decimal(security.get('PREVLEGALCLOSEPRICE'))
                secname: str = security.get('SECNAME')
                matdate: str = security.get('MATDATE')

                cost: Decimal = facevalue * prevlegalcloseprice * Decimal(0.01) + accruedint
                income: Decimal = facevalue + couponvalue * coupons_count
                profit: Decimal = income - cost

                result['PROFIT'] = result.get('PROFIT', []) + [float(profit.quantize(Decimal('1.00')))]
                result['SECID'] = result.get('SECID', []) + [secid]
                result['SECNAME'] = result.get('SECNAME', []) + [secname]
                result['FACEVALUE'] = result.get('FACEVALUE', []) + [float(facevalue.quantize(Decimal('1.00')))]
                result['COUPONVALUE'] = result.get('COUPONVALUE', []) + [float(couponvalue.quantize(Decimal('1.00')))]
                result['MATDATE'] = result.get('MATDATE', []) + [matdate]

        self.add_data({'profit': [result]})


async def main():
    create_logger()
    my_config = Config(user=USER, password=PASSWORD, proxy_url='')
    async with MicexAuth(my_config) as my_auth:
        await my_auth.auth()

    async with MicexISSClient(config=my_config, auth=my_auth, handler=MyDataHandler, container=MyData) as iss:
        await iss.get_available_bonds(
            7, 58, 193, **{'iss.dp': 'comma',
                           'iss.meta': 'off',
                           'iss.only': 'securities,marketdata',
                           'securities.columns': 'SECID,BOARDID,SECNAME,FACEUNIT,FACEVALUE,'
                                                 'MATDATE,PREVLEGALCLOSEPRICE,ACCRUEDINT,COUPONVALUE',
                           'marketdata.columns': 'SECID,DURATION',
                           })

        from_date: str = iss.handler.get_start_date()
        boards: dict = iss.handler.get_boards_with_secids()

        async with asyncio.TaskGroup() as tg:   #: Группа задач получения истории по ценным бумагам
            for board, secids in boards.items():
                tg.create_task(iss.get_securities_history(
                    'stock',
                    'bonds',
                    board,
                    *secids,
                    **{'iss.only': 'history',
                       'history.columns': 'SECID,NUMTRADES',
                       'from': from_date},
                ))

        secids: list = iss.handler.get_secids_list()
        await iss.get_bonds_bondization(
            *secids, **{'iss.meta': 'off',
                        'iss.only': 'coupons',
                        'coupons.columns': 'coupondate,secid',
                        'lang': 'ru',
                        'from': datetime.now().strftime('%Y-%m-%d'),
                        'limit': 'unlimited',
                        })

        iss.handler.calculate_profit(*secids)
        iss.handler.container.print_data()


if __name__ == '__main__':
    asyncio.run(main())
