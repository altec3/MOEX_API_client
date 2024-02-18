from pydantic import BaseModel, field_validator
from typing import Optional


class MyBaseModel(BaseModel):
    """
    Базовая модель, от которой мы будем наследовать наши классы.
    Предоставляет базовые параметры конфигурации
    """

    class Config:
        validate_assignment = True


class BlockItems(MyBaseModel):
    columns: list[str]
    data: list[Optional[list]]

    @field_validator('columns')
    def upper_columns_name(cls, titles: list[str]) -> list[str]:
        return list(map(lambda title: title.upper(), titles))


class Blocks(MyBaseModel):
    marketdata: BlockItems | None = None
    marketdata_yields: BlockItems | None = None
    securities: BlockItems | None = None
    history: BlockItems | None = None
    coupons: BlockItems | None = None


class Data(MyBaseModel):
    marketdata: list[dict] | None = None
    marketdata_yields: list[dict] | None = None
    securities: list[dict] | None = None
    history: list[dict] | None = None
    coupons: list[dict] | None = None


if __name__ == '__main__':
    from pprint import pprint

    bonds = {'marketdata': {'columns': ['SECID', 'DURATION'],
                            'data': [['AMUNIBB2DER6', 0], ['RU000A0JVNL1', 0]]
                            },
             'securities': {'columns': ['SECID', 'BOARDID'],
                            'data': [['AMUNIBB2DER6', 'TQOD'], ['RU000A0JVNL1', 'TQOD']]
                            }
             }
    bonds_ = {'marketdata': [{'SECID': 'AMUNIBB2DER6', 'DURATION': 0}, {'SECID': 'RU000A0JVNL1', 'DURATION': 0}],
              'securities': [{'SECID': 'AMUNIBB2DER6', 'BOARDID': 'TQOD'},
                             {'SECID': 'RU000A0JVNL1', 'BOARDID': 'TQOD'}]
              }
    history = {'history': {'columns': ['SECID', 'NUMTRADES'],
                           'data': [],
                           'metadata': {'NUMTRADES': {'type': 'double'},
                                        'SECID': {'bytes': 36,
                                                  'max_size': 0,
                                                  'type': 'string'}}}}

    bondization = {'coupons': {'columns': ['coupondate', 'secid'],
                               'data': [['2024-01-17', 'SU26227RMFS7'], ['2024-07-17', 'SU26227RMFS7']]
                               }
                   }
    profit = {'profit': [{'COUPONVALUE': [39.14, 33.66],
                          'FACEVALUE': [1000.0, 1000.0],
                          'MATDATE': ['2024-07-08', '2023-12-12'],
                          'PROFIT': [91.73, 45.02],
                          'SECID': ['RU000A0JQ7Z2', 'RU000A102HB3'],
                          'SECNAME': ['"Российские ЖД" ОАО 19 обл.', 'РОСНАНО АО БО-002P-05']
                          }]
              }

    blocks = Blocks(**bonds)
    pprint(blocks.model_dump(exclude_none=True))
    data = Data(**bonds_)
    pprint(data.model_dump(exclude_none=True))
