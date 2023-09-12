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
