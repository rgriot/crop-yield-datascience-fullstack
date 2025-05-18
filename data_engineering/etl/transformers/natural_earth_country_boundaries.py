from dataclasses import dataclass
import pandas as pd
from geoalchemy2.shape import from_shape
import logging

from .abstract import Transformer

logger = logging.getLogger(__name__)


@dataclass
class ColumnSelectorTransformer(Transformer):
    columns_to_keep: list[str]

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            f"transformer {self.__class__.__name__} : columns selected {self.columns_to_keep}"
        )
        return data[self.columns_to_keep]


@dataclass
class GeometryTransformer(Transformer):
    geometry_column: str

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self.geometry_column] = data[self.geometry_column].apply(
            lambda x: from_shape(x, srid=4326)
        )
        logger.info(
            f"transformer {self.__class__.__name__} : column {self.geometry_column} transformed"
        )
        return data
