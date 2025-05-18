from dataclasses import dataclass
import pandas as pd
from slugify import slugify
import logging

from .abstract import Transformer

logger = logging.getLogger(__name__)


@dataclass
class ReplaceWithNoneTransformer(Transformer):
    none_value: int | float | str

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        cleaned_data = data.replace(self.none_value, None)
        return cleaned_data


@dataclass
class ConvertDateTransformer(Transformer):
    date_column: str

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self.date_column] = data[self.date_column].apply(
            lambda x: x[:4] + "-" + x[4:]
        )
        return data
