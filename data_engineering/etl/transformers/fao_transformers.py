from dataclasses import dataclass
import pandas as pd
from slugify import slugify
import logging

from .abstract import Transformer

logger = logging.getLogger(__name__)


@dataclass
class DropColumnTransformer(Transformer):
    columns_to_drop: list

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            f"transformer {self.__class__.__name__} : drop columns {self.columns_to_drop}"
        )
        return data.drop(self.columns_to_drop, axis=1)


@dataclass
class RenameColumnNameTransformer(Transformer):
    column_names_mapping: dict

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        logger.info(
            f"transformer {self.__class__.__name__} : rename columns {self.column_names_mapping}"
        )
        return data.rename(columns=self.column_names_mapping)


@dataclass
class StringValueTransformer(Transformer):
    columns_to_formate: list[str]

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        for column in self.columns_to_formate:
            data[column] = data[column].apply(lambda x: slugify(x))
        logger.info(
            f"transformer {self.__class__.__name__} : slugify columns {self.columns_to_formate}"
        )
        return data


@dataclass
class ElementToColumnTransformer(Transformer):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        pivot_data = data.pivot(
            index=[
                "country_name",
                "country_iso3_id",
                "crop_name",
                "year",
                "value_source",
            ],
            columns="element",
            values="value",
        ).reset_index()
        pivot_data.columns.name = None
        pivot_data = pivot_data.rename(
            columns={
                "area-harvested": "area_harvested",
                "yield": "yield_kg_per_ha",  # Tu peux adapter ce nom à ton besoin
            }
        )
        logger.info(f"transformer {self.__class__.__name__} : pivot dataframe")
        return pivot_data
