from dataclasses import dataclass
import pandas as pd

from data_engineering.etl.loaders.database_loaders import SQLAlchemyLoader
from data_engineering.etl.loaders.csv_loaders import CSVLoader
from data_engineering.etl.transformers.fao_transformers import (
    DropColumnTransformer,
    RenameColumnNameTransformer,
    StringValueTransformer,
    ElementToColumnTransformer,
)
from database.models import FAOCountryCropYield

from ..extractors.csv_extractors import CSVExtractor
from ..transformers.abstract import TransformerPipeline
from .abstract import ETL


drop_columns_transformer = DropColumnTransformer(
    columns_to_drop=[
        "Domain Code",
        "Domain",
        "Item Code (CPC)",
        "Element Code",
        "Year Code",
        "Flag",
        "Note",
    ]
)
rename_columns_transformer = RenameColumnNameTransformer(
    column_names_mapping={
        "Area": "country_name",
        "Area Code (ISO3)": "country_iso3_id",
        "Element": "element",
        "Item": "crop_name",
        "Year": "year",
        "Unit": "unit",
        "Value": "value",
        "Flag Description": "value_source",
    }
)

string_transformer = StringValueTransformer(
    columns_to_formate=["country_name", "element", "crop_name", "value_source"]
)
pivot_transformer = ElementToColumnTransformer()


fao_country_crop_yield_extractor = CSVExtractor(
    file_path="data\FAOSTAT_data_en_5-8-2025.csv"
)

fao_country_crop_yield_transformer = TransformerPipeline(
    transformers=[
        drop_columns_transformer,
        rename_columns_transformer,
        string_transformer,
        pivot_transformer,
    ]
)

fao_country_crop_yield_sql_loader = SQLAlchemyLoader(
    model=FAOCountryCropYield, clear_table=True
)
fao_country_crop_yield_csv_loader = CSVLoader(
    file_path="data\processed\country_crop_yield.csv"
)


fao_country_crop_yield_sql_etl = ETL(
    extractor=fao_country_crop_yield_extractor,
    transformer=fao_country_crop_yield_transformer,
    loader=fao_country_crop_yield_sql_loader,
)

fao_country_crop_yield_csv_etl = ETL(
    extractor=fao_country_crop_yield_extractor,
    transformer=fao_country_crop_yield_transformer,
    loader=fao_country_crop_yield_csv_loader,
)
