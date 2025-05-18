from database.models import CountryBoudaries

from data_engineering.etl.extractors.api_extractors import NASAPowerAPIExtractor
from data_engineering.etl.loaders.database_loaders import SQLAlchemyLoader
from data_engineering.etl.loaders.csv_loaders import CSVLoader
from data_engineering.etl.transformers.fao_transformers import (
    RenameColumnNameTransformer,
)
from data_engineering.etl.transformers.nasa_power import (
    ReplaceWithNoneTransformer,
    ConvertDateTransformer,
)

from ..transformers.abstract import TransformerPipeline
from .abstract import NASAPowerETL

nasa_power_extractor = NASAPowerAPIExtractor(
    buffer=100,
    end_year=2025,
    parameters=[
        "T2M",
        "T2MDEW",
        "T2M_MIN",
        "T2M_MAX",
        "RH2M",
        "WS2M",
        "PS",
        "CLOUD_AMT",
        "PRECTOTCORR",
        "GWETPROF",
        "QV2M",
        "GWETROOT",
        "GWETTOP",
        "FROST_DAYS",
    ],
)

rename_column_transformer = RenameColumnNameTransformer(
    column_names_mapping={
        "T2M": "average_temperature",
        "T2MDEW": "frost_point",
        "T2M_MIN": "min_temperature",
        "T2M_MAX": "max_temperature",
        "RH2M": "relative_humidity",
        "WS2M": "wind_speed",
        "PS": "surface_pressure",
        "CLOUD_AMT": "cloud_amount",
        "PRECTOTCORR": "corrected_precipitation",
        "GWETPROF": "profile_soil_moisture",
        "QV2M": "specific_humidity",
        "GWETROOT": "root_soil_wetness",
        "GWETTOP": "surface_soil_wetness",
        "FROST_DAYS": "frost_days",
    }
)

replace_with_none_transformer = ReplaceWithNoneTransformer(none_value=-999)
date_transformer = ConvertDateTransformer(date_column="date")

nasa_power_transformer = TransformerPipeline(
    transformers=[
        rename_column_transformer,
        replace_with_none_transformer,
        date_transformer,
    ]
)

nasa_power_csv_loader = CSVLoader(file_path="data\processed\\nasa_power_data.csv")

nasa_power_csv_etl = NASAPowerETL(
    extractor=nasa_power_extractor,
    transformer=nasa_power_transformer,
    loader=nasa_power_csv_loader,
)
