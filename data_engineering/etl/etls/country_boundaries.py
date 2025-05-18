from database.models import CountryBoudaries
from ..extractors.shapely_extractors import ShapelyExtractor
from ..transformers.abstract import TransformerPipeline
from data_engineering.etl.transformers.natural_earth_country_boundaries import (
    ColumnSelectorTransformer,
    GeometryTransformer,
)
from data_engineering.etl.transformers.fao_transformers import (
    RenameColumnNameTransformer,
)
from data_engineering.etl.loaders.database_loaders import (
    SQLAlchemyGeometryLoader,
    SQLAlchemyLoader,
)
from data_engineering.etl.loaders.csv_loaders import CSVLoader
from data_engineering.etl.etls.abstract import ETL

country_boundaries_extractor = ShapelyExtractor(
    file_path="data\countries_boundaries\\ne_110m_admin_0_countries.shp"
)

column_selector_transformer = ColumnSelectorTransformer(
    columns_to_keep=["ADMIN", "ISO_A3", "geometry"]
)
rename_column_transformer = RenameColumnNameTransformer(
    column_names_mapping={"ADMIN": "country_name", "ISO_A3": "country_iso3_id"}
)
geometry_transformer = GeometryTransformer(geometry_column="geometry")

country_boundaries_transformer = TransformerPipeline(
    transformers=[
        column_selector_transformer,
        rename_column_transformer,
        geometry_transformer,
    ]
)

country_boundaries_sql_loader = SQLAlchemyLoader(
    model=CountryBoudaries, clear_table=True
)
country_boundaries_csv_loader = CSVLoader(
    file_path="data\processed\country_boundaries.csv"
)

country_boundaries_sql_etl = ETL(
    extractor=country_boundaries_extractor,
    transformer=country_boundaries_transformer,
    loader=country_boundaries_sql_loader,
)

country_boundaries_csv_etl = ETL(
    extractor=country_boundaries_extractor,
    transformer=country_boundaries_transformer,
    loader=country_boundaries_csv_loader,
)
