from dataclasses import dataclass
from dataclasses import field
import pandas as pd
import geopandas as gpd
import logging

from .abstract import Extractor

logger = logging.getLogger(__name__)


@dataclass
class ShapelyExtractor(Extractor):
    file_path: str

    def extract(self) -> gpd.GeoDataFrame:
        gdf = gpd.read_file(self.file_path)
        logger.info(
            f"extractor {self.__class__.__name__} : extract Shapely file from {self.file_path}"
        )
        return gdf
