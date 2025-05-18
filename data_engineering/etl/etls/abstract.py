from dataclasses import dataclass
import pandas as pd

from ..extractors.abstract import Extractor
from ..transformers.abstract import TransformerPipeline
from ..loaders.abstract import Loader

from database.models import CountryBoudaries


@dataclass
class ETL:
    extractor: Extractor
    transformer: TransformerPipeline
    loader: Loader

    def extract(self):
        return self.extractor.extract()

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.transformer.transform(data)

    def load(self, data: pd.DataFrame):
        self.loader.load(data)

    def process(self):
        data = self.extract()
        cleaned_data = self.transform(data)
        self.load(cleaned_data)


@dataclass
class NASAPowerETL(ETL):
    country: CountryBoudaries | None = None

    def extract(self):
        return self.extractor.extract(self.country)
