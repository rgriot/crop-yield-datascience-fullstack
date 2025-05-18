from dataclasses import dataclass
from dataclasses import field
import pandas as pd
import logging

from .abstract import Extractor

logger = logging.getLogger(__name__)


@dataclass
class CSVExtractor(Extractor):
    file_path: str
    import_options: dict = field(default_factory=lambda: {})

    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.file_path, **self.import_options)
        logger.info(
            f"extractor {self.__class__.__name__} : extract CSV file from {self.file_path}"
        )
        return df
