from dataclasses import dataclass
import pandas as pd
import logging

from .abstract import Loader

logger = logging.getLogger(__name__)


@dataclass
class CSVLoader(Loader):
    file_path: str

    def load(self, data: pd.DataFrame):
        data.to_csv(self.file_path, index=False)
        logger.info(
            f"loader {self.__class__.__name__} : load data to CSV file at {self.file_path}"
        )
