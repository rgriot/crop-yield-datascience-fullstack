from dataclasses import dataclass
import pandas as pd


@dataclass
class Extractor:
    def extract(self) -> pd.DataFrame: ...
