from dataclasses import dataclass
import pandas as pd


@dataclass
class Loader:
    def load(self, data: pd.DataFrame) -> None: ...
