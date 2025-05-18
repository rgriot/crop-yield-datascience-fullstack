from dataclasses import dataclass
import pandas as pd


@dataclass
class Transformer:
    def transform(self, data: pd.DataFrame) -> pd.DataFrame: ...


@dataclass
class TransformerPipeline:
    transformers: list[Transformer]

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        for transformer in self.transformers:
            data = transformer.transform(data)

        return data
