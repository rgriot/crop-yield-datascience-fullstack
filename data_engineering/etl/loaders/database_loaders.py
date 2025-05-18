from dataclasses import dataclass
import pandas as pd
from database.models import Base
from database.engine import engine
from sqlalchemy.orm import Session
import logging

from .abstract import Loader

logger = logging.getLogger(__name__)


@dataclass
class SQLAlchemyLoader(Loader):
    model: Base
    clear_table: bool = True

    def load(self, data: pd.DataFrame) -> None:
        session = Session(engine)

        if self.clear_table:
            session.query(self.model).delete()

        data_dict = data.to_dict(orient="records")
        rows_to_load = [self.model(**row) for row in data_dict]
        session.bulk_save_objects(rows_to_load)
        session.commit()
        session.close()

        logger.info(
            f"loader {self.__class__.__name__} : data loaded to {self.model} table"
        )
