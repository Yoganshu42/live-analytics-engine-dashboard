from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
import pandas as pd


class BaseAnalyticsEngine(ABC):
    def __init__(
        self,
        db: Session,
        job_id: str | None = None,
        source: str | None = None,
    ):
        self.db = db
        self.job_id = job_id
        self.source = source  # 🔑 VERY IMPORTANT

    @abstractmethod
    def load_data(self) -> dict[str, pd.DataFrame]:
        """
        Must return:
        {
            "sales": DataFrame,
            "claims": DataFrame
        }
        """
        ...

    @abstractmethod
    def compute(self) -> dict:
        """
        Must return JSON-serializable analytics
        """
        ...