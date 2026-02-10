# models/data_rows.py

from sqlalchemy import Column, Integer, String, JSON
from db.base import Base


class DataRow(Base):
    __tablename__ = "data_rows"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String, index=True)
    source = Column(String, index=True)        # samsung / reliance / godrej
    dataset_type = Column(String, index=True)  # sales / claims
    data = Column(JSON)                         # raw row data