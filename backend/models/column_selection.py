from pydantic import BaseModel
from typing import List

class ColumnSelectionRequest(BaseModel):
    job_id: str | None = None
    columns: List[str]


