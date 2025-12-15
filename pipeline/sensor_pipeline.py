from pydantic import BaseModel, Field
from typing import Tuple

class RangeToken(BaseModel):
    type:str="RANGE"
    field:str
    value:Tuple[float,float]
    confidence:float=Field(ge=0.0,le=1.0)

import json
from dagster import asset
