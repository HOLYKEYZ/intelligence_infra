from pydantic import BaseModel, Field
from typing import Tuple

class RangeToken(BaseModel):
    type:str="RANGE"
    field:str
    value:Tuple[float,float]
    confidence:float=Field(ge=0.0,le=1.0)

import json
from dagster import asset
from datetime import datetime

@asset
def ingest_realtime():
    with open("data/realtime_data.json") as f:
        return json.load(f)
@asset
def load_rules():
    with open("data/rules.json") as f:
        return json.load(f)["rules"]

@asset
def generate_tokens(ingest_realtime, load_rules):
    tokens = []

    payload = ingest_realtime["payload"]

    for rule in load_rules:
        if rule["status"] != "stable":
            continue

        if rule["field"] == "temperature":
            value = payload["temperature"]
            if rule["min"] <= value <= rule["max"]:
                token = RangeToken(
                    field="temperature_celsius",
                    value=(value - 1, value + 1),
                    confidence=rule["confidence"]
                )
                tokens.append(token.dict())

    return {
        "entity": "sensor_reading",
        "tokens": tokens,
        "timestamp": datetime.utcnow().isoformat()
    }
@asset
def write_semantic_tokens(generate_tokens):
    with open("data/semantic_tokens.json", "w") as f:
        json.dump(generate_tokens, f, indent=2)
