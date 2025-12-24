from pydantic import BaseModel, Field, ValidationError
from typing import Tuple, Dict, Any, List
import json
import logging
from dagster import asset
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from config import REALTIME_DATA_PATH, RULES_PATH, SEMANTIC_TOKENS_PATH, LOG_LEVEL

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RangeToken(BaseModel):
    type: str = "RANGE"
    field: str
    value: Tuple[float, float]
    confidence: float = Field(ge=0.0, le=1.0)

class SensorData(BaseModel):
    source: str
    timestamp: str
    payload: Dict[str, Any]

class Rule(BaseModel):
    rule_id: str
    type: str
    field: str
    min: float
    max: float
    confidence: float
    status: str

@asset
def ingest_realtime() -> Dict[str, Any]:
    try:
        if not REALTIME_DATA_PATH.exists():
            logger.warning(f"File not found: {REALTIME_DATA_PATH}, using defaults")
            return {
                "source": "default",
                "timestamp": datetime.utcnow().isoformat(),
                "payload": {}
            }
        
        with open(REALTIME_DATA_PATH) as f:
            data = json.load(f)
            validated = SensorData(**data)
            logger.info(f"Ingested from {validated.source}")
            return data
            
    except ValidationError as e:
        logger.error(f"Invalid sensor data: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        raise
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise

@asset
def load_rules() -> List[Dict[str, Any]]:
    try:
        if not RULES_PATH.exists():
            logger.warning(f"File not found: {RULES_PATH}, using empty rules")
            return []
        
        with open(RULES_PATH) as f:
            data = json.load(f)
            rules = data.get("rules", [])
            
            validated = []
            for rule in rules:
                try:
                    Rule(**rule)
                    validated.append(rule)
                except ValidationError as e:
                    logger.warning(f"Invalid rule {rule.get('rule_id')}: {e}")
            
            logger.info(f"Loaded {len(validated)} valid rules")
            return validated
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}")
        raise
    except Exception as e:
        logger.error(f"Rule loading failed: {e}")
        raise

@asset
def generate_tokens(ingest_realtime: Dict[str, Any], load_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        tokens = []
        payload = ingest_realtime.get("payload", {})
        
        if not payload:
            logger.warning("Empty payload")
            return {
                "entity": "sensor_reading",
                "tokens": [],
                "timestamp": datetime.utcnow().isoformat(),
                "status": "no_data"
            }
        
        stable_rules = [r for r in load_rules if r.get("status") == "stable"]
        logger.info(f"Processing {len(stable_rules)} stable rules")
        
        for rule in stable_rules:
            field = rule.get("field")
            
            if field not in payload:
                continue
            
            value = payload[field]
            if not isinstance(value, (int, float)):
                logger.warning(f"Non-numeric {field}: {value}")
                continue
            
            if rule["min"] <= value <= rule["max"]:
                token = RangeToken(
                    field=f"{field}_{payload.get('unit', 'unknown')}",
                    value=(value - 1, value + 1),
                    confidence=rule["confidence"]
                )
                tokens.append(token.dict())
                logger.info(f"Token: {field}={value}")
        
        return {
            "entity": "sensor_reading",
            "tokens": tokens,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Token generation failed: {e}")
        raise

@asset
def write_semantic_tokens(generate_tokens: Dict[str, Any]) -> None:
    try:
        SEMANTIC_TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        with open(SEMANTIC_TOKENS_PATH, "w") as f:
            json.dump(generate_tokens, f, indent=2)
        
        token_count = len(generate_tokens.get('tokens', []))
        logger.info(f"Wrote {token_count} tokens to {SEMANTIC_TOKENS_PATH}")
        
    except Exception as e:
        logger.error(f"Write failed: {e}")
        raise
