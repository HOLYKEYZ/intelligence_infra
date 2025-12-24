# Semantic Token Pipeline

Real-time sensor data processing pipeline that generates semantic tokens based on validation rules.

## Stack

- **Dagster**: Pipeline orchestration
- **Pydantic**: Data validation
- **FastMCP**: Tool server for agent integration

## Setup

```bash
pip install -r requirements.txt
```

## Structure

```
data/
  realtime_data.json      # Sensor input
  rules.json              # Validation rules
  semantic_tokens.json    # Generated output
pipeline/
  sensor_pipeline.py      # Core ETL logic
agent_mcp.py             # MCP tool server
```

## Run

### Pipeline Only

```bash
dagster dev -f pipeline/sensor_pipeline.py
```

### MCP Agent

```bash
python agent_mcp.py
```

## How It Works

1. Ingest sensor data (temperature readings)
2. Load validation rules (range constraints)
3. Generate semantic tokens with confidence scores
4. Write structured output for downstream systems

## Output Example

```json
{
  "entity": "sensor_reading",
  "tokens": [
    {
      "type": "RANGE",
      "field": "temperature_celsius",
      "value": [41.7, 43.7],
      "confidence": 0.91
    }
  ],
  "timestamp": "2025-01-16T09:01:00Z"
}
```
