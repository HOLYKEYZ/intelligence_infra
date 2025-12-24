from mcp.server.fastmcp import FastMCP
from pipeline.sensor_pipeline import (
    ingest_realtime,
    load_rules,
    generate_tokens,
    write_semantic_tokens
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("sensor_pipeline")

@mcp.tool()
def run_pipeline() -> dict:
    """Execute sensor pipeline, return structured results"""
    try:
        data = ingest_realtime()
        rules = load_rules()
        tokens = generate_tokens(data, rules)
        write_semantic_tokens(tokens)
        
        return {
            "status": "success",
            "tokens_generated": len(tokens.get("tokens", [])),
            "timestamp": tokens.get("timestamp"),
            "details": tokens
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

if __name__ == "__main__":
    mcp.run(transport="stdio")
