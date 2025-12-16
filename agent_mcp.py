from mcp.server.fastmcp import FastMCP
from pipelines.sensor_pipeline import (
    ingest_realtime,
    load_rules,
    generate_tokens,
    write_semantic_tokens
)

mcp = FastMCP("autonomous-agent")

@mcp.tool()
def run_pipeline():
    data = ingest_realtime()
    rules = load_rules()
    tokens = generate_tokens(data, rules)
    write_semantic_tokens(tokens)
    return "Pipeline executed safely"
