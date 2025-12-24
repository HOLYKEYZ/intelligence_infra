import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

REALTIME_DATA_PATH = DATA_DIR / "realtime_data.json"
RULES_PATH = DATA_DIR / "rules.json"
SEMANTIC_TOKENS_PATH = DATA_DIR / "semantic_tokens.json"

LLM_CONFIG = {
    "model": "gpt-4.1-mini",
    "temperature": 0.2,
    "max_tokens": 512
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
```

**.env.example** (NEW FILE)
```
DATA_DIR=data
LOG_LEVEL=INFO
