from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class KQIModel:
    timestamp: str
    cell: str
    metrics: Dict[str, Any]

