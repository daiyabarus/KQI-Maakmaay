from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class ResultDTO:
    metrics: Dict[str, Any]

