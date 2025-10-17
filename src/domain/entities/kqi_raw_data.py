from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class KQIRawData:
    """Raw KQI data record as read from source CSV/GZ files.

    Fields are intentionally permissive; parsing/validation happens elsewhere.
    """

    timestamp: str
    cell: str
    metrics: Dict[str, Any]

