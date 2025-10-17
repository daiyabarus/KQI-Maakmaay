from typing import Optional
from ..entities.cell_identifier import CellIdentifier


def extract_cell_id(cell_str: str) -> Optional[CellIdentifier]:
    """Extract a CellIdentifier from a string like '310-260-12345-1' or '0x...' hex.

    This is a small, permissive extractor used by higher-level services.
    """
    if not cell_str:
        return None
    parts = cell_str.split('-')
    try:
        if len(parts) == 4:
            mcc, mnc, enodeb, cell_id = map(int, parts)
            return CellIdentifier(mcc=mcc, mnc=mnc, enodeb=enodeb, cell_id=cell_id)
        # fallback: try comma or slash
        for sep in [',', '/']:
            if sep in cell_str:
                parts = cell_str.split(sep)
                if len(parts) >= 4:
                    mcc, mnc, enodeb, cell_id = map(int, parts[:4])
                    return CellIdentifier(mcc=mcc, mnc=mnc, enodeb=enodeb, cell_id=cell_id)
    except ValueError:
        return None
    return None

