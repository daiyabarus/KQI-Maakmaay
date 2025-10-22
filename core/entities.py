from dataclasses import dataclass
from typing import Dict
import pandas as pd

INDONESIA_OPERATORS = {
    "51010": "Telkomsel",
    "51001": "Telkomsel", 
    "51021": "Indosat",
    "51011": "XL",
    "51009": "SF",
    "51028": "SF",
    "51089": "Tri",
    "51088": "Tri",
    "51008": "Axis",
    "51027": "Bolt",
}

@dataclass(frozen=True)
class NetworkIdentifier:
    """Value Object untuk Network ID yang sudah dikonversi"""
    mcc: str
    mnc: str
    plmn: str
    enodeb_id: int
    cell_id: int
    operator: str

    @classmethod
    def from_cgisai(cls, cgisai: str) -> "NetworkIdentifier":
        """Convert CGISAI string ke NetworkIdentifier"""
        cgisai = str(cgisai).zfill(12)
        plmn = cgisai[:5]
        mcc = plmn[:3]
        mnc = plmn[3:5]

        enodeb_hex = cgisai[5:10]
        enodeb_id = int(enodeb_hex, 16)

        cell_hex = cgisai[10:12]
        cell_id = int(cell_hex, 16)

        operator = INDONESIA_OPERATORS.get(plmn, "Unknown")

        return cls(
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            enodeb_id=enodeb_id,
            cell_id=cell_id,
            operator=operator,
        )


@dataclass(frozen=True)
class TowerMapping:
    """Entity untuk mapping tower"""
    tower_id: str
    swe_l5: str
    enodeb_id: int


@dataclass
class KQIRecord:
    """Entity untuk record KQI"""
    timestamp: str
    cgisai: str
    operator: str
    raw_data: Dict
    network_id: NetworkIdentifier = None
    tower_mapping: TowerMapping = None


@dataclass
class ProcessingResult:
    """Result container untuk processing output"""
    mapped_data: 'pd.DataFrame'
    unmapped_data: 'pd.DataFrame'
    mapped_file_path: str = ""
    unmapped_file_path: str = ""