from dataclasses import dataclass


@dataclass
class CellIDModel:
    mcc: int
    mnc: int
    enodeb: int
    cell_id: int

