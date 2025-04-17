from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import yaml


# ---------- IMU row ----------
@dataclass(slots=True)
class IMURecord:
    ts: datetime  # millisecond resolution, tz‑aware
    ax: float
    ay: float
    az: float
    gx: float
    gy: float
    gz: float


# ---------- label segment ----------
@dataclass(slots=True)
class LabelSegment:
    ts_start: datetime
    ts_end: datetime
    exercise_id: int | None  # None == rest


# ---------- exercise dictionary ----------
_EXERCISE_PATH = Path(__file__).with_suffix(".yaml").parent / "exercise_map.yaml"


def exercise_name(code: int | None) -> str | None:
    if code is None:
        return None
    with _EXERCISE_PATH.open("r") as fh:
        mapping: dict[int, str] = yaml.safe_load(fh)
    return mapping.get(code)
