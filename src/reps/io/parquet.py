import polars as pl
from pathlib import Path
from datetime import datetime, timezone  # noqa: F401  (will be useful later)
from ..domain.models import LabelSegment  # IMURecord not used here → remove

_TS_FMT = "%Y-%m-%d %H:%M:%S%.f"


def _read_parquet(path: Path) -> pl.DataFrame:
    return (
        pl.read_parquet(path)
        .with_columns(
            pl.col("Timestamp")
            .str.strptime(pl.Datetime, _TS_FMT)  # naive → datetime
            .dt.replace_time_zone("UTC")  # mark as UTC
            .alias("ts")
        )
        .drop("Timestamp")
    )


def load_acc(path: Path) -> pl.DataFrame:
    # cols: Accelerometer_X/Y/Z  -> ax/ay/az
    return (
        _read_parquet(path)
        .rename(
            {"Accelerometer_X": "ax", "Accelerometer_Y": "ay", "Accelerometer_Z": "az"}
        )
        .select(["ts", "ax", "ay", "az"])
    )


def load_gyro(path: Path) -> pl.DataFrame:
    # cols: Gyroscope_X/Y/Z  -> gx/gy/gz
    cols = {"Gyroscope_X": "gx", "Gyroscope_Y": "gy", "Gyroscope_Z": "gz"}
    return _read_parquet(path).rename(cols).select(["ts", "gx", "gy", "gz"])


def load_labels(path: Path) -> list[LabelSegment]:
    df = _read_parquet(path).rename({"Exercise": "exercise_id"})
    segments: list[LabelSegment] = []
    last_id = df["exercise_id"][0]
    start = df["ts"][0]
    for ts, ex in zip(df["ts"][1:], df["exercise_id"][1:]):
        if ex != last_id:
            segments.append(LabelSegment(start, ts, last_id))
            start, last_id = ts, ex
    # close final segment
    segments.append(LabelSegment(start, df["ts"][-1], last_id))
    return segments
