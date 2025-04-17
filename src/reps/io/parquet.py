import polars as pl
from pathlib import Path
from datetime import datetime # noqa: F401  (will be useful later)
from ..domain.models import LabelSegment  # IMURecord not used here → remove

#_TS_FMT = "%Y-%m-%d %H:%M:%S%.f"


def _read_parquet(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)

    if "Timestamp" in df.columns:
        df = df.with_columns(
            pl.col("Timestamp")
            .str.slice(0, 26)  # trim from "2016-02-17T11:25:00.0000000" → "2016-02-17T11:25:00.000000"
            .str.to_datetime(time_zone="US/Eastern")
            .alias("ts")
        ).drop("Timestamp")

    return df



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
