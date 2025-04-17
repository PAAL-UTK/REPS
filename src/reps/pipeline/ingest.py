from __future__ import annotations
from dataclasses import asdict
from pathlib import Path
import duckdb
import polars as pl
from ..config import settings
from ..io.parquet import load_acc, load_gyro, load_labels
from ..processing.resample import align_acc_gyro

_STRUCTURED = "structured"
_UNSTRUCTURED = "unstructured"

_DDL = """
CREATE TABLE IF NOT EXISTS {tbl} (
    id           VARCHAR,
    ts           TIMESTAMP,
    ax REAL, ay REAL, az REAL,
    gx REAL, gy REAL, gz REAL
);
CREATE TABLE IF NOT EXISTS {lbl} (
    id           VARCHAR,
    ts_start     TIMESTAMP,
    ts_end       TIMESTAMP,
    exercise_id  INTEGER
);
"""


def _open_db() -> duckdb.DuckDBPyConnection:
    db = duckdb.connect(settings.DWH_PATH, read_only=False)
    db.execute(_DDL.format(tbl="imu_structured", lbl="labels_structured"))
    db.execute(_DDL.format(tbl="imu_unstructured", lbl="labels_unstructured"))
    return db


def _session_kind(label_path: Path) -> str:
    return _STRUCTURED if "structured" in label_path.parts else _UNSTRUCTURED


def ingest_subject(subject_id: str, /) -> None:
    root = settings.RAW_ROOT
    acc_p = root / "acc" / f"REPS-{subject_id}_acc.parquet"
    gyro_p = root / "gyro" / f"REPS-{subject_id}_gyro.parquet"

    # Pick first existing label folder to infer kind
    lbl_struct = (
        root / "exercise_labels" / "structured" / f"REPS-{subject_id}_labels.parquet"
    )
    lbl_unstruct = (
        root / "exercise_labels" / "unstructured" / f"REPS-{subject_id}_labels.parquet"
    )
    label_p = lbl_struct if lbl_struct.exists() else lbl_unstruct

    session = _session_kind(label_p)  # "structured" | "unstructured"
    imu_tbl = f"imu_{session}"
    lbl_tbl = f"labels_{session}"

    print(f"[{subject_id}] ingesting {session}")

    acc = load_acc(acc_p)
    gyro = load_gyro(gyro_p)
    df = (
        align_acc_gyro(acc, gyro)
        .with_columns(pl.lit(subject_id).alias("id"))
        .select(
            ["ts", "ax", "ay", "az", "gx", "gy", "gz", "id"]
        )  # ← keep only expected cols
    )

    labels = load_labels(label_p)
    lbl_df = pl.from_dicts([asdict(seg) | {"id": subject_id} for seg in labels])

    with _open_db() as db:
        db.append(imu_tbl, df.to_pandas(), by_name=True)
        db.append(lbl_tbl, lbl_df.to_pandas(), by_name=True)
