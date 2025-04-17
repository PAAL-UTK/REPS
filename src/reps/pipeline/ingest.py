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
    id VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL,
    ax REAL, ay REAL, az REAL,
    gx REAL, gy REAL, gz REAL
);
CREATE TABLE IF NOT EXISTS {lbl} (
    id VARCHAR NOT NULL,
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    exercise_id INTEGER
);
"""


def _open_db() -> duckdb.DuckDBPyConnection:
    db_path = settings.DWH_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)  # ← add this
    db = duckdb.connect(db_path, read_only=False)
    db.execute(_DDL.format(tbl="imu_structured", lbl="labels_structured"))
    db.execute(_DDL.format(tbl="imu_unstructured", lbl="labels_unstructured"))
    return db


def _session_kind(label_path: Path) -> str:
    return _STRUCTURED if "structured" in label_path.parts else _UNSTRUCTURED


def ingest_subject(subject_id: str, /) -> None:
    root = settings.RAW_ROOT
    acc_p = root / "acc" / f"REPS-{subject_id}_acc.parquet"
    gyro_p = root / "gyro" / f"REPS-{subject_id}_gyro.parquet"

    acc = load_acc(acc_p)
    gyro = load_gyro(gyro_p)
    df = (
        align_acc_gyro(acc, gyro)
        .with_columns(pl.lit(subject_id).alias("id"))
        .select(["ts", "ax", "ay", "az", "gx", "gy", "gz", "id"])
    )

    for label_p, session in [
        (
            root
            / "exercise_labels"
            / "structured"
            / f"REPS-{subject_id}_labels.parquet",
            "structured",
        ),
        (
            root
            / "exercise_labels"
            / "unstructured"
            / f"REPS-{subject_id}_labels.parquet",
            "unstructured",
        ),
    ]:
        if not label_p.exists():
            continue

        print(f"[{subject_id}] ingesting {session}")

        labels = load_labels(label_p)
        start = min(seg.ts_start for seg in labels)
        end = max(seg.ts_end for seg in labels)
        df_clip = df.filter((pl.col("ts") >= start) & (pl.col("ts") <= end))
        lbl_df = pl.from_dicts([asdict(seg) | {"id": subject_id} for seg in labels])
        imu_tbl = f"imu_{session}"
        lbl_tbl = f"labels_{session}"

        db_path = settings.DWH_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(db_path, read_only=False) as db:
            db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {imu_tbl} (
                    id VARCHAR NOT NULL,
                    ts TIMESTAMP NOT NULL,
                    ax FLOAT, ay FLOAT, az FLOAT,
                    gx FLOAT, gy FLOAT, gz FLOAT
                );
            """
            )
            db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {lbl_tbl} (
                    id VARCHAR NOT NULL,
                    ts_start TIMESTAMP NOT NULL,
                    ts_end TIMESTAMP NOT NULL,
                    exercise_id INTEGER
                );
            """
            )
            db.execute(f"CREATE INDEX IF NOT EXISTS idx_{imu_tbl}_ts ON {imu_tbl}(ts);")
            db.execute(f"CREATE INDEX IF NOT EXISTS idx_{imu_tbl}_id ON {imu_tbl}(id);")
            db.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{lbl_tbl}_start ON {lbl_tbl}(ts_start);"
            )
            db.execute(f"CREATE INDEX IF NOT EXISTS idx_{lbl_tbl}_id ON {lbl_tbl}(id);")

            db.append(imu_tbl, df_clip.to_pandas(), by_name=True)
            db.append(lbl_tbl, lbl_df.to_pandas(), by_name=True)
