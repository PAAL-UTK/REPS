from pathlib import Path
import polars as pl
import duckdb
from reps.pipeline.ingest import ingest_subject
from reps.config import settings

def test_ingest_roundtrip(tmp_path: Path, monkeypatch):
    # --- redirect config paths into tmp workspace --------------------------------
    raw_root = tmp_path / "raw"
    (raw_root / "acc").mkdir(parents=True)
    (raw_root / "gyro").mkdir()
    (raw_root / "exercise_labels" / "structured").mkdir(parents=True)

    db_path = tmp_path / "reps.duckdb"

    # monkeypatch Settings instance (use raising=False because BaseSettings blocks setattr)
    monkeypatch.setattr(settings, "RAW_ROOT", raw_root, raising=False)
    monkeypatch.setattr(settings, "DWH_PATH", db_path,  raising=False)

    # --- fabricate minimal sensor + label Parquets ------------------------------
    ts = "2020-01-01 00:00:00.000000"
    pl.DataFrame({
        "Accelerometer_X": [1.0], "Accelerometer_Y": [2.0], "Accelerometer_Z": [3.0],
        "Timestamp": [ts],
    }).write_parquet(raw_root / "acc" / "REPS-000_acc.parquet")

    pl.DataFrame({
        "Gyroscope_X": [4.0], "Gyroscope_Y": [5.0], "Gyroscope_Z": [6.0],
        "Timestamp": [ts],
    }).write_parquet(raw_root / "gyro" / "REPS-000_gyro.parquet")

    pl.DataFrame({
        "Exercise": [1], "Timestamp": [ts],
    }).write_parquet(raw_root / "exercise_labels" / "structured" / "REPS-000_labels.parquet")

    # --- run ingest & assert DB contents ----------------------------------------
    ingest_subject("000")

    conn = duckdb.connect(db_path, read_only=True)
    result = conn.execute("SELECT ax, gx FROM imu_structured").fetchone()
    assert result is not None
    ax, gx = result
    assert ax == 1.0 and gx == 4.0

