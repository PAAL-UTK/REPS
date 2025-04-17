from pathlib import Path
from reps.io.parquet import load_acc
from reps.io.parquet import _read_parquet
import polars as pl


def test_loaders_roundtrip(tmp_path: Path):
    # create tiny dummy DataFrame → parquet → loader → shape equals
    import polars as pl

    df = pl.DataFrame(
        {
            "Accelerometer_X": [0.1],
            "Accelerometer_Y": [0.2],
            "Accelerometer_Z": [0.3],
            "Timestamp": ["2016-02-17 00:00:00.000000"],
        }
    )
    fn = tmp_path / "acc.parquet"
    df.write_parquet(fn)
    out = load_acc(fn)
    assert out.shape == (1, 4)


def test_read_parquet_parses_timestamp(sample_parquet_path):
    df = _read_parquet(sample_parquet_path)

    # Check column presence
    assert "ts" in df.columns
    assert "Timestamp" not in df.columns  # should be renamed

    # Check type and content
    assert df["ts"].dtype == pl.Datetime("us", "US/Eastern")
    assert df["ts"][0].year == 2016
    assert df["ts"][0].hour == 11
