from pathlib import Path
from reps.io.parquet import load_acc


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
