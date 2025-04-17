# tests/io/conftest.py
import pytest
import polars as pl
from pathlib import Path


@pytest.fixture
def sample_parquet_path(tmp_path: Path) -> Path:
    df = pl.DataFrame(
        {
            "Timestamp": ["2016-02-17 11:25:00.000", "2016-02-17 11:25:01.000"],
            "Exercise": [1.0, 1.0],
        }
    )
    path = tmp_path / "sample.parquet"
    df.write_parquet(path)
    return path
