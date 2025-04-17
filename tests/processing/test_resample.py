import polars as pl
from datetime import datetime, timezone, timedelta
from reps.processing.resample import align_acc_gyro


def _ts(idx: int):  # helper
    return datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=idx * 10)


def test_align_lengths():
    # 90 Hz acc (every 11.1 ms ≈ round to 10/20/30…)
    acc = pl.DataFrame(
        {
            "ts": [_ts(i) for i in range(0, 10, 1)],
            "ax": range(10),
            "ay": range(10),
            "az": range(10),
        }
    )
    # 100 Hz gyro (10 ms)
    gyro = pl.DataFrame(
        {
            "ts": [_ts(i) for i in range(0, 10)],
            "gx": range(10),
            "gy": range(10),
            "gz": range(10),
        }
    )
    df = align_acc_gyro(acc, gyro)
    # Expect 10 rows (100 Hz window)
    assert df.height == 10
