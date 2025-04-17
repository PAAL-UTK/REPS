import polars as pl


def align_acc_gyro(acc: pl.DataFrame, gyro: pl.DataFrame) -> pl.DataFrame:
    df = acc.join(gyro, on="ts", how="full").sort("ts")

    ts_start, ts_end = df["ts"][0], df["ts"][-1]
    timeline = pl.DataFrame(
        {"ts": pl.datetime_range(ts_start, ts_end, "10ms", eager=True)}
    )
    df = timeline.join(df, on="ts", how="left")

    # linear‑interpolate acc columns, series‑wise (avoids the stub mismatch)
    interp_cols = ["ax", "ay", "az"]
    df = df.with_columns([pl.col(c).interpolate() for c in interp_cols])

    # forward‑fill remaining gaps (gyro, any residual)
    df = df.fill_null(strategy="forward")

    return df
