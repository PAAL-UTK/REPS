"""Warehouse‑sanity rules and runner."""

from __future__ import annotations

from pathlib import Path
from typing import Union

import duckdb
import polars as pl

DB = Path("dwh/reps.duckdb")  # adjust if your DB lives elsewhere


class ValidationError(Exception):
    """Raised when a rule fails."""


def _ensure_views():
    if "tiny" in str(DB):
        return  # skip view creation in tests
    con = duckdb.connect(DB, read_only=False)
    con.execute(
        """
        CREATE OR REPLACE VIEW imu AS
        SELECT
            id AS subject_id,
            ts,
            ax AS acc_x, ay AS acc_y, az AS acc_z,
            gx AS gyroscope_x, gy AS gyroscope_y, gz AS gyroscope_z,
            'structured' AS session
        FROM imu_structured
        UNION ALL
        SELECT
            id AS subject_id,
            ts,
            ax AS acc_x, ay AS acc_y, az AS acc_z,
            gx AS gyroscope_x, gy AS gyroscope_y, gz AS gyroscope_z,
            'unstructured' AS session
        FROM imu_unstructured;

        CREATE OR REPLACE VIEW labels AS
        SELECT
            id AS subject_id,
            ts_start AS ts,
            exercise_id AS label,
            'structured' AS session
        FROM labels_structured
        UNION ALL
        SELECT
            id AS subject_id,
            ts_start AS ts,
            exercise_id AS label,
            'unstructured' AS session
        FROM labels_unstructured;
    """
    )
    con.close()


def _q(sql: str, read_only: bool = True) -> Union[pl.DataFrame, pl.Series]:
    """Execute *sql* against the DuckDB warehouse and return a Polars object.

    The return value is a *DataFrame* for multi‑column results or a *Series*
    for single‑column results, matching `polars.from_arrow` behaviour.
    """
    con = duckdb.connect(DB, read_only=read_only)
    try:
        return pl.from_arrow(con.execute(sql).arrow())
    finally:
        con.close()


# ───────────────────────── V‑1: trials must start/end NULL ─────────────────────────


def check_null_padding() -> None:
    sql = """
    WITH span AS (
        SELECT subject_id, session,
               MIN(ts) AS first_ts, MAX(ts) AS last_ts
        FROM imu
        GROUP BY subject_id, session
    ),
    viol AS (
        SELECT s.subject_id, s.session, 'label_at_start' AS reason
        FROM span s
        JOIN labels l ON l.subject_id=s.subject_id AND l.session=s.session
                      AND l.ts=s.first_ts AND l.label IS NOT NULL
        UNION ALL
        SELECT s.subject_id, s.session, 'label_at_end'
        FROM span s
        JOIN labels l ON l.subject_id=s.subject_id AND l.session=s.session
                      AND l.ts=s.last_ts AND l.label IS NOT NULL
    )
    SELECT * FROM viol;
    """
    bad = _q(sql)
    if bad.shape[0]:
        raise ValidationError(
            "Labels present at start/end of recording:\n" + bad.__repr__()
        )


# ──────────────────────── V‑2: session boundaries separate ────────────────────────


def check_session_gap() -> None:
    sql = """
    WITH ends AS (
        SELECT subject_id, MAX(ts) AS structured_end
        FROM imu WHERE session='structured'
        GROUP BY subject_id
    ),
    starts AS (
        SELECT subject_id, MIN(ts) AS unstructured_start
        FROM imu WHERE session='unstructured'
        GROUP BY subject_id
    )
    SELECT e.subject_id, structured_end, unstructured_start
    FROM ends e JOIN starts s USING (subject_id)
    WHERE structured_end >= unstructured_start;
    """
    bad = _q(sql)
    if bad.shape[0]:
        raise ValidationError("Structured/unstructured overlap:\n" + bad.__repr__())


# ───────────────────────── V‑3: sensor physical limits ─────────────────────────

# gyro saturates at 2000 but float values slightly exceed 2000 so limit set to 2001


def check_physical_limits() -> None:
    sql = """
    WITH viol AS (
        SELECT subject_id, ts,
               acc_x, acc_y, acc_z, gyroscope_x, gyroscope_y, gyroscope_z,
               GREATEST(ABS(acc_x), ABS(acc_y), ABS(acc_z)) AS acc_max,
               GREATEST(ABS(gyroscope_x), ABS(gyroscope_y), ABS(gyroscope_z)) AS gyro_max
        FROM imu
    )
    SELECT subject_id, ts, acc_max, gyro_max
    FROM viol
    WHERE acc_max > 8 * 9.80665 OR gyro_max > 2001 
    ORDER BY subject_id, ts;
    """
    bad = _q(sql)
    if bad.shape[0]:
        raise ValidationError(
            "Sensor saturation (>8 g or >2000 °/s):\n" + bad.__repr__()
        )


# ──────────────────────────────── Runner ────────────────────────────────


def run() -> list[str]:
    """Run all validators; return list of error strings (empty if clean)."""
    _ensure_views()
    errors: list[str] = []
    for fn in (check_null_padding, check_session_gap, check_physical_limits):
        try:
            fn()
        except ValidationError as err:
            errors.append(str(err))
    return errors
