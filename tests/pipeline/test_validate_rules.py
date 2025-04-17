import duckdb
import pytest
from reps.pipeline import validate


@pytest.fixture
def tiny_db(tmp_path):
    db = tmp_path / "tiny.duckdb"
    con = duckdb.connect(db)
    con.sql(
        """
        CREATE TABLE imu (
            subject_id INT, session TEXT, ts DOUBLE,
            acc_x DOUBLE, acc_y DOUBLE, acc_z DOUBLE,
            gyroscope_x DOUBLE, gyroscope_y DOUBLE, gyroscope_z DOUBLE
        );
        CREATE TABLE labels (
            subject_id INT, session TEXT, ts DOUBLE, label INT
        );
    """
    )
    con.close()
    return db


def test_null_padding_fail(tiny_db, monkeypatch):
    con = duckdb.connect(tiny_db)
    con.sql("INSERT INTO imu VALUES (1,'structured',0,0,0,0,0,0,0);")
    con.sql("INSERT INTO labels VALUES (1,'structured',0,42);")
    con.close()

    monkeypatch.setattr(validate, "DB", tiny_db)

    # Prevent recursion: store original before patching
    orig_q = validate._q
    monkeypatch.setattr(
        validate, "_q", lambda sql, read_only=True: orig_q(sql, read_only=False)
    )

    errs = validate.run()
    assert any("label_at_start" in e for e in errs)


def test_session_overlap_fail(tiny_db, monkeypatch):
    con = duckdb.connect(tiny_db)
    con.sql(
        """
        INSERT INTO imu VALUES
        (1,'structured',10,0,0,0,0,0,0),
        (1,'unstructured',9,0,0,0,0,0,0)
    """
    )
    con.close()
    monkeypatch.setattr(validate, "DB", tiny_db)
    # Prevent recursion: store original before patching
    orig_q = validate._q
    monkeypatch.setattr(
        validate, "_q", lambda sql, read_only=True: orig_q(sql, read_only=False)
    )
    errs = validate.run()
    assert any("overlap" in e for e in errs)


def test_physical_limit_fail(tiny_db, monkeypatch):
    con = duckdb.connect(tiny_db)
    con.sql("INSERT INTO imu VALUES (1,'structured',0,100,0,0,0,0,0)")  # ~10 g
    con.close()
    monkeypatch.setattr(validate, "DB", tiny_db)
    # Prevent recursion: store original before patching
    orig_q = validate._q
    monkeypatch.setattr(
        validate, "_q", lambda sql, read_only=True: orig_q(sql, read_only=False)
    )
    errs = validate.run()
    assert any("saturation" in e for e in errs)
