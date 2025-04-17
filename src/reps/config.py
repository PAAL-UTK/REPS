from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    RAW_ROOT: Path = Path("data/raw")
    DWH_PATH: Path = Path("dwh/reps.duckdb")

settings = Settings()  # import‑time singleton

