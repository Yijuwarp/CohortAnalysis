"""
Short summary: centralizes configuration values for backend modules.
"""
from pathlib import Path

DATABASE_PATH = Path(__file__).resolve().parents[2] / "cohort_analysis.duckdb"
