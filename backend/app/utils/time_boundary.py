from datetime import datetime, timezone
import duckdb

def get_observation_end_time(connection: duckdb.DuckDBPyConnection) -> datetime | None:
    """
    Returns a robust observation boundary for analytics.
    Uses p99.9 of event_time from events_scoped, clamped to UTC now.
    """
    row = connection.execute("""
        SELECT quantile_cont(event_time, 0.999)
        FROM events_scoped
    """).fetchone()

    if not row or row[0] is None:
        return None

    p99_time = row[0]
    # Ensure p99_time is a datetime
    if not isinstance(p99_time, datetime):
        # DuckDB might return a string if not cast correctly in some environments, 
        # but usually it's a datetime for TIMESTAMP columns.
        try:
            p99_time = datetime.fromisoformat(str(p99_time))
        except Exception:
            return None
    
    now = datetime.now() # Use local naive now to match likely naive DB timestamps
    # Clamp to prevent future leakage
    observation_end_time = min(p99_time, now)

    if p99_time > now:
        print(f"[WARN] Future timestamps detected: p99={p99_time}, clamped to now={now}")

    return observation_end_time
