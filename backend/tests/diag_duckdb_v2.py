
import duckdb

conn = duckdb.connect(':memory:')
print("Day boundaries:")
print(f"1s cross: {conn.execute("SELECT DATE_DIFF('day', '2023-01-01 23:59:59'::TIMESTAMP, '2023-01-02 00:00:01'::TIMESTAMP)").fetchone()[0]}")
print(f"47.9h:    {conn.execute("SELECT DATE_DIFF('day', '2023-01-01 00:00:01'::TIMESTAMP, '2023-01-02 23:59:59'::TIMESTAMP)").fetchone()[0]}")
print(f"Exact 24h: {conn.execute("SELECT DATE_DIFF('day', '2023-01-01 00:00:00'::TIMESTAMP, '2023-01-02 00:00:00'::TIMESTAMP)").fetchone()[0]}")

print("\nHour boundaries:")
print(f"1s cross: {conn.execute("SELECT DATE_DIFF('hour', '2023-01-01 22:59:59'::TIMESTAMP, '2023-01-01 23:00:01'::TIMESTAMP)").fetchone()[0]}")
print(f"59m:      {conn.execute("SELECT DATE_DIFF('hour', '2023-01-01 22:00:01'::TIMESTAMP, '2023-01-01 22:59:59'::TIMESTAMP)").fetchone()[0]}")
