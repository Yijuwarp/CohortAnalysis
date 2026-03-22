
import duckdb

conn = duckdb.connect(':memory:')
conn.execute("SELECT DATE_DIFF('day', '2023-01-01 23:59:59'::TIMESTAMP, '2023-01-02 00:00:01'::TIMESTAMP) as d1")
print(f"Diff 1 (cross midnight): {conn.fetchone()[0]}")

conn.execute("SELECT DATE_DIFF('day', '2023-01-01 00:00:00'::TIMESTAMP, '2023-01-02 23:59:59'::TIMESTAMP) as d2")
print(f"Diff 2 (long day): {conn.fetchone()[0]}")

conn.execute("SELECT DATE_DIFF('day', DATE_TRUNC('day', '2023-01-01 23:59:59'::TIMESTAMP), DATE_TRUNC('day', '2023-01-02 00:00:01'::TIMESTAMP)) as d3")
print(f"Diff 3 (truncated cross): {conn.fetchone()[0]}")
