import duckdb
import pandas as pd
from datetime import datetime

conn = duckdb.connect(':memory:')

# Mock the setup
conn.execute("CREATE TABLE events_normalized (user_id TEXT, event_name TEXT, event_time TIMESTAMP, p1 TEXT, event_count DOUBLE)")
conn.execute("CREATE TABLE cohorts (cohort_id INTEGER, name TEXT, is_active BOOLEAN, hidden BOOLEAN)")
conn.execute("CREATE TABLE cohort_membership (user_id TEXT, cohort_id INTEGER, join_time TIMESTAMP)")

# Insert test data
# Case: Same timestamp, different events
conn.execute("INSERT INTO events_normalized VALUES ('u1', 'A', '2024-01-01 10:00:00', 'v1', 1.0)")
conn.execute("INSERT INTO events_normalized VALUES ('u1', 'B', '2024-01-01 10:00:00', 'v1', 1.0)")

# Case: Same timestamp, same event, different props
conn.execute("INSERT INTO events_normalized VALUES ('u1', 'C', '2024-01-01 11:00:00', 'v1', 1.0)")
conn.execute("INSERT INTO events_normalized VALUES ('u1', 'C', '2024-01-01 11:00:00', 'v2', 1.0)")

conn.execute("INSERT INTO cohorts VALUES (1, 'TestCohort', True, False)")
conn.execute("INSERT INTO cohort_membership VALUES ('u1', 1, '2024-01-01 00:00:00')")

# Logic from paths_service.py (simplified)
prefix = ""
tie_breaker = "event_name, user_id"

query = f"""
WITH base AS (
  SELECT
    cohort_id, user_id, event_name, event_time,
    ROW_NUMBER() OVER (
      PARTITION BY cohort_id, user_id
      ORDER BY event_time, {tie_breaker}
    ) AS rn
  FROM events_normalized join cohort_membership using (user_id)
),
step_1 AS (
  SELECT cohort_id, user_id, event_time AS t1, rn AS rn1
  FROM base
  WHERE event_name = 'A'
),
step_2_candidates AS (
  SELECT
      s.cohort_id, s.user_id, s.t1,
      b.event_time AS t2,
      b.rn AS rn2
  FROM step_1 s
  JOIN base b ON b.user_id = s.user_id AND b.cohort_id = s.cohort_id
  WHERE b.event_name = 'B'
    AND (b.event_time > s.t1 OR (b.event_time = s.t1 AND b.rn > s.rn1))
),
step_2 AS (
  SELECT * FROM step_2_candidates QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_id, user_id ORDER BY t2, rn2) = 1
),
step_3_candidates AS (
    SELECT
        s.cohort_id, s.user_id, s.t1, s.t2,
        b.event_time AS t3,
        b.rn AS rn3
    FROM step_2 s
    JOIN base b ON b.user_id = s.user_id AND b.cohort_id = s.cohort_id
    WHERE b.event_name = 'C'
      AND (b.event_time > s.t2 OR (b.event_time = s.t2 AND b.rn > s.rn2))
),
step_3 AS (
    SELECT * FROM step_3_candidates QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_id, user_id ORDER BY t3, rn3) = 1
),
step_4_candidates AS (
    SELECT
        s.cohort_id, s.user_id, s.t1, s.t2, s.t3,
        b.event_time AS t4,
        b.rn AS rn4
    FROM step_3 s
    JOIN base b ON b.user_id = s.user_id AND b.cohort_id = s.cohort_id
    WHERE b.event_name = 'C'
      AND (b.event_time > s.t3 OR (b.event_time = s.t3 AND b.rn > s.rn3))
),
step_4 AS (
    SELECT * FROM step_4_candidates QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_id, user_id ORDER BY t4, rn4) = 1
)
SELECT * FROM step_4
"""

print("Running test...")
df = conn.execute(query).df()
print(df)
