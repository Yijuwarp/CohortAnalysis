import sys
import os
import duckdb

# Add backend to path to import recompute logic
sys.path.append(os.path.abspath('backend'))

from app.domains.revenue.revenue_recompute import recompute_modified_revenue_columns

def setup_test_db():
    conn = duckdb.connect(':memory:')
    
    # Create events_normalized table
    conn.execute("""
        CREATE TABLE events_normalized (
            user_id TEXT,
            event_name TEXT,
            event_time TIMESTAMP,
            event_count DOUBLE,
            original_revenue DOUBLE,
            modified_revenue DOUBLE
        )
    """)
    
    # Create revenue_event_selection table
    conn.execute("""
        CREATE TABLE revenue_event_selection (
            event_name VARCHAR PRIMARY KEY,
            is_included BOOLEAN NOT NULL DEFAULT FALSE,
            override_revenue DOUBLE
        )
    """)
    
    # Insert test data
    # Event 1: Included, no override -> should use original_revenue (5.0)
    # Event 2: Included, with override -> should use override * count (10.0 * 2 = 20.0)
    # Event 3: Not included, no override -> should use 0.0
    # Event 4: Not included, with override -> SHOULD BE 0.0, but currently uses override (THE BUG)
    
    conn.execute("""
        INSERT INTO events_normalized VALUES 
        ('u1', 'event_included', '2024-01-01 10:00:00', 1, 5.0, 0.0),
        ('u2', 'event_included_override', '2024-01-01 10:00:00', 2, 5.0, 0.0),
        ('u3', 'event_not_included', '2024-01-01 10:00:00', 1, 5.0, 0.0),
        ('u4', 'event_not_included_override', '2024-01-01 10:00:00', 3, 5.0, 0.0)
    """)
    
    conn.execute("""
        INSERT INTO revenue_event_selection VALUES 
        ('event_included', TRUE, NULL),
        ('event_included_override', TRUE, 10.0),
        ('event_not_included', FALSE, NULL),
        ('event_not_included_override', FALSE, 20.0)
    """)
    
    return conn

def run_test():
    conn = setup_test_db()
    
    print("Running recompute_modified_revenue_columns...")
    try:
        recompute_modified_revenue_columns(conn, "events_normalized")
    except Exception as e:
        print(f"Error during recompute: {e}")
        sys.exit(1)
    
    results_rows = conn.execute("SELECT event_name, modified_revenue FROM events_normalized").fetchall()
    results = {name: rev for name, rev in results_rows}
    
    print("\nResults:")
    for name, rev in sorted(results.items()):
        print(f"  {name}: {rev}")
    
    expected = {
        'event_included': 5.0,
        'event_included_override': 20.0,
        'event_not_included': 0.0,
        'event_not_included_override': 0.0
    }
    
    failed = False
    for name in expected:
        actual = results.get(name)
        if actual != expected[name]:
            print(f"FAILED: {name} expected {expected[name]}, got {actual}")
            failed = True
        else:
            print(f"PASSED: {name} got {actual}")
            
    if failed:
        print("\nTest failed! (Confirmed Red phase)")
        sys.exit(1)
    else:
        print("\nTest passed! (Green phase reached)")
        sys.exit(0)

if __name__ == "__main__":
    run_test()
