
import duckdb
import sys
import os
import json

# Add backend to path
sys.path.append(r"c:\Users\venka\Desktop\AntiGravity\CohortAnalysis\CohortAnalysis\backend")

from app.domains.analytics.flow_service import get_l1_flows

# Use a specific user db
DB_PATH = r"c:\Users\venka\Desktop\AntiGravity\CohortAnalysis\CohortAnalysis\backend\data\users\user_84176fc0.duckdb"

def test_value_drift():
    conn = duckdb.connect(DB_PATH)
    
    # Identify a common event
    start_event = "ma_minusone_mainfeature_triggered"
    
    print(f"Testing value drift for start_event: {start_event}")
    
    # Run 1: No property
    print("Run 1: No property...")
    res1 = get_l1_flows(conn, start_event, "forward", depth=1)
    
    # Run 2: Property with empty string (All)
    print("Run 2: Property = '' (All)...")
    res2 = get_l1_flows(conn, start_event, "forward", depth=1, property_column="mobile_unit_id", property_values=[""])
    
    # Run 3: Property with multiple empty-ish values
    print("Run 3: Property = [' ', None]...")
    res3 = get_l1_flows(conn, start_event, "forward", depth=1, property_column="mobile_unit_id", property_values=[" ", None])

    s1 = json.dumps(res1, indent=2, sort_keys=True)
    s2 = json.dumps(res2, indent=2, sort_keys=True)
    s3 = json.dumps(res3, indent=2, sort_keys=True)
    
    print(f"\nRes 1 matches Res 2: {s1 == s2}")
    print(f"Res 1 matches Res 3: {s1 == s3}")
    
    if s1 != s2:
        print("\nDIFF 1 vs 2:")
        import difflib
        for line in difflib.unified_diff(s1.splitlines(), s2.splitlines()):
            print(line)
            
    conn.close()

if __name__ == "__main__":
    test_value_drift()
