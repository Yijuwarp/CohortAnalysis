import pytest
import duckdb
from datetime import datetime
from app.domains.analytics.usage_service import get_usage
from app.domains.analytics.retention_service import get_retention, build_active_cohort_base
from app.domains.analytics.impact_service import run_impact_analysis

def setup_alignment_data(conn: duckdb.DuckDBPyConnection):
    # Setup tables
    conn.execute("CREATE TABLE cohorts (cohort_id INTEGER, name VARCHAR, is_active BOOLEAN, hidden BOOLEAN, split_type VARCHAR, split_value VARCHAR, split_property VARCHAR, split_parent_cohort_id INTEGER)")
    conn.execute("CREATE TABLE cohort_membership (cohort_id INTEGER, user_id VARCHAR, join_time TIMESTAMP)")
    conn.execute("CREATE TABLE events_scoped (user_id VARCHAR, event_name VARCHAR, event_time TIMESTAMP, event_count INTEGER, revenue DOUBLE)")
    
    # Cohort 1: Baseline (5 active, 5 dead)
    conn.execute("INSERT INTO cohorts VALUES (1, 'Baseline', true, false, null, null, null, null)")
    
    T = datetime(2024, 1, 1, 10, 0, 0)
    for i in range(5):
        uid = f'active_{i}'
        conn.execute("INSERT INTO cohort_membership VALUES (1, ?, ?)", [uid, T])
        # Each active user has 10 events on D0, D1, D2
        for day in range(3):
            E = datetime(2024, 1, 1 + day, 12, 0, 0)
            conn.execute("INSERT INTO events_scoped VALUES (?, 'click', ?, 10, 0)", [uid, E])
            
    for i in range(5):
        uid = f'dead_{i}'
        conn.execute("INSERT INTO cohort_membership VALUES (1, ?, ?)", [uid, T])

def test_inclusive_denominator_sync(db_connection):
    setup_alignment_data(db_connection)
    
    # 1. Retention/Usage Base Size
    _, cohort_sizes = build_active_cohort_base(db_connection)
    assert cohort_sizes[1] == 10, f"Usage/Retention size should be 10 (inclusive of dead users), got {cohort_sizes[1]}"
    
    # 2. Impact Size
    impact_res = run_impact_analysis(
        db_connection, 
        baseline_cohort_id=1, 
        variant_cohort_ids=[], 
        start_day=0, 
        end_day=7,
        exposure_events=[],
        interaction_events=[{"event_name": "click"}],
        impact_events=[]
    )
    # Impact response is a dict. Structure: cohorts: [{'id': 1, 'name': 'Baseline', 'size': 10}]
    impact_cohort = impact_res['cohorts'][0]
    assert impact_cohort['size'] == 10, f"Impact size should be 10, got {impact_cohort['size']}"

def test_usage_cumulative_matches_impact_engagement(db_connection):
    setup_alignment_data(db_connection)
    
    # Usage cumulative volume D0-D2
    usage_res = get_usage(db_connection, 'click', 2, 'any')
    usage_row = usage_res['usage_volume_table'][0]
    # Summing manually D0, D1, D2
    # Each active user (5) had 10 events per day -> 50 events/day.
    # Total for 3 days = 150.
    total_usage_events = sum(usage_row['values'].values())
    assert total_usage_events == 150, f"Expected 150 total usage events, got {total_usage_events}"
    
    # Usage per installed user (cumulative at D2)
    # 150 / 10 = 15 events/user
    usage_per_user = total_usage_events / usage_row['size']
    assert usage_per_user == 15.0
    
    # Impact engagement (D0-D2)
    # Window: start_day=0, end_day=2 (inclusive, so 3 days)
    impact_res = run_impact_analysis(
        db_connection, 
        baseline_cohort_id=1, 
        variant_cohort_ids=[], 
        start_day=0, 
        end_day=2,
        exposure_events=[],
        interaction_events=[{"event_name": "click"}],
        impact_events=[]
    )
    
    # Engagement is interaction_counts / total_users
    # Structure: metrics: [{'metric': 'Engagement (Total)', 'values': {'1': {'value': 15.0}}}]
    engagement_metric = next(m for m in impact_res['metrics'] if m['metric'] == "Engagement (Total)")
    impact_engagement = engagement_metric['values']['1']['value']
    
    assert impact_engagement == 15.0, f"Impact Engagement (15.0) should match Usage Per User (15.0). Got {impact_engagement}"
