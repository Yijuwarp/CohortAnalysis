"""
Short summary: statistical comparison service for cohort A vs B across retention, usage, and monetization.

All tests operate on per-user metric vectors, never on already-aggregated values.
"""
from __future__ import annotations

import math
import duckdb
import numpy as np
from fastapi import HTTPException
from scipy import stats

from app.domains.cohorts.cohort_service import ensure_cohort_tables
from app.queries.retention_queries import fetch_retention_active_rows
from app.queries.usage_queries import build_usage_property_filter_clause

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RETENTION_METRICS = {"retention_rate"}
PROPORTION_METRICS = RETENTION_METRICS | {"unique_users_percent", "unique_users_cumulative_percent"}
MEAN_METRICS = {
    "per_installed_user",
    "cumulative_per_installed_user",
    "per_retained_user",
    "per_event_firer",
    "revenue_per_acquired_user",
    "cumulative_revenue_per_acquired_user",
    "revenue_per_retained_user",
}
ALL_METRICS = PROPORTION_METRICS | MEAN_METRICS
USAGE_METRICS = (
    {"per_installed_user", "cumulative_per_installed_user", "per_retained_user", "per_event_firer"}
    | {"unique_users_percent", "unique_users_cumulative_percent"}
)
MONETIZATION_METRICS = {"revenue_per_acquired_user", "cumulative_revenue_per_acquired_user", "revenue_per_retained_user"}

METRIC_LABELS: dict[str, str] = {
    "retention_rate": "Retention Rate",
    "per_installed_user": "Events per Installed User",
    "cumulative_per_installed_user": "Cumulative Events per Installed User",
    "per_retained_user": "Events per Retained User",
    "per_event_firer": "Events per Event Firer",
    "unique_users_percent": "Unique Users %",
    "unique_users_cumulative_percent": "Cumulative Unique Users %",
    "revenue_per_acquired_user": "Revenue per Acquired User",
    "cumulative_revenue_per_acquired_user": "Cumulative Revenue per Acquired User",
    "revenue_per_retained_user": "Revenue per Retained User",
}


def _scoped_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'events_scoped' AND table_schema = 'main'"
        ).fetchone()[0]
    )


def _get_cohort_size(conn: duckdb.DuckDBPyConnection, cohort_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT cm.user_id)
        FROM cohort_membership cm
        JOIN events_scoped es ON cm.user_id = es.user_id
        WHERE cm.cohort_id = ?
        """,
        [cohort_id],
    ).fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Statistical tests (pure Python, no external deps)
# ---------------------------------------------------------------------------

def _normal_cdf(z: float) -> float:
    """Abramowitz & Stegun approximation for standard normal CDF."""
    t = 1.0 / (1.0 + 0.2316419 * abs(z))
    poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    p = 1.0 - (1.0 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * z * z) * poly
    return p if z >= 0 else 1.0 - p


def _two_proportion_z_test(s_a: int, n_a: int, s_b: int, n_b: int) -> float:
    """Two-sample proportion z-test, two-tailed. Returns p-value."""
    if n_a == 0 or n_b == 0:
        return 1.0
    p_a = s_a / n_a
    p_b = s_b / n_b
    p_pool = (s_a + s_b) / (n_a + n_b)
    denom = math.sqrt(p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b))
    if denom == 0:
        return 1.0
    z = (p_a - p_b) / denom
    return 2 * (1 - _normal_cdf(abs(z)))


def _fisher_exact(s_a: int, n_a: int, s_b: int, n_b: int) -> float:
    """
    Fisher's exact test approximated via hypergeometric distribution pmf.
    P-value = sum of hypergeometric probs <= observed prob (two-tailed).
    """
    f_a = n_a - s_a
    f_b = n_b - s_b
    n = n_a + n_b
    k = s_a + s_b

    if n == 0 or k == 0:
        return 1.0

    def log_comb(n: int, k: int) -> float:
        if k < 0 or k > n:
            return -math.inf
        return sum(math.log(n - i) - math.log(i + 1) for i in range(k))

    log_denom = log_comb(n, k)
    if log_denom == -math.inf:
        return 1.0

    # Compute probability for each possible value of x
    p_obs = math.exp(log_comb(n_a, s_a) + log_comb(n_b, s_b) - log_denom) if s_a <= n_a and s_b <= n_b else 0.0

    p_value = 0.0
    for x in range(max(0, k - n_b), min(k, n_a) + 1):
        y = k - x
        lp = log_comb(n_a, x) + log_comb(n_b, y) - log_denom
        p = math.exp(lp)
        if p <= p_obs + 1e-10:
            p_value += p

    return min(p_value, 1.0)


def _welch_t_test(vec_a: list[float], vec_b: list[float]) -> float:
    """Welch's t-test for unequal variances, two-tailed. Returns p-value."""
    n_a = len(vec_a)
    n_b = len(vec_b)
    if n_a < 2 or n_b < 2:
        return 1.0

    mean_a = sum(vec_a) / n_a
    mean_b = sum(vec_b) / n_b
    var_a = sum((x - mean_a) ** 2 for x in vec_a) / (n_a - 1)
    var_b = sum((x - mean_b) ** 2 for x in vec_b) / (n_b - 1)

    se = math.sqrt(var_a / n_a + var_b / n_b)
    if se == 0:
        return 1.0

    t = (mean_a - mean_b) / se

    # Welch–Satterthwaite degrees of freedom
    num = (var_a / n_a + var_b / n_b) ** 2
    den = (var_a / n_a) ** 2 / (n_a - 1) + (var_b / n_b) ** 2 / (n_b - 1)
    df = num / den if den > 0 else 1.0

    # Approximate t-distribution CDF using normal for large df, or simple integration for small
    # Use normal approximation (good for df > 30) otherwise use a series
    if df >= 30:
        return 2 * (1 - _normal_cdf(abs(t)))

    # Incomplete beta regularized B(df/2, 1/2) via numerical integration (simple)
    x = df / (df + t * t)
    return _incomplete_beta_cdf(x, df / 2, 0.5)


def _incomplete_beta_cdf(x: float, a: float, b: float) -> float:
    """
    Regularized incomplete beta function I_x(a, b) via continued fraction.
    Used for the t-distribution p-value.
    """
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0

    # Use symmetry when x > 0.5 (converges faster)
    if x > (a + 1) / (a + b + 2):
        return 1.0 - _incomplete_beta_cdf(1 - x, b, a)

    lbeta = math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
    front = math.exp(math.log(x) * a + math.log(1 - x) * b - lbeta) / a

    # Lentz continued fraction
    eps = 1e-10
    tiny = 1e-30
    f = tiny
    C = f
    D = 0.0
    for m in range(200):
        for j in (0, 1):
            if m == 0 and j == 0:
                d = 1.0
            elif j == 0:
                d = m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m))
            else:
                d = -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1))

            D = 1.0 + d * D
            if abs(D) < tiny:
                D = tiny
            D = 1.0 / D

            C = 1.0 + d / C
            if abs(C) < tiny:
                C = tiny

            delta = C * D
            f *= delta
            if abs(delta - 1.0) < eps:
                break

    return front * f


def _mann_whitney_u(vec_a: list[float], vec_b: list[float]) -> float:
    """
    Mann-Whitney U test – two-tailed p-value using normal approximation.
    Suitable for large samples.
    """
    n_a = len(vec_a)
    n_b = len(vec_b)
    if n_a == 0 or n_b == 0:
        return 1.0

    # Rank combined
    combined = [(v, 0) for v in vec_a] + [(v, 1) for v in vec_b]
    combined.sort(key=lambda x: x[0])

    n = n_a + n_b
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j < n - 1 and combined[j][0] == combined[j + 1][0]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            ranks[k] = avg_rank
        i = j + 1

    rank_sum_a = sum(ranks[i] for i, (_, grp) in enumerate(combined) if grp == 0)
    U_a = rank_sum_a - n_a * (n_a + 1) / 2.0
    U_b = n_a * n_b - U_a

    U = min(U_a, U_b)

    mu = n_a * n_b / 2.0
    sigma = math.sqrt(n_a * n_b * (n + 1) / 12.0)
    if sigma == 0:
        return 1.0

    z = (U - mu) / sigma
    return 2 * (1 - _normal_cdf(abs(z)))


# ---------------------------------------------------------------------------
# Per-user metric computation
# ---------------------------------------------------------------------------

def _compute_retention_vectors(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    day: int,
    retention_event: str | None = None,
    granularity: str = "day",
    retention_type: str = "classic",
) -> tuple[list[float], int, int]:
    """
    Returns (binary_vector, n_success, n_total) for retention on given day.
    binary_vector[i] = 1 if user retained on day, 0 otherwise.
    """
    unit = "day" if granularity == "day" else "hour"
    
    event_filter = ""
    params: list = [cohort_id, day]
    if retention_event and retention_event != "any":
        event_filter = "AND es.event_name = ?"
        params = [cohort_id, retention_event, day]

    # NOTE:
    # Day = calendar day difference (DATE_TRUNC)
    # Hour = calendar hour bucket (NOT rolling duration)
    # Classic retention MUST match previous implementation exactly.
    # Do not modify query structure without updating regression tests.
    
    if retention_type == "classic":
        query = f"""
        SELECT DISTINCT cm.user_id
        FROM cohort_membership cm
        JOIN events_scoped es
          ON es.user_id = cm.user_id
        WHERE cm.cohort_id = ?
          {event_filter}
          AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) = ?
        """
    else:
        query = f"""
        SELECT DISTINCT cm.user_id
        FROM cohort_membership cm
        WHERE cm.cohort_id = ?
          AND EXISTS (
              SELECT 1 FROM events_scoped es
              WHERE es.user_id = cm.user_id
                {event_filter.replace('AND es.event_name', 'AND es.event_name')}
                AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) >= ?
          )
        """

    retained_users = conn.execute(query, params).fetchall()
    retained_set = {row[0] for row in retained_users}

    all_users = conn.execute(
        "SELECT DISTINCT user_id FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchall()

    vec = [1.0 if row[0] in retained_set else 0.0 for row in all_users]
    return vec, len(retained_set), len(all_users)


def _compute_usage_volume_vectors(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    event: str | None,
    day: int,
    metric: str,
    granularity: str = "day",
    property: str | None = None,
    operator: str = "=",
    value: str | None = None,
) -> list[float]:
    """
    Returns per-user metric vector for volume-based usage metrics.
    """
    unit = "day" if granularity == "day" else "hour"
    cumulative = metric in ("cumulative_per_installed_user",)
    day_condition = "<= ?" if cumulative else "= ?"

    property_clause, property_params = build_usage_property_filter_clause(
        property=property,
        operator=operator,
        value=value,
        table_alias="es",
    )

    rows = conn.execute(
        f"""
        SELECT cm.user_id, COALESCE(SUM(es.event_count), 0) AS event_count
        FROM cohort_membership cm
        LEFT JOIN events_scoped es
          ON es.user_id = cm.user_id
         AND es.event_name = ?
         AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) {day_condition}
         AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) >= 0{property_clause}
        WHERE cm.cohort_id = ?
        GROUP BY cm.user_id
        """,
        [event, day, *property_params, cohort_id],
    ).fetchall()

    cohort_size = conn.execute(
        "SELECT COUNT(DISTINCT user_id) FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchone()[0]

    if metric in ("per_installed_user", "cumulative_per_installed_user"):
        # Each user's event count divided by 1 → vector of per-user counts
        return [float(r[1]) for r in rows]

    if metric == "per_retained_user":
        # Denominator: retained users on day
        retained_rows = conn.execute(
            f"""
            SELECT DISTINCT cm.user_id
            FROM cohort_membership cm
            JOIN events_scoped es
              ON es.user_id = cm.user_id
            WHERE cm.cohort_id = ?
              AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) = ?
            """,
            [cohort_id, day],
        ).fetchall()
        retained_set = {r[0] for r in retained_rows}
        # Only include retained users in denominator context
        return [float(r[1]) for r in rows if r[0] in retained_set]

    if metric == "per_event_firer":
        # Only users who fired the event
        return [float(r[1]) for r in rows if float(r[1]) > 0]

    return [float(r[1]) for r in rows]


def _compute_unique_users_vectors(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    event: str | None,
    day: int,
    metric: str,
    granularity: str = "day",
    property: str | None = None,
    operator: str = "=",
    value: str | None = None,
) -> tuple[list[float], int, int]:
    """
    Returns (binary_vector, n_success, n_total) for unique user proportion metrics.
    """
    unit = "day" if granularity == "day" else "hour"
    cumulative = metric == "unique_users_cumulative_percent"
    day_condition = "<= ?" if cumulative else "= ?"

    property_clause, property_params = build_usage_property_filter_clause(
        property=property,
        operator=operator,
        value=value,
        table_alias="es",
    )

    fired_rows = conn.execute(
        f"""
        SELECT DISTINCT cm.user_id
        FROM cohort_membership cm
        JOIN events_scoped es
          ON es.user_id = cm.user_id
         AND es.event_name = ?
         AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) {day_condition}
         AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) >= 0{property_clause}
        WHERE cm.cohort_id = ?
        """,
        [event, day, *property_params, cohort_id],
    ).fetchall()

    fired_set = {r[0] for r in fired_rows}
    all_users = conn.execute(
        "SELECT DISTINCT user_id FROM cohort_membership WHERE cohort_id = ?",
        [cohort_id],
    ).fetchall()

    vec = [1.0 if r[0] in fired_set else 0.0 for r in all_users]
    return vec, len(fired_set), len(all_users)


def _compute_revenue_vectors(
    conn: duckdb.DuckDBPyConnection,
    cohort_id: int,
    day: int,
    metric: str,
    granularity: str = "day",
) -> list[float]:
    """
    Returns per-user metric vector for monetization metrics.
    """
    unit = "day" if granularity == "day" else "hour"
    cumulative = metric == "cumulative_revenue_per_acquired_user"
    day_condition = "<= ?" if cumulative else "= ?"

    rows = conn.execute(
        f"""
        SELECT cm.user_id, COALESCE(SUM(es.modified_revenue), 0.0) AS revenue
        FROM cohort_membership cm
        LEFT JOIN events_scoped es
          ON es.user_id = cm.user_id
         AND es.event_name IN (SELECT event_name FROM revenue_event_selection WHERE is_included = TRUE)
         AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) {day_condition}
         AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) >= 0
        WHERE cm.cohort_id = ?
        GROUP BY cm.user_id
        """,
        [day, cohort_id],
    ).fetchall()

    if metric in ("revenue_per_acquired_user", "cumulative_revenue_per_acquired_user"):
        return [float(r[1]) for r in rows]

    if metric == "revenue_per_retained_user":
        retained_rows = conn.execute(
            f"""
            SELECT DISTINCT cm.user_id
            FROM cohort_membership cm
            JOIN events_scoped es
              ON es.user_id = cm.user_id
            WHERE cm.cohort_id = ?
              AND DATE_DIFF('{unit}', DATE_TRUNC('{unit}', cm.join_time), DATE_TRUNC('{unit}', es.event_time)) = ?
            """,
            [cohort_id, day],
        ).fetchall()
        retained_set = {r[0] for r in retained_rows}
        return [float(r[1]) for r in rows if r[0] in retained_set]

    return [float(r[1]) for r in rows]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compare_cohorts(
    conn: duckdb.DuckDBPyConnection,
    cohort_a: int,
    cohort_b: int,
    tab: str,
    metric: str,
    day: int,
    event: str | None = None,
    granularity: str = "day",
    retention_type: str = "classic",
    property: str | None = None,
    operator: str = "=",
    value: str | None = None,
) -> dict:
    if metric != "retention_rate" and granularity != "day":
        raise HTTPException(
            status_code=400,
            detail="Hourly granularity is only supported for retention_rate"
        )
    if retention_type not in {"classic", "ever_after"}:
        raise HTTPException(
            status_code=400,
            detail="retention_type must be classic or ever_after"
        )

    if cohort_a == cohort_b:
        raise HTTPException(status_code=400, detail="cohort_a and cohort_b must be different")

    if tab not in {"retention", "usage", "monetization"}:
        raise HTTPException(status_code=400, detail=f"Unknown tab: {tab}")

    if metric not in ALL_METRICS:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric}")

    if tab == "retention" and metric not in RETENTION_METRICS:
        raise HTTPException(status_code=400, detail="Metric not valid for retention tab")
    if tab == "usage" and metric not in USAGE_METRICS:
        raise HTTPException(status_code=400, detail="Metric not valid for usage tab")
    if tab == "monetization" and metric not in MONETIZATION_METRICS:
        raise HTTPException(status_code=400, detail="Metric not valid for monetization tab")

    ensure_cohort_tables(conn)

    if not _scoped_exists(conn):
        raise HTTPException(status_code=400, detail="No events data found")

    # Validate cohorts exist
    for cid, label in [(cohort_a, "cohort_a"), (cohort_b, "cohort_b")]:
        row = conn.execute(
            "SELECT cohort_id FROM cohorts WHERE cohort_id = ? AND is_active = TRUE",
            [cid],
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Cohort {cid} not found or inactive")

    count = conn.execute("SELECT COUNT(*) FROM events_scoped").fetchone()[0]
    print("COMPARE: using events_scoped row count:", count)

    # ------------------------------------------------------------------
    # Compute vectors
    # ------------------------------------------------------------------
    if metric == "retention_rate":
        vec_a, s_a, n_a = _compute_retention_vectors(conn, cohort_a, day, event, granularity, retention_type)
        vec_b, s_b, n_b = _compute_retention_vectors(conn, cohort_b, day, event, granularity, retention_type)
        val_a = s_a / n_a if n_a > 0 else 0.0
        val_b = s_b / n_b if n_b > 0 else 0.0

    elif metric in ("unique_users_percent", "unique_users_cumulative_percent"):
        if not event:
            raise HTTPException(status_code=400, detail="event is required for usage metrics")
        vec_a, s_a, n_a = _compute_unique_users_vectors(conn, cohort_a, event, day, metric, granularity, property, operator, value)
        vec_b, s_b, n_b = _compute_unique_users_vectors(conn, cohort_b, event, day, metric, granularity, property, operator, value)
        val_a = s_a / n_a if n_a > 0 else 0.0
        val_b = s_b / n_b if n_b > 0 else 0.0

    elif metric in {"per_installed_user", "cumulative_per_installed_user", "per_retained_user", "per_event_firer"}:
        if not event:
            raise HTTPException(status_code=400, detail="event is required for usage metrics")
        vec_a = _compute_usage_volume_vectors(conn, cohort_a, event, day, metric, granularity, property, operator, value)
        vec_b = _compute_usage_volume_vectors(conn, cohort_b, event, day, metric, granularity, property, operator, value)
        n_a, n_b = len(vec_a), len(vec_b)
        s_a = s_b = None
        val_a = sum(vec_a) / n_a if n_a > 0 else 0.0
        val_b = sum(vec_b) / n_b if n_b > 0 else 0.0

    elif metric in MONETIZATION_METRICS:
        vec_a = _compute_revenue_vectors(conn, cohort_a, day, metric, granularity)
        vec_b = _compute_revenue_vectors(conn, cohort_b, day, metric, granularity)
        n_a, n_b = len(vec_a), len(vec_b)
        s_a = s_b = None
        val_a = sum(vec_a) / n_a if n_a > 0 else 0.0
        val_b = sum(vec_b) / n_b if n_b > 0 else 0.0

    else:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric}")

    # ------------------------------------------------------------------
    # Run statistical tests
    # ------------------------------------------------------------------
    # Guard against very small samples: statistical tests are unreliable
    # and some libraries emit warnings or errors. In these cases, we
    # return a well-formed response without any tests.
    sample_size_a = len(vec_a)
    sample_size_b = len(vec_b)

    if sample_size_a < 2 or sample_size_b < 2:
        tests: list[dict] = []
        p_value: float | None = None
    else:
        if metric in PROPORTION_METRICS:
            p_z = _two_proportion_z_test(s_a, n_a, s_b, n_b)

            # Fisher's exact test becomes extremely slow for large cohorts.
            # For large sample sizes rely on the z-test only.
            if (n_a + n_b) > 5000:
                tests = [
                    {"name": "two_proportion_z_test", "p_value": round(float(p_z), 6)},
                ]
                p_value = float(p_z)
            else:
                p_fish = _fisher_exact(s_a, n_a, s_b, n_b)
                tests = [
                    {"name": "two_proportion_z_test", "p_value": round(float(p_z), 6)},
                    {"name": "fisher_exact", "p_value": round(float(p_fish), 6)},
                ]
                p_value = float(min(p_z, p_fish))
        else:
            # Continuous metrics: Welch t-test and Mann-Whitney U via scipy.stats
            var_a = np.var(vec_a)
            var_b = np.var(vec_b)

            if var_a == 0 and var_b == 0:
                p_value = None
                tests = [
                    {"name": "mann_whitney_u", "p_value": None},
                    {"name": "welch_t_test", "p_value": None},
                ]
            else:
                t_res = stats.ttest_ind(vec_a, vec_b, equal_var=False, alternative="two-sided")
                mw_res = stats.mannwhitneyu(vec_a, vec_b, alternative="two-sided", method="asymptotic")

                p_t = float(t_res.pvalue) if not np.isnan(t_res.pvalue) else None
                p_mw = float(mw_res.pvalue) if not np.isnan(mw_res.pvalue) else None

                tests = [
                    {"name": "mann_whitney_u", "p_value": round(p_mw, 6) if p_mw is not None else None},
                    {"name": "welch_t_test", "p_value": round(p_t, 6) if p_t is not None else None},
                ]
                p_value = p_mw

    # ------------------------------------------------------------------
    # Derive summary statistics
    # ------------------------------------------------------------------
    difference = val_a - val_b
    # Guard against division by zero – when the control value is zero,
    # a relative lift is not well-defined so we return None.
    relative_lift = (difference / val_b) if val_b != 0 else None
    significant = bool(p_value is not None and p_value < 0.05)

    label = METRIC_LABELS.get(metric, metric)
    if metric == "retention_rate" and retention_type == "ever_after":
        label = "Ever-After Retention %"

    if granularity == "hour":
        label_prefix = "Hour"
        day_ref = f" (D{day // 24})" if day % 24 == 0 and day > 0 else ""
        metric_label = f"{label_prefix} {day}{day_ref} {label}"
    else:
        label_prefix = "Day"
        metric_label = f"{label_prefix} {day} {label}"

    return {
        "metric_label": metric_label,
        "cohort_a_value": round(float(val_a), 6),
        "cohort_b_value": round(float(val_b), 6),
        "difference": round(float(difference), 6),
        "relative_lift": round(float(relative_lift), 6) if relative_lift is not None else None,
        "p_value": round(float(p_value), 6) if p_value is not None else None,
        "significant": significant,
        "tests": tests,
    }
