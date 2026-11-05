
import duckdb
import time
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]

con = duckdb.connect(str(ROOT / "assessment.duckdb"))

original_query = """
SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    d.full_date AS transaction_date
FROM dim_employee e, dim_project p,
     fact_transactions t, dim_date d
WHERE e.employee_id = p.project_manager_id
AND e.is_current = TRUE
AND p.project_key = t.project_key
AND d.date_key = t.date_key
AND LOWER(TRIM(p.status)) NOT IN ('completed', 'on hold')
AND t.payment_status = 'Pending'
AND t.amount > (
    SELECT AVG(amount)
    FROM fact_transactions
    WHERE payment_status = 'Pending'
)
ORDER BY e.department, t.amount DESC
"""

optimized_query = (
    ROOT / "solutions" / "submissions"
    / "mohamed-alqubaisi"
    / "02_sql_and_viz"
    / "query_optimization.sql"
).read_text(encoding="utf-8")


def benchmark(query, runs=20):
    # Warm up the query.
    con.execute(query).fetchall()

    timings = []

    for _ in range(runs):
        start = time.perf_counter()
        con.execute(query).fetchall()
        timings.append(
            (time.perf_counter() - start) * 1000
        )

    return statistics.median(timings)


try:
    # Verify the two queries return equivalent results.
    original = con.execute(original_query).fetchdf()
    optimized = con.execute(optimized_query).fetchdf()

    # Both queries sort by department and amount.
    # Ignore data types when checking returned values.
    import pandas as pd

    pd.testing.assert_frame_equal(
        original,
        optimized,
        check_dtype=False
    )

    print("Query results match: YES")

    # Alternate execution order to reduce cache bias.
    original_times = []
    optimized_times = []

    for _ in range(5):
        original_times.append(
            benchmark(original_query, runs=4)
        )
        optimized_times.append(
            benchmark(optimized_query, runs=4)
        )

    original_ms = statistics.median(original_times)
    optimized_ms = statistics.median(optimized_times)

    print("\n=== PERFORMANCE COMPARISON ===")
    print(f"Original median: {original_ms:.3f} ms")
    print(f"Optimized median: {optimized_ms:.3f} ms")
    print(f"Speedup factor: {original_ms / optimized_ms:.2f}x")

finally:
    con.close()
