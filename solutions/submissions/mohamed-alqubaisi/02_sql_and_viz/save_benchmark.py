
import duckdb
import time
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
FOLDER = Path(__file__).resolve().parent

con = duckdb.connect(str(ROOT / "assessment.duckdb"))

original_query = """
SELECT
    e.full_name, e.department, e.role,
    p.project_name, p.status, p.budget,
    p.actual_cost, t.amount, t.category,
    t.payment_status,
    d.full_date AS transaction_date
FROM dim_employee e, dim_project p,
     fact_transactions t, dim_date d
WHERE e.employee_id = p.project_manager_id
AND e.is_current = TRUE
AND p.project_key = t.project_key
AND d.date_key = t.date_key
AND LOWER(TRIM(p.status))
    NOT IN ('completed', 'on hold')
AND t.payment_status = 'Pending'
AND t.amount > (
    SELECT AVG(amount)
    FROM fact_transactions
    WHERE payment_status = 'Pending'
)
ORDER BY e.department, t.amount DESC
"""

optimized_query = (
    FOLDER / "query_optimization.sql"
).read_text(encoding="utf-8")

# Only keep the query before the index section.
optimized_query = optimized_query.split(
    "-- INDEX RECOMMENDATIONS"
)[0].strip()


def benchmark(query, runs=20):
    con.execute(query).fetchall()
    times = []

    for _ in range(runs):
        start = time.perf_counter()
        con.execute(query).fetchall()
        times.append(
            (time.perf_counter() - start) * 1000
        )

    return statistics.median(times)


try:
    original_result = con.execute(original_query).fetchdf()
    optimized_result = con.execute(optimized_query).fetchdf()

    import pandas as pd

    pd.testing.assert_frame_equal(
        original_result,
        optimized_result,
        check_dtype=False
    )

    original_ms = benchmark(original_query)
    optimized_ms = benchmark(optimized_query)

    original_plan = con.execute(
        "EXPLAIN ANALYZE " + original_query
    ).fetchone()[1]

    optimized_plan = con.execute(
        "EXPLAIN ANALYZE " + optimized_query
    ).fetchone()[1]

    report = f"""TASK 2.3 - QUERY OPTIMIZATION EVIDENCE

Results match: YES
Rows returned: {len(original_result)}

Original median: {original_ms:.3f} ms
Rewritten median: {optimized_ms:.3f} ms
Speedup factor: {original_ms / optimized_ms:.2f}x

=== ORIGINAL EXPLAIN ANALYZE ===
{original_plan}

=== REWRITTEN EXPLAIN ANALYZE ===
{optimized_plan}
"""

    output = FOLDER / "benchmark_evidence.txt"
    output.write_text(report, encoding="utf-8")

    print(report.split(
        "=== ORIGINAL EXPLAIN ANALYZE ==="
    )[0])

    print("Saved benchmark_evidence.txt")

finally:
    con.close()
