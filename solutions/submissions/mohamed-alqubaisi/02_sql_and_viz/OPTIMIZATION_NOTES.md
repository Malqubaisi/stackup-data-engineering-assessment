# Task 2.3 — SQL Query Optimization Report

## Objective
Compare the original finance query against rewritten alternatives and evaluate execution performance using DuckDB.

## Benchmark Results

| Query | Median execution time |
|---|---:|
| Original | 19.079 ms |
| Version 1 — CTEs and explicit joins | 21.356 ms |
| Version 2 — Window function | 33.200 ms |

These results were obtained from separate benchmark runs. The Version 2 comparison returned a speedup factor of 0.60x relative to its original-query baseline.

## Query Optimizations Investigated

**Version 1**
- Converted implicit joins to explicit joins.
- Introduced a CTE for average pending transaction amount.
- Applied early filtering.
- Selected only required columns.

**Version 2**
- Used `AVG(amount) OVER ()` to calculate the pending transaction average.
- Applied filtering against the window result.
- Used explicit joins.

## Execution Plan Analysis

The original query was already efficiently optimized by DuckDB.

The execution plan showed:
- Sequential scans on relatively small tables.
- Hash joins for dimension relationships.
- A nested-loop comparison against a single average value.
- One aggregate calculation for the pending transaction average.

The original subquery was uncorrelated and was not executed once per transaction.

## Findings

Neither rewrite produced a measurable performance improvement. The window-function approach was substantially slower in the observed benchmark.

The original query remains the fastest measured implementation.

## Index Recommendations

Potential production indexes include:
- `fact_transactions(payment_status, project_key)`
- `dim_project(project_manager_id)`
- `dim_employee(employee_id, is_current)`

These are recommendations only. Their effectiveness must be evaluated using execution plans and benchmarks. Index maintenance and storage overhead must also be considered.

## Conclusion

SQL readability improvements do not guarantee faster execution. DuckDB already optimized the original query effectively, so the proposed rewrites provided no performance benefit at the tested data volume.

A larger dataset, different query patterns, or a different database engine may produce different results.
