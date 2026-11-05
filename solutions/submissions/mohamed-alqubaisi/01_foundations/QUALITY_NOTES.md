# Pillar 1 — Foundations: Implementation Notes

## Task 1.1 — Project Transformation
- Processed 500 project records using Python and Pandas.
- Calculated budget variance, budget utilisation, project duration, over-budget indicators, status categories, and risk levels.
- Used vectorised operations rather than row-by-row loops.
- Preserved missing dates and undefined budget utilisation values where appropriate.
- Generated `outputs/projects_clean.csv` containing 500 rows and 17 columns.

## Task 1.2 — Data Warehouse and SCD Type 2
- Designed and populated six tables using DuckDB.
- Loaded 500 projects, 25 vendors, 1,538 transaction dates, 50,000 transactions, and 500 project-manager assignments.
- Created 2,231 employee dimension versions, including 1,000 current records.
- Validated that each employee has exactly one current version and that historical validity periods do not overlap.
- Consolidated one pair of same-day employee changes into the final effective state for that date.
- Used a documented technical fallback date for eight employees whose hire dates were invalid and could not be recovered.
- Preserved 740 disputed transactions with unknown amounts as NULL.
- Identified 2,469 transactions without matched employee dimension keys: 2,444 with missing approvers and 25 requiring historical-date reconciliation.

## Task 1.3 — Employee Data Quality
- Processed 1,000 employee records.
- Detected 10 missing emails, eight invalid hire dates, five negative experience values, and three suspicious Junior salary values.
- Flagged 25 distinct employees for review.
- Converted invalid dates and negative experience values into missing values.
- Preserved missing email values and suspicious salary amounts rather than inventing replacements.
- Generated `outputs/employees_clean.csv` and an employee data-quality summary.

## Assumptions and Limitations
- Salary thresholds are provisional anomaly-detection rules, not verified organizational compensation policies.
- Technical fallback dates in the historical dimension must not be interpreted as verified employment dates.
- The employee-project bridge contains known project-manager relationships, not a complete history of team assignments.
- Additional source-to-warehouse reconciliation and review of unresolved data quality exceptions are recommended before production use.
