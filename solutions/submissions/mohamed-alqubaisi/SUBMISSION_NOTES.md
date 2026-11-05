# StackUp Engineering Academy — Data Engineering Assessment

## Submission Summary

This submission contains solutions for all four assessment pillars, covering data engineering foundations, SQL and visualization, big data processing, infrastructure, and data governance.

### Pillar 1 — Foundations

- Cleaned and transformed 500 project records.
- Processed 1,000 employee records and identified data-quality anomalies.
- Implemented employee data-quality checks and reporting.
- Created SQL data models and SCD Type 2 employee history processing.

### Pillar 2 — SQL & Visualization

- Implemented six business SQL queries.
- Developed an ETL pipeline processing 50,000 transactions.
- Preserved missing-value handling decisions and documented transformation rules.
- Performed query benchmarking and optimization; measured improvement was approximately 1.11×, rather than the aspirational 10×.
- Created an annotated dashboard mockup in PDF format.

### Pillar 3 — Big Data Processing

**Apache Spark**
- Processed 99,996 events across 12 monthly JSONL files.
- Generated five analytical output datasets in Parquet format.
- Used PyArrow for Parquet export because native Spark writes were blocked by missing Windows Hadoop utilities.

**Apache Kafka**
- Configured and ran a local Kafka broker.
- Produced and consumed 8,333 events.
- Forwarded 14 critical escalation messages.

**Apache Airflow**
- Implemented a daily ETL DAG with parallel extraction, data-quality validation, transformation, output verification, and pipeline reporting.
- Tested the Python functions and simulated XCom reporting successfully.
- Actual scheduler execution and Airflow UI verification were unavailable because the Windows environment lacked Docker and WSL.

### Pillar 4 — Infrastructure & Governance

**Docker**
- Created container configuration using `python:3.11-slim`.
- Implemented an ETL entry point supporting `DATA_DIR` and `OUTPUT_DIR`.
- Verified processing of all 50,000 transactions locally.
- Latest local ETL execution time: **9.14 seconds**.
- Docker image building, mounted-volume execution, and container performance remain unverified because Docker was unavailable.

**Data Governance**
- Prepared a governance document covering projects, employees, transactions, and salary history.
- Documented classifications, PII handling, ownership, retention proposals, access control, and data lineage.

**Data Quality Framework**
- Implemented six configurable validation checks.
- Generated four Markdown quality reports and a JSON summary.
- Executed 24 checks: 19 passed and five detected data-quality issues.
- Added conditional completeness rules for salary-history hiring records.

## Environment Constraints

Development was performed on a restricted corporate Windows environment without administrator access or Docker Desktop.

Alternative local validation methods were used where possible. Tasks requiring an operational Airflow environment or Docker engine are identified as unverified rather than reported as successfully executed.

## Submission Notes

The implemented code and generated assessment evidence are organized by pillar. The original assessment datasets and any derived records should only be published where their distribution is authorized.

This submission documents known limitations and outstanding runtime-verification requirements.