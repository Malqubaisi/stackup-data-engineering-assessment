
# Pillar 3 — Implementation and Testing Notes

## Task 3.1 — Apache Spark

- Python 3.11.9, Java 17.0.21, PySpark 3.5.9.
- Loaded 99,996 events from 12 monthly JSONL files.
- Validated required IDs and removed duplicates.
- Generated five aggregation tables.
- Exported five Parquet datasets.
- Total execution time: 182.54 seconds.
- Processing throughput: 547.81 events/second.
- Spark transformations executed successfully.
- Native Spark Parquet writes were blocked by missing Windows Hadoop utilities.
- Pandas/PyArrow was used to export the aggregated results, retaining the required partition directories.

## Task 3.2 — Apache Kafka

- Apache Kafka 3.9.2 running locally in KRaft mode.
- Docker was not required.
- Created both assessment topics.
- Produced 8,333 January event messages.
- Consumed 8,333 messages.
- Forwarded 14 critical escalation messages.
- Verified critical topic partition 0 latest offset: 14.
- Consumer throughput: 6,839.9 messages/second.
- Generated outputs/kafka/summary.json.

## Task 3.3 — Apache Airflow

- Implemented presight_etl_pipeline DAG.
- Configured daily schedule at 06:00 Asia/Dubai.
- Defined three parallel extraction tasks.
- Implemented data-quality validation with an 80% completeness threshold.
- Verified valid assessment data passes quality checks.
- Verified intentionally incomplete data raises ValueError.
- Executed transaction transformation successfully.
- Verified cleaned output files.
- Generated pipeline report using simulated XCom values.
- Python syntax validation passed.

### Environment limitation

The assessment was developed on a restricted corporate Windows laptop.
Docker and WSL were unavailable.

Apache Airflow was not installed or executed in a supported Linux environment.
Therefore, DAG import, scheduler execution, retry behavior, actual task dependencies,
and Airflow UI visibility were not independently verified.

The implementation is ready for validation in a supported Airflow environment.
