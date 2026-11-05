import os
import time
from pathlib import Path

# Java 17 configuration for local Spark on Windows
JAVA_HOME = (
    r"C:\Users\mohamed.alqubaisi\Downloads"
    r"\OpenJDK17U-jdk_x64_windows_hotspot_17.0.21_9"
    r"\jdk-17.0.21+9"
)

if not Path(JAVA_HOME, "bin", "java.exe").exists():
    raise FileNotFoundError(
        f"Java installation not found: {JAVA_HOME}"
    )

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = (
    str(Path(JAVA_HOME) / "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)

# Ensure Spark uses the Python 3.11 environment
ROOT = Path(__file__).resolve().parents[4]
SPARK_PYTHON = ROOT / "venv_spark" / "Scripts" / "python.exe"

os.environ["PYSPARK_PYTHON"] = str(SPARK_PYTHON)
os.environ["PYSPARK_DRIVER_PYTHON"] = str(SPARK_PYTHON)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    MapType,
)

# Start performance timer
pipeline_start = time.time()

# Repository root
ROOT = Path(__file__).resolve().parents[4]

# Input files
EVENTS_FOLDER = ROOT / "datasets" / "events_stream"

event_files = sorted(
    EVENTS_FOLDER.glob("events_2025_*.jsonl")
)

print("Monthly files found:", len(event_files))

if len(event_files) != 12:
    raise ValueError(
        f"Expected 12 monthly files, found {len(event_files)}"
    )

EVENTS_PATH = [str(file) for file in event_files]

# Start Spark
spark = (
    SparkSession.builder
    .appName("PresightEventsPipeline")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Explicit schema
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField(
        "payload",
        MapType(StringType(), StringType()),
        True,
    ),
])

# Load all monthly JSONL files
events = (
    spark.read
    .schema(event_schema)
    .json(EVENTS_PATH)
)

# Verify counts and schema
total_events = events.count()

print("=" * 50)
print("TASK 3.1a - SPARK EVENT LOADING")
print("=" * 50)

print("Total events loaded:", total_events)
print("Input path:", EVENTS_PATH)

events.printSchema()

print("Sample events:")
events.select(
    "event_id",
    "event_type",
    "project_id",
    "user_id",
    "timestamp",
).show(5, truncate=False)

print(
    "Loading execution time:",
    round(time.time() - pipeline_start, 2),
    "seconds",
)


# ==========================================
# TASK 3.1b - VALIDATE AND CLEAN
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1b - EVENT VALIDATION AND CLEANING")
print("=" * 50)

# Step 1: Remove events with missing IDs
before_null_drop = events.count()

valid_events = events.dropna(
    subset=["event_id", "user_id"]
)

after_null_drop = valid_events.count()

print("Rows before null removal:", before_null_drop)
print("Rows after null removal:", after_null_drop)
print("Rows removed (missing IDs):",
      before_null_drop - after_null_drop)

# Step 2: Keep earliest event for duplicate IDs
dedup_window = Window.partitionBy("event_id").orderBy(
    F.col("timestamp").asc_nulls_last(),
    F.col("user_id").asc(),
    F.col("event_type").asc()
)

clean_events = (
    valid_events
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

after_dedup = clean_events.count()

print("Rows before deduplication:", after_null_drop)
print("Rows after deduplication:", after_dedup)
print("Duplicate event IDs removed:",
      after_null_drop - after_dedup)

# Step 3: Derive date and time fields
clean_events = (
    clean_events
    .withColumn("event_date", F.to_date("timestamp"))
    .withColumn("event_hour", F.hour("timestamp"))
    .withColumn(
        "event_month",
        F.date_format("timestamp", "yyyy-MM")
    )
)

print("\nCleaned event schema:")
clean_events.printSchema()

print("\nSample cleaned events:")
clean_events.select(
    "event_id",
    "event_type",
    "event_date",
    "event_hour",
    "event_month"
).show(5, truncate=False)

print("TASK 3.1b COMPLETED SUCCESSFULLY!")


# ==========================================
# TASK 3.1c - TABLE 1
# PROJECT ACTIVITY SUMMARY
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1c - PROJECT ACTIVITY SUMMARY")
print("=" * 50)

aggregation_start = time.time()

project_activity_summary = (
    clean_events
    .filter(F.col("project_id").isNotNull())
    .groupBy("project_id")
    .agg(
        F.count("*").alias("total_events"),

        F.sum(
            F.when(
                F.col("event_type") == "escalation_raised", 1
            ).otherwise(0)
        ).alias("escalation_count"),

        F.sum(
            F.when(
                F.col("event_type") == "task_completed", 1
            ).otherwise(0)
        ).alias("task_completions"),

        F.sum(
            F.when(
                F.col("event_type") == "document_uploaded", 1
            ).otherwise(0)
        ).alias("document_uploads"),

        F.max("timestamp").alias("last_event_timestamp"),

        F.countDistinct("user_id").alias("unique_users"),

        F.countDistinct("event_type").alias("unique_event_types"),
    )
)

project_count = project_activity_summary.count()

print("Projects with activity:", project_count)

project_activity_summary.orderBy(
    F.desc("total_events")
).show(10, truncate=False)

print(
    "Aggregation time:",
    round(time.time() - aggregation_start, 2),
    "seconds"
)

print("PROJECT ACTIVITY SUMMARY COMPLETED!")


# ==========================================
# TASK 3.1c - TABLE 2
# USER ACTIVITY SUMMARY
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1c - USER ACTIVITY SUMMARY")
print("=" * 50)

user_start = time.time()

user_activity_summary = (
    clean_events
    .groupBy("user_id")
    .agg(
        F.sum(
            F.when(F.col("event_type") == "login", 1)
            .otherwise(0)
        ).alias("login_count"),

        F.sum(
            F.when(F.col("event_type") == "logout", 1)
            .otherwise(0)
        ).alias("logout_count"),

        F.sum(
            F.when(
                ~F.col("event_type").isin("login", "logout"),
                1
            ).otherwise(0)
        ).alias("actions_taken"),

        F.countDistinct("project_id")
        .alias("projects_touched"),

        F.min("timestamp").alias("first_active"),

        F.max("timestamp").alias("last_active"),

        F.countDistinct("event_date")
        .alias("active_days"),
    )
)

user_count = user_activity_summary.count()

print("Users with activity:", user_count)

print("\nTop 10 users by actions taken:")
user_activity_summary.orderBy(
    F.desc("actions_taken")
).show(10, truncate=False)

print(
    "User aggregation time:",
    round(time.time() - user_start, 2),
    "seconds"
)

print("USER ACTIVITY SUMMARY COMPLETED!")


# ==========================================
# TASK 3.1c - TABLE 3
# ESCALATION LOG
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1c - ESCALATION LOG")
print("=" * 50)

escalation_start = time.time()

# Raised escalation records
raised = (
    clean_events
    .filter(
        (F.col("event_type") == "escalation_raised")
        & F.col("project_id").isNotNull()
    )
    .select(
        F.col("event_id").alias("escalation_id"),
        "project_id",
        F.col("timestamp").alias("raised_at"),
        F.col("payload").getItem("severity").alias("severity"),
    )
)

# Resolution records
resolved_events = (
    clean_events
    .filter(
        (F.col("event_type") == "escalation_resolved")
        & F.col("project_id").isNotNull()
    )
    .select(
        F.col("event_id").alias("resolution_event_id"),
        "project_id",
        F.col("timestamp").alias("resolved_at"),
        F.col("payload").getItem("resolved_by").alias("resolved_by"),
    )
)

# Assign a chronological sequence within each project.
# This matches the first raised escalation with the first
# resolution after it, without reusing resolution events.


# ==========================================
# MATCH RAISED AND RESOLVED ESCALATIONS
# ==========================================

from collections import deque
from pyspark.sql.types import BooleanType, DoubleType

# Keep only escalation events for chronological matching
escalation_events = (
    clean_events
    .filter(
        F.col("event_type").isin(
            "escalation_raised",
            "escalation_resolved"
        )
        & F.col("project_id").isNotNull()
        & F.col("timestamp").isNotNull()
    )
    .select(
        "event_id",
        "project_id",
        "event_type",
        "timestamp",
        F.col("payload")["severity"].alias("severity"),
        F.col("payload")["resolved_by"].alias("resolved_by")
    )
)

# Match events separately within each project
def match_project_events(group):
    project_id, records = group

    # Sort by time; raised events come before resolved
    # events when their timestamps are identical.
    ordered = sorted(
        records,
        key=lambda r: (
            r[2],
            0 if r[1] == "escalation_raised" else 1,
            r[0]
        )
    )

    open_escalations = deque()
    results = []

    for event_id, event_type, event_time, severity, resolver in ordered:
        if event_type == "escalation_raised":
            open_escalations.append(
                (event_id, event_time, severity)
            )

        elif event_type == "escalation_resolved":
            if open_escalations:
                raised_id, raised_at, raised_severity = (
                    open_escalations.popleft()
                )

                hours = (
                    event_time - raised_at
                ).total_seconds() / 3600.0

                results.append((
                    raised_id,
                    project_id,
                    raised_at,
                    raised_severity,
                    event_time,
                    resolver,
                    hours,
                    True
                ))

    # Any remaining raised events were not resolved
    for raised_id, raised_at, severity in open_escalations:
        results.append((
            raised_id,
            project_id,
            raised_at,
            severity,
            None,
            None,
            None,
            False
        ))

    return results


# Schema for the completed escalation log
escalation_schema = StructType([
    StructField("escalation_id", StringType(), False),
    StructField("project_id", StringType(), False),
    StructField("raised_at", TimestampType(), True),
    StructField("severity", StringType(), True),
    StructField("resolved_at", TimestampType(), True),
    StructField("resolved_by", StringType(), True),
    StructField("resolution_time_hours", DoubleType(), True),
    StructField("resolved", BooleanType(), False),
])

# Distribute matching by project using Spark RDDs
matched_rdd = (
    escalation_events.rdd
    .map(
        lambda r: (
            r["project_id"],
            (
                r["event_id"],
                r["event_type"],
                r["timestamp"],
                r["severity"],
                r["resolved_by"]
            )
        )
    )
    .groupByKey()
    .flatMap(match_project_events)
)

escalation_log = spark.createDataFrame(
    matched_rdd,
    schema=escalation_schema
).cache()

# Validate the completed results
total_escalations = escalation_log.count()

resolved_count = escalation_log.filter(
    F.col("resolved") == True
).count()

unresolved_count = escalation_log.filter(
    F.col("resolved") == False
).count()

print("\nESCALATION MATCHING RESULTS")
print("Total escalations:", total_escalations)
print("Resolved escalations:", resolved_count)
print("Unresolved escalations:", unresolved_count)

if total_escalations != raised.count():
    raise ValueError(
        "Escalation matching lost or duplicated raised events!"
    )

print("\nSample escalation log:")
escalation_log.orderBy(
    F.desc("resolution_time_hours")
).show(10, truncate=False)

print("ESCALATION LOG COMPLETED SUCCESSFULLY!")


print("Raised escalation events:", raised.count())
print("Resolution events:", resolved_events.count())


# ==========================================
# TASK 3.1c - TABLE 4
# DAILY EVENT VOLUME
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1c - DAILY EVENT VOLUME")
print("=" * 50)

daily_start = time.time()

# Count events per day and event type
daily_counts = (
    clean_events
    .filter(F.col("event_date").isNotNull())
    .groupBy("event_date", "event_type")
    .agg(F.count("*").alias("event_count"))
)

# Running total for each event type
daily_window = (
    Window.partitionBy("event_type")
    .orderBy("event_date")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

daily_event_volume = (
    daily_counts
    .withColumn(
        "cumulative_count",
        F.sum("event_count").over(daily_window)
    )
)

daily_count = daily_event_volume.count()

print("Daily event volume rows:", daily_count)

print("\nSample daily event volume:")
daily_event_volume.orderBy(
    F.asc("event_date"),
    F.desc("event_count")
).show(10, truncate=False)

print(
    "Daily aggregation time:",
    round(time.time() - daily_start, 2),
    "seconds"
)

print("DAILY EVENT VOLUME COMPLETED SUCCESSFULLY!")


# ==========================================
# TASK 3.1c - TABLE 5
# PEAK USAGE ANALYSIS
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1c - PEAK USAGE ANALYSIS")
print("=" * 50)

peak_start = time.time()

# Count events and users for each date and hour
peak_usage_analysis = (
    clean_events
    .filter(
        F.col("event_date").isNotNull()
        & F.col("event_hour").isNotNull()
    )
    .groupBy("event_date", "event_hour")
    .agg(
        F.count("*").alias("total_events"),

        F.countDistinct("user_id").alias("unique_users"),

        F.countDistinct("event_type")
        .alias("event_types_per_hour"),
    )
    .orderBy(
        F.desc("total_events"),
        F.asc("event_date"),
        F.asc("event_hour"),
    )
    .limit(20)
)

peak_count = peak_usage_analysis.count()

print("Peak usage rows:", peak_count)

print("\nTop 20 busiest hours:")
peak_usage_analysis.show(20, truncate=False)

print(
    "Peak usage aggregation time:",
    round(time.time() - peak_start, 2),
    "seconds"
)

print("PEAK USAGE ANALYSIS COMPLETED SUCCESSFULLY!")



# ==========================================
# TASK 3.1d - EXPORT FIVE PARQUET TABLES
# ==========================================

import pandas as pd
import pyarrow.parquet as pq
import shutil

print("\n" + "=" * 50)
print("TASK 3.1d - EXPORT ALL PARQUET TABLES")
print("=" * 50)

spark_output = ROOT / "outputs" / "spark"
spark_output.mkdir(parents=True, exist_ok=True)

# All aggregations have already been calculated
output_tables = {
    "project_activity_summary": project_activity_summary,
    "user_activity_summary": user_activity_summary,
    "escalation_log": escalation_log,
    "daily_event_volume": daily_event_volume,
    "peak_usage_analysis": peak_usage_analysis,
}

# Required partition columns
partition_columns = {
    "escalation_log": "severity",
    "daily_event_volume": "event_date",
}

export_results = {}
export_start = time.time()

for table_name, spark_df in output_tables.items():

    table_start = time.time()
    output_path = spark_output / table_name

    print(f"\nExporting {table_name}...")

    # Convert only the small aggregated result to Pandas.
    # All transformations remain in Spark.
    pandas_df = spark_df.toPandas()

    expected_rows = len(pandas_df)

    # Overwrite previous output for this table
    if output_path.exists():
        shutil.rmtree(output_path)

    output_path.mkdir(parents=True, exist_ok=True)

    partition_col = partition_columns.get(table_name)

    if partition_col is not None:

        # Write one file per partition.
        # This produces Hive-style partition folders.
        for partition_value, group_df in pandas_df.groupby(
            partition_col, dropna=False
        ):

            if pd.isna(partition_value):
                partition_name = "__HIVE_DEFAULT_PARTITION__"
            else:
                partition_name = str(partition_value)

            partition_path = (
                output_path
                / f"{partition_col}={partition_name}"
            )

            partition_path.mkdir(
                parents=True,
                exist_ok=True
            )

            # Partition value is represented by directory name
            data_to_write = group_df.drop(
                columns=[partition_col]
            )

            data_to_write.to_parquet(
                partition_path / "part-00000.parquet",
                engine="pyarrow",
                index=False
            )

    else:

        # One Parquet file for small unpartitioned tables
        pandas_df.to_parquet(
            output_path / "part-00000.parquet",
            engine="pyarrow",
            index=False
        )

    # Verify rows from the actual saved Parquet files
    parquet_files = list(
        output_path.rglob("*.parquet")
    )

    verified_rows = sum(
        pq.read_metadata(str(file)).num_rows
        for file in parquet_files
    )

    if verified_rows != expected_rows:
        raise ValueError(
            f"{table_name}: expected {expected_rows} rows, "
            f"found {verified_rows} in saved Parquet files"
        )

    elapsed = time.time() - table_start

    export_results[table_name] = {
        "rows": verified_rows,
        "files": len(parquet_files),
        "seconds": round(elapsed, 2),
    }

    print("Saved rows:", verified_rows)
    print("Parquet files:", len(parquet_files))
    print("Export time:", round(elapsed, 2), "seconds")
    print("Output:", output_path)

print("\n" + "=" * 50)
print("PARQUET EXPORT SUMMARY")
print("=" * 50)

for name, result in export_results.items():
    print(
        f"{name}: {result['rows']} rows, "
        f"{result['files']} files"
    )

print(
    "\nTotal export time:",
    round(time.time() - export_start, 2),
    "seconds"
)

print("ALL FIVE PARQUET TABLES EXPORTED SUCCESSFULLY!")

# ==========================================
# TASK 3.1e - PERFORMANCE BASELINE
# ==========================================

print("\n" + "=" * 50)
print("TASK 3.1e - PERFORMANCE BASELINE")
print("=" * 50)

total_execution_time = time.time() - pipeline_start

print("Total raw events processed:", total_events)
print("Total clean events:", after_dedup)
print(
    "Total execution time:",
    round(total_execution_time, 2),
    "seconds"
)

print(
    "Events processed per second:",
    round(total_events / total_execution_time, 2)
)

print("\nAggregation timings:")
print(
    "Project activity:",
    round(user_start - aggregation_start, 2),
    "seconds"
)
print(
    "User activity:",
    round(escalation_start - user_start, 2),
    "seconds"
)
print(
    "Escalation stage:",
    round(daily_start - escalation_start, 2),
    "seconds"
)
print(
    "Daily event volume:",
    round(peak_start - daily_start, 2),
    "seconds"
)
print(
    "Peak usage:",
    round(export_start - peak_start, 2),
    "seconds"
)

print("\nTASK 3.1 PIPELINE COMPLETED SUCCESSFULLY!")


spark.stop()

print("TASK 3.1a COMPLETED SUCCESSFULLY!")
