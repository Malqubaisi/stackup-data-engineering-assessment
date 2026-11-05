import subprocess
import sys

import csv
import json
from pathlib import Path

from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator


# Repository root
ROOT = Path(__file__).resolve().parents[4]
DATASETS = ROOT / "datasets"


def count_csv_rows(file_path):
    """Count data rows, excluding the header."""
    with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
        return sum(1 for row in csv.DictReader(file))


def load_projects():
    """Extract project records."""
    path = DATASETS / "projects.csv"
    count = count_csv_rows(path)

    print(f"Projects extracted: {count}")
    return {"raw_count": count, "source": str(path)}


def load_employees():
    """Extract employee records."""
    path = DATASETS / "employees.csv"
    count = count_csv_rows(path)

    print(f"Employees extracted: {count}")
    return {"raw_count": count, "source": str(path)}


def load_transactions():
    """Extract transaction records."""
    path = DATASETS / "transactions.json"

    with open(path, "r", encoding="utf-8") as file:
        content = file.read().strip()

    try:
        data = json.loads(content)
        if isinstance(data, list):
            count = len(data)
        elif isinstance(data, dict):
            count = len(data.get("transactions", []))
        else:
            raise ValueError("Unsupported JSON structure")
    except json.JSONDecodeError:
        # Support newline-delimited JSON as well
        count = sum(
            1 for line in content.splitlines()
            if line.strip()
        )

    print(f"Transactions extracted: {count}")
    return {"raw_count": count, "source": str(path)}

# ==========================================
# TASK 3.3d - DATA QUALITY GATE
# ==========================================

def run_data_quality_checks(dataset_name, records, key_columns):
    """Validate completeness of required key columns."""

    total = len(records)

    if total == 0:
        raise ValueError(
            f"{dataset_name}: Dataset is empty"
        )

    results = {}

    for column in key_columns:
        valid = sum(
            1 for row in records
            if row.get(column) is not None
            and str(row.get(column)).strip() != ""
        )

        completeness = valid / total * 100

        results[column] = round(completeness, 2)

        print(
            f"{dataset_name}.{column}: "
            f"{completeness:.2f}% complete"
        )

        if completeness < 80:
            raise ValueError(
                f"DATA QUALITY GATE FAILED: "
                f"{dataset_name}.{column} completeness "
                f"is {completeness:.2f}%, below 80%"
            )

    return results


def validate_data_quality():
    """Reload datasets and run critical DQ checks."""

    print("Starting data quality validation...")

    with open(
        DATASETS / "projects.csv",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        projects = list(csv.DictReader(file))

    with open(
        DATASETS / "employees.csv",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        employees = list(csv.DictReader(file))

    with open(
        DATASETS / "transactions.json",
        encoding="utf-8"
    ) as file:
        transactions = json.load(file)

    if isinstance(transactions, dict):
        transactions = transactions["transactions"]

    results = {
        "projects": run_data_quality_checks(
            "projects",
            projects,
            ["project_id"]
        ),
        "employees": run_data_quality_checks(
            "employees",
            employees,
            ["employee_id"]
        ),
        "transactions": run_data_quality_checks(
            "transactions",
            transactions,
            ["transaction_id", "project_id"]
        ),
    }

    print("DATA QUALITY GATE PASSED")
    return results


# ==========================================
# TASK 3.3 - TRANSFORM AND ENRICH
# ==========================================

def transform_and_enrich():
    """Run the previously completed transaction ETL pipeline."""

    print("Starting transaction transformation...")

    etl_script = (
        ROOT
        / "solutions"
        / "submissions"
        / "mohamed-alqubaisi"
        / "02_sql_and_viz"
        / "etl_full.py"
    )

    if not etl_script.exists():
        raise FileNotFoundError(
            f"ETL script not found: {etl_script}"
        )

    # Reuse the existing Pillar 2 ETL implementation
    result = subprocess.run(
        [sys.executable, str(etl_script)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=True
    )

    print(result.stdout)

    clean_files = {
        "projects": ROOT / "outputs" / "projects_clean.csv",
        "employees": ROOT / "outputs" / "employees_clean.csv",
        "transactions": ROOT / "outputs" / "transactions_clean.csv",
    }

    clean_counts = {}

    for name, path in clean_files.items():
        if not path.exists():
            raise FileNotFoundError(
                f"Clean dataset not found: {path}"
            )

        count = count_csv_rows(path)
        clean_counts[name] = count

        print(f"{name}: {count} clean records")

    print("TRANSFORM AND ENRICH COMPLETED")

    # Airflow will automatically push this return value to XCom
    return clean_counts


# ==========================================
# TASK 3.3 - LOAD AND PIPELINE REPORT
# ==========================================

def write_outputs():
    """Verify cleaned ETL outputs and return their locations."""

    output_files = {
        "projects": ROOT / "outputs" / "projects_clean.csv",
        "employees": ROOT / "outputs" / "employees_clean.csv",
        "transactions": ROOT / "outputs" / "transactions_clean.csv",
    }

    written_paths = {}

    for name, path in output_files.items():
        if not path.is_file():
            raise FileNotFoundError(
                f"Required output missing: {path}"
            )

        written_paths[name] = str(path)
        print(f"Verified output: {path}")

    print("LOAD TO OUTPUT COMPLETED")
    return written_paths


def generate_pipeline_report(**context):
    """Build the ETL execution report from Airflow XCom values."""

    ti = context["ti"]

    raw_tasks = {
        "projects": "extract_projects",
        "employees": "extract_employees",
        "transactions": "extract_transactions",
    }

    clean_counts = ti.xcom_pull(
        task_ids="transform_and_enrich"
    )

    dq_results = ti.xcom_pull(
        task_ids="validate_data_quality"
    )

    output_paths = ti.xcom_pull(
        task_ids="load_to_output"
    )

    if not all(isinstance(x, dict) for x in (
        clean_counts, dq_results, output_paths
    )):
        raise ValueError("Missing required XCom results")

    execution_date = context["logical_date"].strftime(
        "%Y-%m-%d"
    )

    report_path = (
        ROOT / "outputs" /
        f"pipeline_report_{execution_date}.txt"
    )

    lines = [
        "PRESIGHT ETL PIPELINE REPORT",
        "=" * 50,
        f"Execution date: {execution_date}",
        "",
        "RAW VS CLEAN ROW COUNTS",
    ]

    for name, task_id in raw_tasks.items():
        raw_result = ti.xcom_pull(task_ids=task_id)

        if not isinstance(raw_result, dict):
            raise ValueError(
                f"Missing extraction XCom: {task_id}"
            )

        raw_count = raw_result["raw_count"]
        clean_count = clean_counts[name]

        lines.append(
            f"{name}: {raw_count} raw / "
            f"{clean_count} clean"
        )

    lines.extend([
        "",
        "DATA QUALITY RESULTS",
    ])

    for dataset, checks in dq_results.items():
        for column, completeness in checks.items():
            lines.append(
                f"{dataset}.{column}: "
                f"{completeness:.2f}% complete"
            )

    lines.extend([
        "",
        "OUTPUT FILES",
    ])

    for name, path in output_paths.items():
        lines.append(f"{name}: {path}")

    report_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    report_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8"
    )

    print(f"Pipeline report written: {report_path}")

    return str(report_path)

# UAE timezone (UTC+4)
uae_timezone = pendulum.timezone("Asia/Dubai")

# Airflow DAG configuration
with DAG(
    dag_id="presight_etl_pipeline",
    description="Presight daily ETL pipeline with data quality checks",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026, 11, 1, tz=uae_timezone
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["presight", "etl", "assessment"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    
    # Extract source datasets in parallel
    extract_projects = PythonOperator(
        task_id="extract_projects",
        python_callable=load_projects,
    )

    extract_employees = PythonOperator(
        task_id="extract_employees",
        python_callable=load_employees,
    )

    extract_transactions = PythonOperator(
        task_id="extract_transactions",
        python_callable=load_transactions,
    )

    # Temporary dependencies while building the DAG
    start >> [
        extract_projects,
        extract_employees,
        extract_transactions,
    ]

    validate_dq = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    transform_task = PythonOperator(
        task_id="transform_and_enrich",
        python_callable=transform_and_enrich,
    )

    load_task = PythonOperator(
        task_id="load_to_output",
        python_callable=write_outputs,
    )

    report_task = PythonOperator(
        task_id="generate_pipeline_report",
        python_callable=generate_pipeline_report,
    )

    [
        extract_projects,
        extract_employees,
        extract_transactions,
    ] >> validate_dq >> transform_task >> load_task >> report_task >> end