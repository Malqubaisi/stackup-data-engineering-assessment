
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]

import os

DATA_DIR = Path(os.environ.get("DATA_DIR", str(ROOT / "datasets")))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(ROOT / "outputs")))


def load_transactions():
    """Load transaction JSON into a DataFrame."""

    path = DATA_DIR / "transactions.json"
    df = pd.read_json(path)

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        errors="coerce"
    )

    return df


def enrich_transactions(transactions, projects, employees):
    """Join transactions to projects and current employees."""

    df = transactions.copy()

    # Avoid duplicate columns and unnecessary joins.
    project_lookup = projects[
        ["project_id", "project_name", "department"]
    ].drop_duplicates("project_id")

    df = df.merge(
        project_lookup,
        on="project_id",
        how="left",
        validate="many_to_one"
    )

    employee_lookup = employees[
        ["employee_id", "full_name"]
    ].drop_duplicates("employee_id")

    df = df.merge(
        employee_lookup,
        left_on="approved_by",
        right_on="employee_id",
        how="left",
        validate="many_to_one"
    )

    # Required derived columns
    df["is_approved"] = df["approved_by"].notna()

    df["amount_aed"] = (
        pd.to_numeric(df["amount"], errors="coerce")
        .fillna(0.0)
        .astype(float)
    )

    df["transaction_year_month"] = (
        df["transaction_date"].dt.strftime("%Y-%m")
    )

    return df


def main():
    start = time.perf_counter()

    projects = pd.read_csv(
        OUTPUT_DIR / "projects_clean.csv"
    )

    employees = pd.read_csv(
        OUTPUT_DIR / "employees_clean.csv"
    )

    transactions = load_transactions()

    cleaned_transactions = enrich_transactions(
        transactions,
        projects,
        employees
    )

    # Validate row preservation
    if len(cleaned_transactions) != len(transactions):
        raise ValueError("Transaction row count changed after joins")

    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    cleaned_transactions.to_csv(
        output_dir / "transactions_clean.csv",
        index=False
    )

    elapsed = time.perf_counter() - start

    summary = f"""Pipeline Run Summary
Run timestamp: {datetime.now().isoformat()}
Projects before/after: 500 / {len(projects)}
Employees before/after: 1000 / {len(employees)}
Transactions before/after: {len(transactions)} / {len(cleaned_transactions)}
Missing amounts: {transactions['amount'].isna().sum()}
Missing approvers: {transactions['approved_by'].isna().sum()}
Data quality decisions:
- NULL transaction amounts retained in original amount field.
- amount_aed uses 0.0 for NULL amounts as required by Task 2.2.
- Missing approvers remain NULL.
- is_approved indicates presence of an approver ID, not payment status.
- Project and employee lookups use left joins to retain transactions.
Execution time: {elapsed:.3f} seconds
"""

    (output_dir / "pipeline_summary.txt").write_text(
        summary, encoding="utf-8"
    )

    print(summary)
    print("Saved transactions_clean.csv")


if __name__ == "__main__":
    main()
