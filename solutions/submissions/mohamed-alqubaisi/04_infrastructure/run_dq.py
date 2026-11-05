
import json
import logging
from pathlib import Path

import pandas as pd

from dq_framework import run_data_quality_checks

ROOT = Path(__file__).resolve().parents[4]
DATA_DIR = ROOT / "datasets"
OUTPUT_DIR = ROOT / "outputs"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)


def main():
    datasets = {
        "projects": pd.read_csv(
            DATA_DIR / "projects.csv"
        ),
        "employees": pd.read_csv(
            DATA_DIR / "employees.csv"
        ),
        "transactions": pd.read_json(
            DATA_DIR / "transactions.json"
        ),
        "employees_salary_history": pd.read_csv(
            DATA_DIR / "employees_salary_history.csv"
        ),
    }

    summaries = {}

    for name, df in datasets.items():
        print(f"\nValidating {name} ({len(df):,} rows)")

        result = run_data_quality_checks(
            df,
            name,
            reference_data=datasets
        )

        summaries[name] = result

        print(
            f"Passed: {result['checks_passed']} / "
            f"{result['checks_run']}"
        )

    summary_path = OUTPUT_DIR / "dq_summary.json"
    summary_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    summary_path.write_text(
        json.dumps(summaries, indent=2),
        encoding="utf-8"
    )

    print(f"\nSummary saved: {summary_path}")
    print("DATA QUALITY FRAMEWORK EXECUTION COMPLETE")


if __name__ == "__main__":
    main()
