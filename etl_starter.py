
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent

DATA_DIR = Path(
    os.environ.get("DATA_DIR", str(ROOT / "datasets"))
).resolve()

OUTPUT_DIR = Path(
    os.environ.get("OUTPUT_DIR", str(ROOT / "outputs"))
).resolve()

SUBMISSION = (
    ROOT / "solutions" / "submissions" / "mohamed-alqubaisi"
)

SCRIPTS = [
    SUBMISSION / "01_foundations" / "etl-pipeline.py",
    SUBMISSION / "01_foundations" / "employee_quality.py",
    SUBMISSION / "02_sql_and_viz" / "etl_full.py",
]


def main():
    start = time.perf_counter()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(
            f"Dataset directory not found: {DATA_DIR}"
        )

    env = os.environ.copy()
    env["DATA_DIR"] = str(DATA_DIR)
    env["OUTPUT_DIR"] = str(OUTPUT_DIR)

    print("=" * 55, flush=True)
    print("PRESIGHT DOCKER ETL PIPELINE", flush=True)
    print(f"Input:  {DATA_DIR}", flush=True)
    print(f"Output: {OUTPUT_DIR}", flush=True)
    print("=" * 55, flush=True)

    for script in SCRIPTS:
        if not script.is_file():
            raise FileNotFoundError(
                f"ETL script missing: {script}"
            )

        print(f"\nRunning: {script.name}", flush=True)

        subprocess.run(
            [sys.executable, str(script)],
            cwd=str(ROOT),
            env=env,
            check=True,
        )

    expected = [
        "projects_clean.csv",
        "employees_clean.csv",
        "transactions_clean.csv",
    ]

    for filename in expected:
        path = OUTPUT_DIR / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Expected output missing: {path}"
            )

        print(f"Verified: {filename}", flush=True)

    elapsed = time.perf_counter() - start

    print("\nETL PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Total execution time: {elapsed:.2f} seconds")

    if elapsed >= 30:
        print("WARNING: Pipeline exceeded 30 seconds")


if __name__ == "__main__":
    main()
