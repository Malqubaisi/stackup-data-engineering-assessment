import os

import pandas as pd
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]

DATA_DIR = Path(os.environ.get("DATA_DIR", str(ROOT / "datasets")))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(ROOT / "outputs")))


# Configure detection logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


def clean_employees():
    """Detect, document and clean employee data issues."""

    df = pd.read_csv(DATA_DIR / "employees.csv")
    df = df.copy()

    logger.info(f"Loaded {len(df)} employees")

    issues = {}

    # 1. Detect missing emails
    email_missing = (
        df["email"].isna()
        | df["email"].fillna("").astype(str).str.strip().eq("")
    )
    issues["Missing email"] = int(email_missing.sum())
    logger.info(f"Missing emails: {email_missing.sum()}")

    # Keep missing emails as NULL rather than inventing them
    df.loc[email_missing, "email"] = pd.NA

    # 2. Detect invalid or implausible hire dates
    dates = pd.to_datetime(df["hire_date"], errors="coerce")
    invalid_dates = (
        dates.isna()
        | dates.lt(pd.Timestamp("1990-01-01"))
        | dates.gt(pd.Timestamp.today())
    )
    issues["Invalid hire date"] = int(invalid_dates.sum())
    logger.info(f"Invalid hire dates: {invalid_dates.sum()}")

    # Preserve unknown dates as missing values
    df["hire_date"] = dates.mask(invalid_dates)

    # 3. Detect negative or implausible experience
    experience = pd.to_numeric(
        df["years_experience"], errors="coerce"
    )

    invalid_experience = (
        experience.isna()
        | experience.lt(0)
        | experience.gt(50)
    )

    issues["Invalid experience"] = int(
        invalid_experience.sum()
    )
    logger.info(
        f"Invalid experience: {invalid_experience.sum()}"
    )

    df["years_experience"] = experience.mask(
        invalid_experience
    )

    # 4. Detect salary anomalies by level
    # Thresholds are investigation rules, not confirmed pay bands.
    salary_limits = {
        "Junior": (10000, 17000),
        "Mid": (17000, 25000),
        "Senior": (25000, 37000),
        "Lead": (37000, 51000),
        "Director": (50000, 75000)
    }

    salaries = pd.to_numeric(df["salary"], errors="coerce")

    invalid_salary = salaries.isna() | salaries.le(0)
    suspicious_salary = pd.Series(
        False, index=df.index
    )

    for level, (minimum, maximum) in salary_limits.items():
        mask = (
            df["level"].eq(level)
            & ~salaries.between(minimum, maximum)
            & ~invalid_salary
        )
        suspicious_salary |= mask

    issues["Invalid salary"] = int(invalid_salary.sum())
    issues["Suspicious salary"] = int(
        suspicious_salary.sum()
    )

    logger.info(
        f"Suspicious salaries: {suspicious_salary.sum()}"
    )

    df["salary"] = salaries.mask(invalid_salary)

    # 5. Check employee ID duplicates
    duplicate_ids = df["employee_id"].duplicated(
        keep=False
    )
    issues["Duplicate employee ID"] = int(
        duplicate_ids.sum()
    )
    logger.info(
        f"Duplicate employee IDs: {duplicate_ids.sum()}"
    )

    # 6. Check inactive managers on active projects
    projects = pd.read_csv(DATA_DIR / "projects.csv")

    active_projects = projects[
        projects["status"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("in progress")
    ]

    inactive_manager = (
        df["status"].fillna("").astype(str)
        .str.strip().str.lower().eq("inactive")
        & df["employee_id"].isin(
            active_projects["project_manager_id"]
        )
    )

    issues["Inactive active-project manager"] = int(
        inactive_manager.sum()
    )

    logger.info(
        "Inactive managers on active projects: "
        f"{inactive_manager.sum()}"
    )

    # 7. Record which employees require review
    df["dq_needs_review"] = (
        email_missing
        | invalid_dates
        | invalid_experience
        | invalid_salary
        | suspicious_salary
        | duplicate_ids
        | inactive_manager
    )

    # Save cleaned dataset
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(
        output_dir / "employees_clean.csv",
        index=False
    )

    # Save issue counts
    report_dir = (
        output_dir / "results"
        / "mohamed-alqubaisi"
        / "01_foundations"
    )
    report_dir.mkdir(parents=True, exist_ok=True)

    summary = pd.DataFrame(
        issues.items(),
        columns=["issue_type", "affected_rows"]
    )

    summary.to_csv(
        report_dir / "employee_dq_summary.csv",
        index=False
    )

    logger.info(
        f"Employees requiring review: "
        f"{df['dq_needs_review'].sum()}"
    )
    logger.info("Employee cleaning completed")

    return df, summary


if __name__ == "__main__":
    cleaned, summary = clean_employees()

    print("\n=== DATA QUALITY SUMMARY ===")
    print(summary.to_string(index=False))

    print("\nCleaned employees:", len(cleaned))
    print(
        "Employees requiring review:",
        cleaned["dq_needs_review"].sum()
    )
