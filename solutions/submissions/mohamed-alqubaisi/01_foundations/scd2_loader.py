
import pandas as pd
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


def build_employee_history():
    """Create non-overlapping SCD Type 2 employee versions."""

    employees = pd.read_csv(ROOT / "datasets/employees.csv")
    history = pd.read_csv(
        ROOT / "datasets/employees_salary_history.csv"
    )

    employees["hire_date"] = pd.to_datetime(
        employees["hire_date"], errors="coerce"
    )

    history["effective_date"] = pd.to_datetime(
        history["effective_date"], errors="coerce"
    )

    if employees["employee_id"].duplicated().any():
        raise ValueError("Duplicate current employee IDs")

    # Preserve source order for consecutive same-day changes.
    history["source_order"] = range(len(history))

    history = history.sort_values(
        ["employee_id", "effective_date", "source_order"],
        kind="stable"
    )

    # The final same-day event represents the end-of-day state.
    same_day = history.duplicated(
        ["employee_id", "effective_date"]
    ).sum()
    print("Same-day changes consolidated:", same_day)

    history = history.drop_duplicates(
        ["employee_id", "effective_date"], keep="last"
    ).copy()

    # Construct versions using the state after each change.
    versions = history.rename(columns={
        "new_salary": "salary",
        "new_role": "role",
        "new_level": "level",
        "effective_date": "valid_from"
    })[
        ["employee_id", "salary", "role",
         "level", "valid_from", "change_reason"]
    ].copy()

    # Include the initial state when history starts after hiring.
    first = history.groupby("employee_id", sort=False).head(1)
    first = first.merge(
        employees[["employee_id", "hire_date"]],
        on="employee_id",
        how="left"
    )

    needs_initial = (
        first["hire_date"].notna()
        & (first["hire_date"] < first["effective_date"])
        & first["previous_salary"].notna()
        & first["previous_role"].notna()
        & first["previous_level"].notna()
    )

    initial = first.loc[needs_initial].rename(columns={
        "previous_salary": "salary",
        "previous_role": "role",
        "previous_level": "level",
        "hire_date": "valid_from"
    })[
        ["employee_id", "salary", "role",
         "level", "valid_from"]
    ].copy()
    initial["change_reason"] = "Initial known state"

    # Employees without history have one current version.
    no_history = employees.loc[
        ~employees["employee_id"].isin(history["employee_id"])
    ].copy()

    no_history = no_history.rename(
        columns={"hire_date": "valid_from"}
    )[
        ["employee_id", "salary", "role",
         "level", "valid_from"]
    ]
    no_history["change_reason"] = "Current state only"

    versions = pd.concat(
        [versions, initial, no_history],
        ignore_index=True
    )

    
    # Detect missing or invalid effective dates
    invalid_dates = versions["valid_from"].isna()

    print(
        "Employee versions with invalid dates:",
        invalid_dates.sum()
    )

    # Save a data quality report before applying the fallback
    if invalid_dates.any():
        output_dir = ROOT / "outputs" / "results" / "mohamed-alqubaisi" / "01_foundations"
        output_dir.mkdir(parents=True, exist_ok=True)

        invalid_records = versions.loc[
            invalid_dates, ["employee_id", "valid_from"]
        ].copy()

        invalid_records["issue_type"] = "Invalid hire date"
        invalid_records["resolution"] = (
            "Technical fallback: 1900-01-01"
        )

        invalid_records.to_csv(
            output_dir / "invalid_employee_dates.csv",
            index=False
        )

        # Technical fallback only, not an actual hire date
        versions.loc[
            invalid_dates, "valid_from"
        ] = pd.Timestamp("1900-01-01")


    versions = versions.sort_values(
        ["employee_id", "valid_from"]
    ).reset_index(drop=True)

    if versions.duplicated(
        ["employee_id", "valid_from"]
    ).any():
        raise ValueError("Duplicate version start dates")

    # Half-open periods: valid_from <= date < valid_to.
    versions["valid_to"] = (
        versions.groupby("employee_id")["valid_from"].shift(-1)
    )

    versions["is_current"] = versions["valid_to"].isna()

    versions["valid_to"] = versions["valid_to"].apply(
        lambda d: "9999-12-31"
        if pd.isna(d) else d.date().isoformat()
    )

    versions["valid_from"] = (
        versions["valid_from"].dt.strftime("%Y-%m-%d")
    )

    # Add descriptive employee attributes.
    descriptions = employees[
        ["employee_id", "full_name", "email",
         "department", "hire_date"]
    ].copy()

    descriptions["hire_date"] = (
        descriptions["hire_date"].dt.strftime("%Y-%m-%d")
    )

    versions = versions.merge(
        descriptions, on="employee_id", how="left",
        validate="many_to_one"
    )

    versions.insert(
        0, "employee_key", range(1, len(versions) + 1)
    )

    # Validate exactly one current version per employee.
    current_counts = (
        versions.loc[versions["is_current"]]
        .groupby("employee_id").size()
    )

    assert len(current_counts) == len(employees)
    assert current_counts.eq(1).all()

    print("Total employee versions:", len(versions))
    print("Current records:", versions["is_current"].sum())
    print("Historical records:", (~versions["is_current"]).sum())

    return versions


def load_into_duckdb(versions):
    """Replace employee dimension rows in a transaction."""
    con = duckdb.connect(str(ROOT / "assessment.duckdb"))

    try:
        con.execute(
            "ALTER TABLE dim_employee "
            "ADD COLUMN IF NOT EXISTS level VARCHAR(50)"
        )

        columns = [
            "employee_key", "employee_id", "full_name",
            "email", "department", "role", "level",
            "salary", "hire_date", "valid_from",
            "valid_to", "is_current", "change_reason"
        ]

        con.register("employee_versions", versions[columns])

        con.execute("BEGIN TRANSACTION")
        con.execute("DELETE FROM dim_employee")

        con.execute("""
            INSERT INTO dim_employee (
                employee_key, employee_id, full_name,
                email, department, role, level,
                salary, hire_date, valid_from,
                valid_to, is_current, change_reason
            )
            SELECT
                employee_key, employee_id, full_name,
                email, department, role, level,
                salary, hire_date, valid_from,
                valid_to, is_current, change_reason
            FROM employee_versions
        """)

        con.execute("COMMIT")
        print("Employee dimension loaded successfully")

    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    employee_versions = build_employee_history()
    load_into_duckdb(employee_versions)
