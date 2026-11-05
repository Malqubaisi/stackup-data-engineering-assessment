
import pandas as pd
from pathlib import Path

# Locate the main project folder
ROOT = Path(__file__).resolve().parents[4]

import os

DATA_DIR = Path(os.environ.get("DATA_DIR", str(ROOT / "datasets")))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(ROOT / "outputs")))

# Step 1: Load the projects dataset
def load_projects():
    file_path = DATA_DIR / "projects.csv"

    df = pd.read_csv(
        file_path,
        parse_dates=["start_date", "end_date"]
    )

    print(f"Loaded {len(df)} projects")

    return df


# Step 2: Transform the projects dataset

def transform_projects(df):
    """Clean and transform project data."""

    df = df.copy()

    # Keep track of missing financial values
    missing_financials = (
        df["budget"].isna() |
        df["actual_cost"].isna()
    )

    # Replace missing financial values with zero
    df["budget"] = df["budget"].fillna(0)
    df["actual_cost"] = df["actual_cost"].fillna(0)

    # 1. Budget variance
    df["budget_variance"] = df["actual_cost"] - df["budget"]

    # 2. Is the project over budget?
    df["is_over_budget"] = (
        (df["actual_cost"] > df["budget"]) &
        ~missing_financials
    )

    # 3. Project duration in days
    df["duration_days"] = (
        df["end_date"] - df["start_date"]
    ).dt.days

    # 4. Percentage of budget used
    df["budget_utilisation_pct"] = (
        df["actual_cost"]
        .div(df["budget"].replace(0, float("nan")))
        .mul(100)
    )
    
    # 5. Standardise project status
    df["status"] = (
        df["status"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    # Map individual statuses to broader categories
    status_mapping = {
        "in progress": "Active",
        "active": "Active",
        "completed": "Closed",
        "cancelled": "Closed",
        "canceled": "Closed",
        "on hold": "Pending",
        "planned": "Pending",
        "pending": "Pending",
        "not started": "Pending"
    }

    df["status_category"] = df["status"].map(status_mapping)

    # Flag unknown statuses for investigation
    unknown_status = (
        df["status"].notna() &
        df["status_category"].isna()
    )

    if unknown_status.any():
        print(
            "Unmapped statuses:",
            df.loc[unknown_status, "status"].unique()
        )

    # 6. Standardise priority
    df["priority"] = (
        df["priority"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    # 7. Calculate project risk levels
    high_risk = (
        df["priority"].eq("critical").fillna(False) |
        df["is_over_budget"]
    )

    medium_risk = (
        df["priority"].eq("high").fillna(False) |
        df["budget_utilisation_pct"].gt(90).fillna(False)
    )

    df["risk_level"] = "Low"
    df.loc[medium_risk, "risk_level"] = "Medium"
    df.loc[high_risk, "risk_level"] = "High"

    return df



# Step 3: Run the functions


if __name__ == "__main__":
    projects = load_projects()
    projects_clean = transform_projects(projects)

    # Save the completed dataset
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "projects_clean.csv"
    projects_clean.to_csv(output_path, index=False)

    print(f"Saved cleaned projects to: {output_path}")
    print(f"Rows: {len(projects_clean)}")
    print(f"Columns: {len(projects_clean.columns)}")

    print(
        projects_clean[
            [
                "project_id",
                "status_category",
                "budget_variance",
                "budget_utilisation_pct",
                "risk_level"
            ]
        ].head(10).to_string(index=False)
    )



