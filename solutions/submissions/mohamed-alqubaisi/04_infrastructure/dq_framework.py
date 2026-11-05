
import logging
from pathlib import Path

import pandas as pd

from dq_config import DQ_CONFIG

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[4]
DATA_DIR = ROOT / "datasets"
OUTPUT_DIR = ROOT / "outputs"

CHECK_NAMES = [
    "completeness",
    "uniqueness",
    "validity_numeric",
    "validity_date",
    "consistency",
    "referential_integrity",
]


def is_missing(series):
    """Identify null, empty and whitespace-only values."""
    return (
        series.isna()
        | series.astype("string").str.strip().eq("").fillna(False)
    )


def load_reference(dataset_name, reference_data):
    """Use supplied reference data, or load the raw dataset."""
    if dataset_name in reference_data:
        return reference_data[dataset_name]

    path = DATA_DIR / f"{dataset_name}.csv"
    if path.exists():
        return pd.read_csv(path, dtype="string")

    raise FileNotFoundError(
        f"Reference dataset not found: {dataset_name}"
    )


def run_data_quality_checks(
    df: pd.DataFrame,
    dataset_name: str,
    reference_data=None,
) -> dict:
    """
    Run six configurable data quality checks.

    To extend rules, edit dq_config.py rather than this file.
    """
    if dataset_name not in DQ_CONFIG:
        raise ValueError(f"No DQ configuration: {dataset_name}")

    config = DQ_CONFIG[dataset_name]
    reference_data = reference_data or {}
    results = {}

    def record(name, failures, details):
        status = "FAIL" if failures else "PASS"
        results[name] = {
            "status": status,
            "details": details,
        }
        if failures:
            logger.warning(
                "%s | %s FAILED: %s",
                dataset_name, name, failures
            )

    # 1. COMPLETENESS
    threshold = config["completeness_threshold"]
    completeness = {}
    failed_columns = []

    
    for col in df.columns:
        required_mask = pd.Series(True, index=df.index)

        conditional = config.get(
            "conditional_required", {}
        ).get(col)

        if conditional:
            for condition_col, excluded_values in (
                conditional["except_when"].items()
            ):
                if condition_col not in df.columns:
                    raise ValueError(
                        f"Missing condition column: {condition_col}"
                    )

                required_mask &= ~df[condition_col].isin(
                    excluded_values
                )

        required_count = int(required_mask.sum())
        valid_count = int(
            (required_mask & ~is_missing(df[col])).sum()
        )

        ratio = (
            valid_count / required_count
            if required_count else 1.0
        )

        completeness[col] = round(float(ratio), 4)

        if ratio < threshold:
            failed_columns.append(col)


    record(
        "completeness",
        failed_columns,
        {
            "threshold": threshold,
            "columns": completeness,
            "failed_columns": failed_columns,
        },
    )

    # 2. UNIQUENESS
    pk_columns = config.get("pk_columns", [])
    uniqueness_issues = []

    if not pk_columns:
        uniqueness_issues.append(
            "No primary key configured"
        )
    else:
        missing_cols = [
            col for col in pk_columns if col not in df.columns
        ]
        uniqueness_issues.extend(
            f"Missing PK column: {col}" for col in missing_cols
        )

        if not missing_cols:
            missing_keys = df[pk_columns].isna().any(axis=1)
            duplicates = df.duplicated(
                subset=pk_columns, keep=False
            )
            uniqueness_issues.extend([
                f"Missing PK rows: {int(missing_keys.sum())}"
                if missing_keys.any() else "",
                f"Duplicate PK rows: {int(duplicates.sum())}"
                if duplicates.any() else "",
            ])
            uniqueness_issues = [
                item for item in uniqueness_issues if item
            ]

    record("uniqueness", uniqueness_issues, {
        "primary_key": pk_columns,
        "issues": uniqueness_issues,
    })

    # 3. NUMERIC VALIDITY
    numeric_issues = []

    for col, limits in config.get(
        "numeric_ranges", {}
    ).items():
        if col not in df.columns:
            numeric_issues.append(f"Missing column: {col}")
            continue

        original = df[col]
        numeric = pd.to_numeric(original, errors="coerce")
        present = ~is_missing(original)

        invalid = present & numeric.isna()
        outside = present & (
            numeric.lt(limits["min"])
            | numeric.gt(limits["max"])
        )

        if invalid.any() or outside.any():
            numeric_issues.append(
                f"{col}: {int(invalid.sum())} non-numeric, "
                f"{int(outside.sum())} outside range"
            )

    record(
        "validity_numeric",
        numeric_issues,
        numeric_issues or "All configured numeric ranges valid",
    )

    # 4. DATE VALIDITY
    date_issues = []

    for col, rules in config.get(
        "date_columns", {}
    ).items():
        if col not in df.columns:
            date_issues.append(f"Missing column: {col}")
            continue

        present = ~is_missing(df[col])
        dates = pd.to_datetime(df[col], errors="coerce", utc=True)

        invalid = present & dates.isna()
        future = (
            present
            & dates.gt(pd.Timestamp.now(tz="UTC"))
            if not rules.get("allow_future", False)
            else pd.Series(False, index=df.index)
        )

        if invalid.any() or future.any():
            date_issues.append(
                f"{col}: {int(invalid.sum())} invalid, "
                f"{int(future.sum())} unexpected future dates"
            )

    record(
        "validity_date",
        date_issues,
        date_issues or "All configured dates valid",
    )

    # 5. CROSS-COLUMN CONSISTENCY
    consistency_issues = []

    for rule in config.get("consistency_rules", []):
        rule_type = rule["type"]

        if rule_type == "before":
            left, right = rule["columns"]
            if left not in df or right not in df:
                consistency_issues.append(
                    f"Missing date columns: {left}, {right}"
                )
                continue

            start = pd.to_datetime(
                df[left], errors="coerce", utc=True
            )
            end = pd.to_datetime(
                df[right], errors="coerce", utc=True
            )

            # Only compare when both dates are present and valid.
            bad = start.notna() & end.notna() & start.ge(end)

            if bad.any():
                consistency_issues.append(
                    f"{left} must be before {right}: "
                    f"{int(bad.sum())} failures"
                )

        elif rule_type == "non_negative":
            col = rule["column"]
            if col not in df:
                consistency_issues.append(
                    f"Missing consistency column: {col}"
                )
                continue

            values = pd.to_numeric(df[col], errors="coerce")
            bad = values.lt(0)

            if bad.any():
                consistency_issues.append(
                    f"{col}: {int(bad.sum())} negative values"
                )
        else:
            consistency_issues.append(
                f"Unsupported consistency rule: {rule_type}"
            )

    record(
        "consistency",
        consistency_issues,
        consistency_issues or "All configured consistency rules valid",
    )

    # 6. REFERENTIAL INTEGRITY
    fk_issues = []

    for col, (ref_name, ref_col) in config.get(
        "foreign_keys", {}
    ).items():
        if col not in df.columns:
            fk_issues.append(f"Missing foreign key: {col}")
            continue

        try:
            reference = (
                df if ref_name == dataset_name
                else load_reference(ref_name, reference_data)
            )
        except FileNotFoundError as error:
            fk_issues.append(str(error))
            continue

        if ref_col not in reference.columns:
            fk_issues.append(
                f"Missing reference column: {ref_name}.{ref_col}"
            )
            continue

        values = df[col].astype("string").str.strip()
        valid_values = set(
            reference[ref_col]
            .dropna()
            .astype("string")
            .str.strip()
        )

        # Null optional FKs are handled by completeness.
        present = ~is_missing(df[col])
        invalid = present & ~values.isin(valid_values)

        if invalid.any():
            fk_issues.append(
                f"{col}: {int(invalid.sum())} unmatched references "
                f"in {ref_name}.{ref_col}"
            )

    record(
        "referential_integrity",
        fk_issues,
        fk_issues or "All configured foreign keys valid",
    )

    # SUMMARY
    checks_passed = sum(
        result["status"] == "PASS"
        for result in results.values()
    )

    summary = {
        "dataset_name": dataset_name,
        "checks_run": len(results),
        "checks_passed": checks_passed,
        "checks_failed": len(results) - checks_passed,
        "results": results,
    }

    # MARKDOWN REPORT
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / f"dq_report_{dataset_name}.md"

    lines = [
        f"# Data Quality Report: {dataset_name}",
        "",
        f"**Rows:** {len(df)}",
        f"**Checks run:** {summary['checks_run']}",
        f"**Passed:** {summary['checks_passed']}",
        f"**Failed:** {summary['checks_failed']}",
        "",
        "| Check | Status |",
        "|---|---|",
    ]

    for name, result in results.items():
        lines.append(f"| {name} | {result['status']} |")

    lines.extend(["", "## Detailed Results", ""])

    for name, result in results.items():
        lines.extend([
            f"### {name}",
            f"Status: **{result['status']}**",
            "",
            "```text",
            str(result["details"]),
            "```",
            "",
        ])

    report_path.write_text(
        "\n".join(lines), encoding="utf-8"
    )

    logger.info("DQ report saved: %s", report_path)

    return summary
