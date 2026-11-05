
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
DB_PATH = ROOT / "assessment.duckdb"

con = duckdb.connect(str(DB_PATH))

try:
    
    # Load JSON into a temporary staging view.
    # Use an explicit SQL path because CREATE VIEW
    # does not support prepared parameters here.

    json_path = (
        ROOT / "datasets" / "transactions.json"
    ).as_posix().replace("'", "''")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW staging_transactions AS
        SELECT *
        FROM read_json_auto('{json_path}')
    """)


    # Check vendor ID consistency.
    conflicts = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT vendor_id
            FROM staging_transactions
            GROUP BY vendor_id
            HAVING COUNT(DISTINCT vendor_name) > 1
        )
    """).fetchone()[0]

    if conflicts:
        raise ValueError(
            f"{conflicts} vendors have conflicting names"
        )

    # Check for duplicate transaction IDs.
    duplicates = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
        FROM staging_transactions
    """).fetchone()[0]

    if duplicates:
        raise ValueError(
            f"{duplicates} duplicate transaction IDs"
        )

    con.execute("BEGIN TRANSACTION")

    # Clear facts first to preserve foreign keys.
    con.execute("DELETE FROM fact_transactions")

    # Load vendor dimension.
    con.execute("DELETE FROM dim_vendor")
    con.execute("""
        INSERT INTO dim_vendor
            (vendor_key, vendor_id, vendor_name)
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY vendor_id)
                 AS INTEGER),
            vendor_id,
            MAX(vendor_name)
        FROM staging_transactions
        GROUP BY vendor_id
    """)

    # Load date dimension from transaction dates.
    con.execute("DELETE FROM dim_date")
    con.execute("""
        INSERT INTO dim_date
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER),
            d,
            CAST(year(d) AS INTEGER),
            CAST(quarter(d) AS INTEGER),
            CAST(month(d) AS INTEGER),
            strftime(d, '%B'),
            CAST(week(d) AS INTEGER),
            CAST(day(d) AS INTEGER),
            CAST(isodow(d) AS INTEGER),
            isodow(d) IN (6, 7)
        FROM (
            SELECT DISTINCT
                CAST(transaction_date AS DATE) AS d
            FROM staging_transactions
        )
    """)

    # Resolve employee SCD2 version at transaction date.
    # The join uses half-open effective periods.
    con.execute("""
        CREATE OR REPLACE TEMP VIEW resolved_transactions AS
        SELECT
            t.transaction_id,
            p.project_key,
            e.employee_key,
            v.vendor_key,
            CAST(
                strftime(
                    CAST(t.transaction_date AS DATE),
                    '%Y%m%d'
                ) AS INTEGER
            ) AS date_key,
            t.amount,
            t.currency,
            t.category,
            t.payment_status
        FROM staging_transactions t
        JOIN dim_project p
            ON t.project_id = p.project_id
        LEFT JOIN dim_vendor v
            ON t.vendor_id = v.vendor_id
        LEFT JOIN dim_employee e
            ON t.approved_by = e.employee_id
            AND CAST(t.transaction_date AS DATE)
                >= e.valid_from
            AND CAST(t.transaction_date AS DATE)
                < e.valid_to
    """)

    # Check for missing or ambiguous dimension matches.
    counts = con.execute("""
        SELECT
            (SELECT COUNT(*) FROM staging_transactions),
            (SELECT COUNT(*) FROM resolved_transactions)
    """).fetchone()

    if counts[0] != counts[1]:
        raise ValueError(
            "Transaction count changed during dimension joins"
        )

    # Load central fact table.
    con.execute("""
        INSERT INTO fact_transactions (
            transaction_key,
            transaction_id,
            project_key,
            employee_key,
            vendor_key,
            date_key,
            amount,
            currency,
            category,
            payment_status
        )
        SELECT
            CAST(
                ROW_NUMBER() OVER (ORDER BY transaction_id)
                AS INTEGER
            ),
            transaction_id,
            project_key,
            employee_key,
            vendor_key,
            date_key,
            amount,
            currency,
            category,
            payment_status
        FROM resolved_transactions
    """)

    con.execute("COMMIT")

    # Verify loaded record counts.
    for table in [
        "dim_vendor",
        "dim_date",
        "fact_transactions"
    ]:
        count = con.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        print(f"{table}: {count} rows")

    print("Transaction loading completed successfully")


except Exception as error:
    print(f"Original error: {error}")

    # Attempt rollback only if a transaction is active.
    try:
        con.execute("ROLLBACK")
    except duckdb.TransactionException:
        pass

    raise

finally:
    con.close()

