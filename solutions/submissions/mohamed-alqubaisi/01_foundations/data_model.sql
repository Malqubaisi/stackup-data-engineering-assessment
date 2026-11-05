
-- =====================================================
-- TASK 1.2 - PRESIGHT DATA WAREHOUSE
-- STAR SCHEMA DESIGN
-- =====================================================

-- DIM_DATE
-- Stores calendar attributes for transaction reporting.
-- date_key uses YYYYMMDD as an integer surrogate key.
-- Calendar attributes support monthly and yearly analysis.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week INTEGER,
    day INTEGER NOT NULL,
    day_of_week INTEGER,
    is_weekend BOOLEAN
);


-- =====================================================
-- DIM_PROJECT
-- =====================================================
-- Stores descriptive information about projects.
-- project_key is the surrogate primary key.
-- project_id preserves the original source system ID.
-- Each project has one row in this dimension.

CREATE TABLE IF NOT EXISTS dim_project (
    project_key INTEGER PRIMARY KEY,
    project_id VARCHAR(20) NOT NULL UNIQUE,
    project_name VARCHAR(255),
    department VARCHAR(100),
    status VARCHAR(50),
    status_category VARCHAR(20),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(18,2),
    actual_cost DECIMAL(18,2),
    budget_variance DECIMAL(18,2),
    is_over_budget BOOLEAN,
    duration_days INTEGER,
    budget_utilisation_pct DOUBLE,
    project_manager_id VARCHAR(20),
    priority VARCHAR(20),
    region VARCHAR(100),
    risk_level VARCHAR(20)
);


-- =====================================================
-- DIM_EMPLOYEE - SCD TYPE 2
-- =====================================================
-- Preserves historical employee salary and role changes.
-- employee_key uniquely identifies each historical version.
-- employee_id preserves the original source identifier.
-- valid_from and valid_to define the effective period.
-- is_current identifies the latest employee version.
-- Historical periods use [valid_from, valid_to).

CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key INTEGER PRIMARY KEY,

    employee_id VARCHAR(20) NOT NULL,
    full_name VARCHAR(255),
    email VARCHAR(255),
    department VARCHAR(100),
    role VARCHAR(100),
    level VARCHAR(50),
    salary DECIMAL(18,2),
    hire_date DATE,

    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT DATE '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    change_reason VARCHAR(255),

    CONSTRAINT valid_employee_period
        CHECK (valid_from < valid_to)
);

-- Employee lookups by natural key and effective period.
CREATE INDEX IF NOT EXISTS idx_employee_history
ON dim_employee(employee_id, valid_from);


-- =====================================================
-- DIM_VENDOR
-- =====================================================
-- Stores vendor information from transaction records.
-- vendor_key is the warehouse surrogate primary key.
-- vendor_id preserves the source system identifier.
-- Each vendor appears once in this dimension.

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key INTEGER PRIMARY KEY,
    vendor_id VARCHAR(50) NOT NULL UNIQUE,
    vendor_name VARCHAR(255),
    vendor_category VARCHAR(100),
    vendor_region VARCHAR(100)
);


-- =====================================================
-- BRIDGE_EMPLOYEE_PROJECT
-- =====================================================
-- Handles the many-to-many relationship between
-- employees and projects.
-- One employee may work on multiple projects.
-- One project may have multiple employees.
-- employee_key references a specific SCD2 version.

CREATE TABLE IF NOT EXISTS bridge_employee_project (
    employee_key INTEGER NOT NULL,
    project_key INTEGER NOT NULL,

    assignment_role VARCHAR(100),
    assigned_from DATE,
    assigned_to DATE,

    PRIMARY KEY (employee_key, project_key),

    FOREIGN KEY (employee_key)
        REFERENCES dim_employee(employee_key),

    FOREIGN KEY (project_key)
        REFERENCES dim_project(project_key),

    CHECK (
        assigned_to IS NULL
        OR assigned_from IS NULL
        OR assigned_to >= assigned_from
    )
);


-- =====================================================
-- FACT_TRANSACTIONS
-- =====================================================
-- Central fact table for financial transactions.
-- Each row represents one transaction.
-- Surrogate foreign keys connect to dimensions.
-- employee_key identifies the employee version
-- valid at the transaction date.

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key INTEGER PRIMARY KEY,

    transaction_id VARCHAR(50) NOT NULL UNIQUE,

    project_key INTEGER NOT NULL,
    employee_key INTEGER,
    vendor_key INTEGER,
    date_key INTEGER NOT NULL,

    amount DECIMAL(18,2),
    currency VARCHAR(3),
    category VARCHAR(100),
    payment_status VARCHAR(50),

    FOREIGN KEY (project_key)
        REFERENCES dim_project(project_key),

    FOREIGN KEY (employee_key)
        REFERENCES dim_employee(employee_key),

    FOREIGN KEY (vendor_key)
        REFERENCES dim_vendor(vendor_key),

    FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key)
);


-- =====================================================
-- SCD TYPE 2 VALIDATION QUERIES
-- =====================================================

-- Q1: Each employee must have exactly one current record.
SELECT employee_id,
       COUNT(*) FILTER (WHERE is_current = TRUE)
           AS current_count
FROM dim_employee
GROUP BY employee_id
HAVING COUNT(*) FILTER (WHERE is_current = TRUE) <> 1;


-- Q2: Show employees with historical versions.
SELECT employee_id,
       COUNT(*) AS version_count
FROM dim_employee
GROUP BY employee_id
ORDER BY version_count DESC
LIMIT 10;


-- Q3: Detect overlapping employee history periods.
SELECT a.employee_id,
       a.employee_key AS version_a,
       b.employee_key AS version_b
FROM dim_employee a
JOIN dim_employee b
    ON a.employee_id = b.employee_id
   AND a.employee_key < b.employee_key
   AND a.valid_from < b.valid_to
   AND b.valid_from < a.valid_to;

