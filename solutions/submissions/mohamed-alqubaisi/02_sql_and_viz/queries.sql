
-- =====================================================
-- TASK 2.1 - SQL BUSINESS QUESTIONS
-- Q1: Department Budget Performance
-- =====================================================

-- Approach:
-- Aggregate project budgets and actual costs by department.
-- Calculate spending percentage against total budget.
-- Include departments above 90% utilisation.
-- Flag departments where spending exceeds the budget.

SELECT
    department,

    SUM(budget) AS total_budget,

    SUM(actual_cost) AS total_actual_cost,

    ROUND(
        SUM(actual_cost) * 100.0
        / NULLIF(SUM(budget), 0),
        2
    ) AS spend_percentage,

    SUM(actual_cost) > SUM(budget) AS over_budget

FROM dim_project

GROUP BY department

HAVING
    SUM(actual_cost) * 100.0
    / NULLIF(SUM(budget), 0) > 90

ORDER BY spend_percentage DESC;


-- =====================================================
-- Q2: PROJECT MANAGER WORKLOAD
-- =====================================================

-- Approach:
-- Join active projects to current employee records.
-- Count projects managed by each employee.
-- Calculate combined budget and actual spending.
-- Return managers overseeing more than 3 projects.

SELECT
    e.full_name,
    e.email,

    COUNT(p.project_id) AS active_project_count,

    SUM(p.budget) AS combined_budget_responsibility,

    SUM(p.actual_cost) AS combined_actual_spend

FROM dim_project p

JOIN dim_employee e
    ON p.project_manager_id = e.employee_id
    AND e.is_current = TRUE

WHERE p.status_category = 'Active'

GROUP BY
    e.employee_id,
    e.full_name,
    e.email

HAVING COUNT(p.project_id) > 3

ORDER BY active_project_count DESC;


-- =====================================================
-- Q3: VENDOR CONCENTRATION RISK
-- =====================================================

-- Approach:
-- Aggregate transaction spending by vendor.
-- Compare each vendor's spend against total spending.
-- NULL amounts are excluded from SUM, not treated
-- as confirmed zero-value transactions.
-- HIGH risk: greater than 10% of total spending.
-- MEDIUM risk: greater than 5% up to 10%.

WITH vendor_spending AS (
    SELECT
        v.vendor_key,
        v.vendor_name,
        SUM(f.amount) AS total_spend,
        COUNT(*) AS transaction_count

    FROM fact_transactions f

    JOIN dim_vendor v
        ON f.vendor_key = v.vendor_key

    GROUP BY
        v.vendor_key,
        v.vendor_name
),

vendor_shares AS (
    SELECT
        vendor_name,
        total_spend,
        transaction_count,

        100.0 * total_spend
            / NULLIF(SUM(total_spend) OVER (), 0)
            AS percentage_of_total_spend

    FROM vendor_spending
)

SELECT
    vendor_name,
    total_spend,
    transaction_count,

    ROUND(
        percentage_of_total_spend, 2
    ) AS percentage_of_total_spend,

    CASE
        WHEN percentage_of_total_spend > 10
            THEN 'HIGH'
        WHEN percentage_of_total_spend > 5
            THEN 'MEDIUM'
        ELSE 'NORMAL'
    END AS risk_flag

FROM vendor_shares

WHERE percentage_of_total_spend > 5

ORDER BY percentage_of_total_spend DESC;


-- =====================================================
-- Q4: PROJECTS WITH OPEN FINANCIAL ISSUES
-- =====================================================

-- Approach:
-- Join transactions to their projects.
-- Include Pending and Disputed transactions.
-- Count every open transaction, including those
-- with NULL amounts.
-- SUM excludes unknown amounts from confirmed value.
-- Return projects with open transaction value > 50000 AED.

SELECT
    p.project_id,
    p.project_name,
    p.department,
    p.status AS project_status,

    COUNT(*) AS open_transaction_count,

    SUM(f.amount) AS open_transaction_value

FROM fact_transactions f

JOIN dim_project p
    ON f.project_key = p.project_key

WHERE f.payment_status IN ('Pending', 'Disputed')

GROUP BY
    p.project_id,
    p.project_name,
    p.department,
    p.status

HAVING SUM(f.amount) > 50000

ORDER BY open_transaction_value DESC;


-- =====================================================
-- Q5: MONTHLY SPEND TREND
-- =====================================================

-- Approach:
-- Aggregate transactions by month and category.
-- Use SUM OVER to calculate cumulative spending.
-- Use LAG to compare spending with the previous month.
-- NULL amounts are excluded from total spending.

WITH monthly_spending AS (
    SELECT
        strftime(d.full_date, '%Y-%m') AS year_month,
        f.category,
        SUM(f.amount) AS monthly_spend

    FROM fact_transactions f

    JOIN dim_date d
        ON f.date_key = d.date_key

    GROUP BY
        strftime(d.full_date, '%Y-%m'),
        f.category
),

monthly_analysis AS (
    SELECT
        year_month,
        category,
        monthly_spend,

        SUM(monthly_spend) OVER (
            PARTITION BY category
            ORDER BY year_month
            ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS running_total,

        LAG(monthly_spend) OVER (
            PARTITION BY category
            ORDER BY year_month
        ) AS previous_month_spend

    FROM monthly_spending
)

SELECT
    year_month,
    category,
    monthly_spend,
    running_total,

    ROUND(
        (monthly_spend - previous_month_spend)
        * 100.0
        / NULLIF(previous_month_spend, 0),
        2
    ) AS month_over_month_pct_change

FROM monthly_analysis

ORDER BY category, year_month;


-- =====================================================
-- Q6: EMPLOYEE COMPENSATION HISTORY
-- =====================================================

-- Approach:
-- Use SCD Type 2 historical employee versions.
-- Join each newer version with the preceding version.
-- Match the previous valid_to with the new valid_from.
-- Calculate absolute and percentage salary increases.
-- Return the 20 largest positive salary increases.

SELECT
    current_version.employee_id,
    current_version.full_name,

    current_version.valid_from AS change_date,

    previous_version.salary AS previous_salary,

    current_version.salary AS new_salary,

    (
        current_version.salary - previous_version.salary
    ) AS increase_amount,

    ROUND(
        (
            current_version.salary - previous_version.salary
        ) * 100.0
        / NULLIF(previous_version.salary, 0),
        2
    ) AS increase_pct

FROM dim_employee current_version

JOIN dim_employee previous_version
    ON current_version.employee_id =
       previous_version.employee_id

    AND previous_version.valid_to =
        current_version.valid_from

WHERE current_version.salary >
      previous_version.salary

ORDER BY increase_amount DESC

LIMIT 20;
