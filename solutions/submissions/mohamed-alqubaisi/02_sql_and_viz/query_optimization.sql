
-- =====================================================
-- TASK 2.3 - OPTIMIZED QUERY VERSION 2
-- =====================================================

-- Approach:
-- 1. Filter pending transactions early.
-- 2. Use a window function to calculate the average.
-- 3. Avoid a separate average aggregation query.
-- 4. Use explicit joins.
-- 5. Select only the required output columns.

WITH pending_transactions AS (
    SELECT
        project_key,
        date_key,
        amount,
        category,
        payment_status,

        AVG(amount) OVER () AS avg_pending_amount

    FROM fact_transactions

    WHERE payment_status = 'Pending'
),

filtered_transactions AS (
    SELECT
        project_key,
        date_key,
        amount,
        category,
        payment_status

    FROM pending_transactions

    WHERE amount > avg_pending_amount
)

SELECT
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    d.full_date AS transaction_date

FROM filtered_transactions t

JOIN dim_project p
    ON t.project_key = p.project_key

JOIN dim_employee e
    ON p.project_manager_id = e.employee_id
   AND e.is_current = TRUE

JOIN dim_date d
    ON t.date_key = d.date_key

WHERE LOWER(TRIM(p.status))
    NOT IN ('completed', 'on hold')

ORDER BY e.department, t.amount DESC;


-- =====================================================
-- INDEX RECOMMENDATIONS
-- =====================================================

-- Index 1: Transaction filtering and project joins.
-- Payment status is first because the query filters
-- on Pending transactions before joining projects.
-- Additional index storage and write overhead apply.
-- CREATE INDEX idx_fact_payment_project
-- ON fact_transactions(payment_status, project_key);

-- Index 2: Project manager lookup.
-- Supports joins between projects and employees.
-- Adds maintenance overhead when project assignments change.
-- CREATE INDEX idx_project_manager
-- ON dim_project(project_manager_id);

-- Index 3: Current employee lookup.
-- Supports employee ID and is_current filters.
-- Has additional maintenance cost during SCD2 updates.
-- CREATE INDEX idx_employee_current
-- ON dim_employee(employee_id, is_current);
