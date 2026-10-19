-- Catch dropped or duplicated transactions even when every column is non-null.
WITH expected AS (
    SELECT COUNT(*) AS n,
        COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS income,
        COALESCE(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 0) AS expenses
    FROM {{ source('raw', 'transactions') }}
), actual AS (
    SELECT 'daily' AS grain, COALESCE(SUM(total_transactions), 0) AS n,
        COALESCE(SUM(income), 0) AS income, COALESCE(SUM(expenses), 0) AS expenses
    FROM {{ ref('fct_cashflow_daily') }}
    UNION ALL
    SELECT 'monthly', COALESCE(SUM(total_transactions), 0),
        COALESCE(SUM(income), 0), COALESCE(SUM(expenses), 0)
    FROM {{ ref('fct_cashflow_monthly') }}
)
SELECT actual.* FROM actual CROSS JOIN expected
WHERE actual.n <> expected.n OR actual.income <> expected.income
    OR actual.expenses <> expected.expenses
