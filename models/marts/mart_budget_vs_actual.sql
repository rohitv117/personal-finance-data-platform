{{
  config(
    materialized='incremental',
    unique_key="month || '_' || category_id::string"
  )
}}

WITH category_totals AS (
    SELECT 
        month,
        category_norm,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE month > (SELECT MAX(month) FROM {{ this }})
    {% endif %}
    GROUP BY month, category_norm
),

category_budgets AS (
    SELECT 
        c.category_id,
        c.name as category_name,
        c.budget_group,
        -- Simple budget allocation based on category type
        CASE 
            WHEN c.budget_group = 'Essential' THEN 2000.00
            WHEN c.budget_group = 'Discretionary' THEN 1000.00
            WHEN c.is_income THEN 0.00
            ELSE 500.00
        END as monthly_budget
    FROM {{ ref('categories') }} c
),

monthly_actuals AS (
    SELECT 
        ct.month,
        ct.category_norm,
        ct.income,
        ct.expenses,
        COALESCE(ct.income, 0) - COALESCE(ct.expenses, 0) as net_amount
    FROM category_totals ct
)

SELECT 
    ma.month,
    cb.category_id,
    cb.category_name,
    cb.budget_group,
    cb.monthly_budget as budget,
    ma.net_amount as actual,
    cb.monthly_budget - ma.net_amount as variance,
    CASE 
        WHEN cb.monthly_budget > 0 THEN 
            ROUND((cb.monthly_budget - ma.net_amount) / cb.monthly_budget, 4)
        ELSE 0 
    END as variance_pct,
    ma.income,
    ma.expenses,
    CURRENT_TIMESTAMP as created_at
FROM monthly_actuals ma
JOIN category_budgets cb ON ma.category_norm = cb.category_name
WHERE cb.monthly_budget > 0  -- Only show categories with budgets
ORDER BY ma.month, cb.budget_group, cb.category_name 