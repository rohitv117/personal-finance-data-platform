{{
  config(
    materialized='incremental',
    unique_key='month'
  )
}}

WITH monthly_totals AS (
    SELECT 
        month,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT DATE(posted_at)) as active_days
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE month > (SELECT MAX(month) FROM {{ this }})
    {% endif %}
    GROUP BY month
),

monthly_balances AS (
    SELECT 
        month,
        SUM(amount) as balance_delta
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE month > (SELECT MAX(month) FROM {{ this }})
    {% endif %}
    GROUP BY month
),

monthly_averages AS (
    SELECT 
        month,
        AVG(CASE WHEN amount > 0 THEN amount ELSE NULL END) as avg_income,
        AVG(CASE WHEN amount < 0 THEN ABS(amount) ELSE NULL END) as avg_expense
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE month > (SELECT MAX(month) FROM {{ this }})
    {% endif %}
    GROUP BY month
)

SELECT 
    mt.month,
    mt.income,
    mt.expenses,
    mt.transaction_count,
    mt.active_days,
    CASE 
        WHEN mt.income > 0 THEN 
            ROUND((mt.income - mt.expenses) / mt.income, 4)
        ELSE 0 
    END as savings_rate,
    mb.balance_delta,
    ma.avg_income,
    ma.avg_expense,
    CURRENT_TIMESTAMP as created_at
FROM monthly_totals mt
LEFT JOIN monthly_balances mb ON mt.month = mb.month
LEFT JOIN monthly_averages ma ON mt.month = ma.month
ORDER BY mt.month 