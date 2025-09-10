{{
  config(
    materialized='incremental',
    unique_key='date'
  )
}}

WITH daily_totals AS (
    SELECT 
        DATE(posted_at) as date,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses,
        COUNT(*) as transaction_count
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE DATE(posted_at) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY DATE(posted_at)
),

daily_balances AS (
    SELECT 
        DATE(posted_at) as date,
        SUM(amount) as balance_delta
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE DATE(posted_at) > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    GROUP BY DATE(posted_at)
)

SELECT 
    dt.date,
    dt.income,
    dt.expenses,
    dt.transaction_count,
    CASE 
        WHEN dt.income > 0 THEN 
            ROUND((dt.income - dt.expenses) / dt.income, 4)
        ELSE 0 
    END as savings_rate,
    db.balance_delta,
    CURRENT_TIMESTAMP as created_at
FROM daily_totals dt
LEFT JOIN daily_balances db ON dt.date = db.date
ORDER BY dt.date 