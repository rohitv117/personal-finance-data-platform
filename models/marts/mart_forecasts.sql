{{
  config(
    materialized='incremental',
    unique_key="forecast_date || '_' || category_id::string"
  )
}}

WITH monthly_category_totals AS (
    SELECT 
        month,
        category_norm,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses,
        COUNT(*) as transaction_count
    FROM {{ ref('stg_transactions') }}
    WHERE amount < 0  -- Only expenses for forecasting
    {% if is_incremental() %}
        AND month > (SELECT MAX(month) FROM (
            SELECT TO_CHAR(forecast_date, 'YYYY-MM') as month 
            FROM {{ this }}
        ) sub)
    {% endif %}
    GROUP BY month, category_norm
    HAVING COUNT(*) >= 3  -- Need at least 3 months of data
),

category_growth_rates AS (
    SELECT 
        category_norm,
        AVG(expenses) as avg_monthly_expense,
        -- Simple linear growth rate calculation
        CASE 
            WHEN COUNT(*) > 1 THEN
                (MAX(expenses) - MIN(expenses)) / NULLIF(COUNT(*) - 1, 0)
            ELSE 0 
        END as monthly_growth_rate,
        STDDEV(expenses) as expense_volatility
    FROM monthly_category_totals
    GROUP BY category_norm
),

forecast_periods AS (
    SELECT 
        generate_series(
            CURRENT_DATE + INTERVAL '1 month',
            CURRENT_DATE + INTERVAL '3 months',
            INTERVAL '1 month'
        )::date as forecast_date
),

forecast_calculations AS (
    SELECT 
        fp.forecast_date,
        cg.category_norm,
        cg.avg_monthly_expense,
        cg.monthly_growth_rate,
        cg.expense_volatility,
        -- Calculate months ahead for growth projection
        EXTRACT(MONTH FROM AGE(fp.forecast_date, CURRENT_DATE)) as months_ahead,
        -- Projected amount with growth
        cg.avg_monthly_expense + (cg.monthly_growth_rate * EXTRACT(MONTH FROM AGE(fp.forecast_date, CURRENT_DATE))) as projected_amount
    FROM forecast_periods fp
    CROSS JOIN category_growth_rates cg
)

SELECT 
    fc.forecast_date,
    c.category_id,
    c.name as category_name,
    c.budget_group,
    ROUND(fc.projected_amount, 2) as forecast_amount,
    ROUND(GREATEST(fc.projected_amount - fc.expense_volatility, 0), 2) as lower_bound,
    ROUND(fc.projected_amount + fc.expense_volatility, 2) as upper_bound,
    CASE 
        WHEN fc.expense_volatility < fc.avg_monthly_expense * 0.1 THEN 0.95
        WHEN fc.expense_volatility < fc.avg_monthly_expense * 0.25 THEN 0.80
        ELSE 0.65
    END as confidence_level,
    fc.avg_monthly_expense,
    fc.monthly_growth_rate,
    fc.expense_volatility,
    CURRENT_TIMESTAMP as created_at
FROM forecast_calculations fc
JOIN {{ ref('categories') }} c ON fc.category_norm = c.name
WHERE c.is_income = FALSE  -- Only forecast expenses
ORDER BY fc.forecast_date, c.budget_group, c.name 