-- Budget vs actual fact table
-- Tracks budget variance by category and month

WITH monthly_expenses AS (
    SELECT 
        month,
        category_std,
        SUM(ABS(amount_usd)) as actual_expenses,
        COUNT(*) as transaction_count,
        AVG(ABS(amount_usd)) as avg_transaction_amount,
        MAX(ABS(amount_usd)) as max_transaction_amount,
        MIN(ABS(amount_usd)) as min_transaction_amount
    FROM {{ ref('stg_transactions') }}
    WHERE is_expense = true
    GROUP BY month, category_std
),

-- Get budget targets (this would typically come from a budget configuration table)
budget_targets AS (
    SELECT 
        category_std,
        -- Default budget targets based on category
        CASE 
            WHEN category_std = 'Food & Dining' THEN 500.0
            WHEN category_std = 'Groceries' THEN 300.0
            WHEN category_std = 'Transportation' THEN 200.0
            WHEN category_std = 'Entertainment' THEN 150.0
            WHEN category_std = 'Technology' THEN 100.0
            WHEN category_std = 'Healthcare' THEN 200.0
            WHEN category_std = 'Utilities' THEN 150.0
            WHEN category_std = 'Online Shopping' THEN 200.0
            WHEN category_std = 'Clothing' THEN 100.0
            ELSE 50.0
        END as budget_target
    FROM (SELECT DISTINCT category_std FROM {{ ref('stg_transactions') }} WHERE is_expense = true) categories
),

-- Calculate variance metrics
variance_calculation AS (
    SELECT 
        e.month,
        e.category_std,
        e.actual_expenses,
        e.transaction_count,
        e.avg_transaction_amount,
        e.max_transaction_amount,
        e.min_transaction_amount,
        COALESCE(b.budget_target, 0) as budget_target,
        
        -- Variance calculations
        e.actual_expenses - COALESCE(b.budget_target, 0) as variance,
        CASE 
            WHEN COALESCE(b.budget_target, 0) > 0 THEN 
                (e.actual_expenses - COALESCE(b.budget_target, 0)) / COALESCE(b.budget_target, 0) * 100
            ELSE NULL
        END as variance_pct,
        
        -- Budget utilization
        CASE 
            WHEN COALESCE(b.budget_target, 0) > 0 THEN 
                e.actual_expenses / COALESCE(b.budget_target, 0) * 100
            ELSE NULL
        END as budget_utilization_pct,
        
        -- Variance status
        CASE 
            WHEN e.actual_expenses > COALESCE(b.budget_target, 0) * 1.1 THEN 'over_budget'
            WHEN e.actual_expenses < COALESCE(b.budget_target, 0) * 0.9 THEN 'under_budget'
            ELSE 'on_budget'
        END as budget_status,
        
        -- Extract date parts
        EXTRACT(YEAR FROM e.month::date) as year,
        EXTRACT(MONTH FROM e.month::date) as month_num,
        EXTRACT(QUARTER FROM e.month::date) as quarter
        
    FROM monthly_expenses e
    LEFT JOIN budget_targets b ON e.category_std = b.category_std
),

-- Add rolling averages and trends
rolling_metrics AS (
    SELECT 
        *,
        -- 3-month rolling averages
        AVG(actual_expenses) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as actual_expenses_3m_avg,
        
        AVG(budget_target) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as budget_target_3m_avg,
        
        AVG(variance) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as variance_3m_avg,
        
        AVG(variance_pct) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as variance_pct_3m_avg,
        
        -- 6-month rolling averages
        AVG(actual_expenses) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as actual_expenses_6m_avg,
        
        AVG(budget_target) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as budget_target_6m_avg,
        
        AVG(variance) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as variance_6m_avg,
        
        AVG(variance_pct) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as variance_pct_6m_avg,
        
        -- 12-month rolling averages
        AVG(actual_expenses) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as actual_expenses_12m_avg,
        
        AVG(budget_target) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as budget_target_12m_avg,
        
        AVG(variance) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as variance_12m_avg,
        
        AVG(variance_pct) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) as variance_pct_12m_avg,
        
        -- Previous month values
        LAG(actual_expenses, 1) OVER (PARTITION BY category_std ORDER BY month) as prev_month_actual,
        LAG(budget_target, 1) OVER (PARTITION BY category_std ORDER BY month) as prev_month_budget,
        LAG(variance, 1) OVER (PARTITION BY category_std ORDER BY month) as prev_month_variance,
        LAG(variance_pct, 1) OVER (PARTITION BY category_std ORDER BY month) as prev_month_variance_pct,
        
        -- Previous year values
        LAG(actual_expenses, 12) OVER (PARTITION BY category_std ORDER BY month) as prev_year_actual,
        LAG(budget_target, 12) OVER (PARTITION BY category_std ORDER BY month) as prev_year_budget,
        LAG(variance, 12) OVER (PARTITION BY category_std ORDER BY month) as prev_year_variance,
        LAG(variance_pct, 12) OVER (PARTITION BY category_std ORDER BY month) as prev_year_variance_pct
        
    FROM variance_calculation
),

-- Calculate period-over-period changes
period_changes AS (
    SELECT 
        *,
        -- Month-over-month changes
        CASE 
            WHEN prev_month_actual IS NOT NULL THEN actual_expenses - prev_month_actual
            ELSE NULL
        END as actual_mom_change,
        
        CASE 
            WHEN prev_month_actual > 0 THEN (actual_expenses - prev_month_actual) / prev_month_actual * 100
            ELSE NULL
        END as actual_mom_change_pct,
        
        CASE 
            WHEN prev_month_variance IS NOT NULL THEN variance - prev_month_variance
            ELSE NULL
        END as variance_mom_change,
        
        CASE 
            WHEN prev_month_variance_pct IS NOT NULL THEN variance_pct - prev_month_variance_pct
            ELSE NULL
        END as variance_pct_mom_change,
        
        -- Year-over-year changes
        CASE 
            WHEN prev_year_actual IS NOT NULL THEN actual_expenses - prev_year_actual
            ELSE NULL
        END as actual_yoy_change,
        
        CASE 
            WHEN prev_year_actual > 0 THEN (actual_expenses - prev_year_actual) / prev_year_actual * 100
            ELSE NULL
        END as actual_yoy_change_pct,
        
        CASE 
            WHEN prev_year_variance IS NOT NULL THEN variance - prev_year_variance
            ELSE NULL
        END as variance_yoy_change,
        
        CASE 
            WHEN prev_year_variance_pct IS NOT NULL THEN variance_pct - prev_year_variance_pct
            ELSE NULL
        END as variance_pct_yoy_change
        
    FROM rolling_metrics
)

SELECT 
    month,
    category_std as category_name,
    year,
    month_num,
    quarter,
    
    -- Core budget metrics
    budget_target,
    actual_expenses,
    variance,
    variance_pct,
    budget_utilization_pct,
    budget_status,
    
    -- Transaction metrics
    transaction_count,
    avg_transaction_amount,
    max_transaction_amount,
    min_transaction_amount,
    
    -- Rolling averages
    actual_expenses_3m_avg,
    budget_target_3m_avg,
    variance_3m_avg,
    variance_pct_3m_avg,
    actual_expenses_6m_avg,
    budget_target_6m_avg,
    variance_6m_avg,
    variance_pct_6m_avg,
    actual_expenses_12m_avg,
    budget_target_12m_avg,
    variance_12m_avg,
    variance_pct_12m_avg,
    
    -- Period-over-period changes
    actual_mom_change,
    actual_mom_change_pct,
    variance_mom_change,
    variance_pct_mom_change,
    actual_yoy_change,
    actual_yoy_change_pct,
    variance_yoy_change,
    variance_pct_yoy_change,
    
    -- Previous period values
    prev_month_actual,
    prev_month_budget,
    prev_month_variance,
    prev_month_variance_pct,
    prev_year_actual,
    prev_year_budget,
    prev_year_variance,
    prev_year_variance_pct,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM period_changes
