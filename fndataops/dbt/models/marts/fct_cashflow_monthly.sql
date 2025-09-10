-- Monthly cashflow fact table
-- Aggregates monthly income, expenses, and savings metrics

WITH monthly_aggregates AS (
    SELECT 
        month,
        account_id,
        source,
        -- Income metrics
        SUM(income) as income,
        SUM(income_transactions) as income_transactions,
        AVG(income) as avg_daily_income,
        
        -- Expense metrics
        SUM(expenses) as expenses,
        SUM(expense_transactions) as expense_transactions,
        AVG(expenses) as avg_daily_expenses,
        
        -- Transfer metrics
        SUM(transfers) as transfers,
        SUM(transfer_transactions) as transfer_transactions,
        
        -- Investment metrics
        SUM(investments) as investments,
        SUM(investment_transactions) as investment_transactions,
        
        -- Total metrics
        SUM(net_amount) as net_amount,
        SUM(total_transactions) as total_transactions,
        AVG(total_transactions) as avg_daily_transactions,
        
        -- Balance metrics
        AVG(closing_balance) as avg_balance,
        MAX(closing_balance) as max_balance,
        MIN(opening_balance) as min_balance,
        SUM(balance_delta) as total_balance_delta,
        
        -- Savings metrics
        AVG(savings_rate) as avg_savings_rate,
        MAX(savings_rate) as max_savings_rate,
        MIN(savings_rate) as min_savings_rate,
        
        -- Rolling averages
        AVG(income_7d_avg) as income_7d_avg,
        AVG(expenses_7d_avg) as expenses_7d_avg,
        AVG(savings_rate_7d_avg) as savings_rate_7d_avg,
        AVG(income_30d_avg) as income_30d_avg,
        AVG(expenses_30d_avg) as expenses_30d_avg,
        AVG(savings_rate_30d_avg) as savings_rate_30d_avg,
        AVG(income_90d_avg) as income_90d_avg,
        AVG(expenses_90d_avg) as expenses_90d_avg,
        AVG(savings_rate_90d_avg) as savings_rate_90d_avg,
        
        -- Date range
        MIN(date) as month_start,
        MAX(date) as month_end,
        COUNT(DISTINCT date) as active_days
        
    FROM {{ ref('fct_cashflow_daily') }}
    GROUP BY month, account_id, source
),

-- Calculate derived metrics
derived_metrics AS (
    SELECT 
        *,
        -- Overall savings rate
        CASE 
            WHEN income > 0 THEN (income - expenses) / income
            ELSE 0
        END as savings_rate,
        
        -- Income vs expense ratio
        CASE 
            WHEN expenses > 0 THEN income / expenses
            ELSE NULL
        END as income_expense_ratio,
        
        -- Transaction density per day
        total_transactions / GREATEST(active_days, 1) as transactions_per_day,
        
        -- Balance volatility
        CASE 
            WHEN avg_balance > 0 THEN (max_balance - min_balance) / avg_balance
            ELSE 0
        END as balance_volatility,
        
        -- Month-over-month calculations (will be calculated in next CTE)
        NULL as income_mom_change,
        NULL as expenses_mom_change,
        NULL as savings_rate_mom_change,
        
        -- Year-over-year calculations
        NULL as income_yoy_change,
        NULL as expenses_yoy_change,
        NULL as savings_rate_yoy_change,
        
        -- Extract year and month for analysis
        EXTRACT(YEAR FROM month_start) as year,
        EXTRACT(MONTH FROM month_start) as month_num,
        EXTRACT(QUARTER FROM month_start) as quarter
        
    FROM monthly_aggregates
),

-- Calculate period-over-period changes
period_changes AS (
    SELECT 
        *,
        -- Month-over-month changes
        LAG(income, 1) OVER (PARTITION BY account_id ORDER BY month) as prev_month_income,
        LAG(expenses, 1) OVER (PARTITION BY account_id ORDER BY month) as prev_month_expenses,
        LAG(savings_rate, 1) OVER (PARTITION BY account_id ORDER BY month) as prev_month_savings_rate,
        
        -- Year-over-year changes
        LAG(income, 12) OVER (PARTITION BY account_id ORDER BY month) as prev_year_income,
        LAG(expenses, 12) OVER (PARTITION BY account_id ORDER BY month) as prev_year_expenses,
        LAG(savings_rate, 12) OVER (PARTITION BY account_id ORDER BY month) as prev_year_savings_rate
        
    FROM derived_metrics
),

-- Final calculations
final_metrics AS (
    SELECT 
        *,
        -- Month-over-month percentage changes
        CASE 
            WHEN prev_month_income > 0 THEN (income - prev_month_income) / prev_month_income * 100
            ELSE NULL
        END as income_mom_change,
        
        CASE 
            WHEN prev_month_expenses > 0 THEN (expenses - prev_month_expenses) / prev_month_expenses * 100
            ELSE NULL
        END as expenses_mom_change,
        
        CASE 
            WHEN prev_month_savings_rate IS NOT NULL THEN savings_rate - prev_month_savings_rate
            ELSE NULL
        END as savings_rate_mom_change,
        
        -- Year-over-year percentage changes
        CASE 
            WHEN prev_year_income > 0 THEN (income - prev_year_income) / prev_year_income * 100
            ELSE NULL
        END as income_yoy_change,
        
        CASE 
            WHEN prev_year_expenses > 0 THEN (expenses - prev_year_expenses) / prev_year_expenses * 100
            ELSE NULL
        END as expenses_yoy_change,
        
        CASE 
            WHEN prev_year_savings_rate IS NOT NULL THEN savings_rate - prev_year_savings_rate
            ELSE NULL
        END as savings_rate_yoy_change
        
    FROM period_changes
)

SELECT 
    month,
    account_id,
    source,
    year,
    month_num,
    quarter,
    month_start,
    month_end,
    active_days,
    
    -- Core metrics
    income,
    expenses,
    savings_rate,
    transfers,
    investments,
    net_amount,
    total_balance_delta,
    avg_balance,
    max_balance,
    min_balance,
    
    -- Transaction counts
    income_transactions,
    expense_transactions,
    transfer_transactions,
    investment_transactions,
    total_transactions,
    transactions_per_day,
    
    -- Daily averages
    avg_daily_income,
    avg_daily_expenses,
    avg_daily_transactions,
    
    -- Savings metrics
    avg_savings_rate,
    max_savings_rate,
    min_savings_rate,
    
    -- Ratios
    income_expense_ratio,
    balance_volatility,
    
    -- Rolling averages
    income_7d_avg,
    expenses_7d_avg,
    savings_rate_7d_avg,
    income_30d_avg,
    expenses_30d_avg,
    savings_rate_30d_avg,
    income_90d_avg,
    expenses_90d_avg,
    savings_rate_90d_avg,
    
    -- Period-over-period changes
    income_mom_change,
    expenses_mom_change,
    savings_rate_mom_change,
    income_yoy_change,
    expenses_yoy_change,
    savings_rate_yoy_change,
    
    -- Previous period values
    prev_month_income,
    prev_month_expenses,
    prev_month_savings_rate,
    prev_year_income,
    prev_year_expenses,
    prev_year_savings_rate,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM final_metrics
