-- Daily cashflow fact table
-- Aggregates daily income, expenses, and savings metrics

WITH daily_aggregates AS (
    SELECT 
        DATE(posted_at) as date,
        account_id,
        source,
        -- Income metrics
        SUM(CASE WHEN is_income THEN amount_usd ELSE 0 END) as income,
        COUNT(CASE WHEN is_income THEN 1 END) as income_transactions,
        
        -- Expense metrics
        SUM(CASE WHEN is_expense THEN ABS(amount_usd) ELSE 0 END) as expenses,
        COUNT(CASE WHEN is_expense THEN 1 END) as expense_transactions,
        
        -- Transfer metrics
        SUM(CASE WHEN is_transfer THEN amount_usd ELSE 0 END) as transfers,
        COUNT(CASE WHEN is_transfer THEN 1 END) as transfer_transactions,
        
        -- Investment metrics
        SUM(CASE WHEN is_investment THEN amount_usd ELSE 0 END) as investments,
        COUNT(CASE WHEN is_investment THEN 1 END) as investment_transactions,
        
        -- Total metrics
        SUM(amount_usd) as net_amount,
        COUNT(*) as total_transactions,
        
        -- Balance tracking
        MAX(balance_after) as closing_balance,
        MIN(balance_after) as opening_balance
        
    FROM {{ ref('stg_transactions') }}
    GROUP BY DATE(posted_at), account_id, source
),

-- Calculate derived metrics
derived_metrics AS (
    SELECT 
        *,
        -- Savings calculation
        CASE 
            WHEN income > 0 THEN (income - expenses) / income
            ELSE 0
        END as savings_rate,
        
        -- Balance delta
        closing_balance - opening_balance as balance_delta,
        
        -- Transaction density
        total_transactions / 1.0 as transaction_density,
        
        -- Income vs expense ratio
        CASE 
            WHEN expenses > 0 THEN income / expenses
            ELSE NULL
        END as income_expense_ratio,
        
        -- Net worth change
        net_amount as net_worth_change,
        
        -- Date parts for analysis
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,
        EXTRACT(DAYOFWEEK FROM date) as day_of_week,
        EXTRACT(QUARTER FROM date) as quarter,
        
        -- Week identifier
        TO_CHAR(date, 'IYYY-"W"IW') as week
        
    FROM daily_aggregates
),

-- Add rolling windows
rolling_metrics AS (
    SELECT 
        *,
        -- 7-day rolling averages
        AVG(income) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as income_7d_avg,
        
        AVG(expenses) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as expenses_7d_avg,
        
        AVG(savings_rate) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as savings_rate_7d_avg,
        
        -- 30-day rolling averages
        AVG(income) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as income_30d_avg,
        
        AVG(expenses) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as expenses_30d_avg,
        
        AVG(savings_rate) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as savings_rate_30d_avg,
        
        -- 90-day rolling averages
        AVG(income) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as income_90d_avg,
        
        AVG(expenses) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as expenses_90d_avg,
        
        AVG(savings_rate) OVER (
            PARTITION BY account_id 
            ORDER BY date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as savings_rate_90d_avg
        
    FROM derived_metrics
)

SELECT 
    date,
    account_id,
    source,
    year,
    month,
    day,
    day_of_week,
    quarter,
    week,
    
    -- Core metrics
    income,
    expenses,
    savings_rate,
    transfers,
    investments,
    net_amount,
    balance_delta,
    closing_balance,
    opening_balance,
    
    -- Transaction counts
    income_transactions,
    expense_transactions,
    transfer_transactions,
    investment_transactions,
    total_transactions,
    transaction_density,
    
    -- Ratios
    income_expense_ratio,
    net_worth_change,
    
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
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM rolling_metrics