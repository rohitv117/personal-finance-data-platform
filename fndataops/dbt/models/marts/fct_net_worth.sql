-- Net worth fact table
-- Tracks assets, liabilities, and net worth over time

WITH account_balances AS (
    SELECT 
        DATE(posted_at) as date,
        account_id,
        source,
        currency,
        -- Get the latest balance for each account per day
        LAST_VALUE(balance_after) OVER (
            PARTITION BY account_id, DATE(posted_at) 
            ORDER BY posted_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as daily_balance,
        -- Get the first balance for each account per day
        FIRST_VALUE(balance_after) OVER (
            PARTITION BY account_id, DATE(posted_at) 
            ORDER BY posted_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as opening_balance
    FROM {{ ref('stg_transactions') }}
    WHERE balance_after IS NOT NULL
),

-- Convert to USD using FX rates
usd_balances AS (
    SELECT 
        a.date,
        a.account_id,
        a.source,
        a.currency,
        a.daily_balance,
        a.opening_balance,
        CASE 
            WHEN a.currency = 'USD' THEN a.daily_balance
            WHEN a.currency = 'EUR' THEN a.daily_balance / COALESCE(f.usd_to_eur, 1.0)
            WHEN a.currency = 'GBP' THEN a.daily_balance / COALESCE(f.usd_to_gbp, 1.0)
            WHEN a.currency = 'CAD' THEN a.daily_balance / COALESCE(f.usd_to_cad, 1.0)
            WHEN a.currency = 'AUD' THEN a.daily_balance / COALESCE(f.usd_to_aud, 1.0)
            ELSE a.daily_balance
        END as daily_balance_usd,
        CASE 
            WHEN a.currency = 'USD' THEN a.opening_balance
            WHEN a.currency = 'EUR' THEN a.opening_balance / COALESCE(f.usd_to_eur, 1.0)
            WHEN a.currency = 'GBP' THEN a.opening_balance / COALESCE(f.usd_to_gbp, 1.0)
            WHEN a.currency = 'CAD' THEN a.opening_balance / COALESCE(f.usd_to_cad, 1.0)
            WHEN a.currency = 'AUD' THEN a.opening_balance / COALESCE(f.usd_to_aud, 1.0)
            ELSE a.opening_balance
        END as opening_balance_usd
    FROM account_balances a
    LEFT JOIN {{ ref('ref_fx_rates') }} f
        ON a.date = f.date
),

-- Classify accounts by type
account_classification AS (
    SELECT 
        u.*,
        CASE 
            WHEN u.source = 'bank' AND u.account_id LIKE '%checking%' THEN 'checking'
            WHEN u.source = 'bank' AND u.account_id LIKE '%savings%' THEN 'savings'
            WHEN u.source = 'card' THEN 'credit'
            WHEN u.source = 'brokerage' THEN 'investment'
            ELSE 'other'
        END as account_type,
        CASE 
            WHEN u.source = 'card' THEN 'liability'
            ELSE 'asset'
        END as balance_type
    FROM usd_balances u
),

-- Calculate daily net worth
daily_net_worth AS (
    SELECT 
        date,
        -- Asset accounts
        SUM(CASE WHEN balance_type = 'asset' THEN daily_balance_usd ELSE 0 END) as total_assets,
        SUM(CASE WHEN balance_type = 'asset' THEN opening_balance_usd ELSE 0 END) as opening_assets,
        
        -- Liability accounts
        SUM(CASE WHEN balance_type = 'liability' THEN ABS(daily_balance_usd) ELSE 0 END) as total_liabilities,
        SUM(CASE WHEN balance_type = 'liability' THEN ABS(opening_balance_usd) ELSE 0 END) as opening_liabilities,
        
        -- Account type breakdown
        SUM(CASE WHEN account_type = 'checking' THEN daily_balance_usd ELSE 0 END) as checking_balance,
        SUM(CASE WHEN account_type = 'savings' THEN daily_balance_usd ELSE 0 END) as savings_balance,
        SUM(CASE WHEN account_type = 'credit' THEN ABS(daily_balance_usd) ELSE 0 END) as credit_balance,
        SUM(CASE WHEN account_type = 'investment' THEN daily_balance_usd ELSE 0 END) as investment_balance,
        
        -- Count accounts
        COUNT(DISTINCT CASE WHEN balance_type = 'asset' THEN account_id END) as asset_accounts,
        COUNT(DISTINCT CASE WHEN balance_type = 'liability' THEN account_id END) as liability_accounts,
        COUNT(DISTINCT account_id) as total_accounts
        
    FROM account_classification
    GROUP BY date
),

-- Calculate derived metrics
derived_metrics AS (
    SELECT 
        *,
        -- Net worth calculation
        total_assets - total_liabilities as net_worth,
        opening_assets - opening_liabilities as opening_net_worth,
        
        -- Daily change
        (total_assets - total_liabilities) - (opening_assets - opening_liabilities) as net_worth_change,
        
        -- Asset allocation percentages
        CASE 
            WHEN total_assets > 0 THEN checking_balance / total_assets
            ELSE 0
        END as checking_pct,
        
        CASE 
            WHEN total_assets > 0 THEN savings_balance / total_assets
            ELSE 0
        END as savings_pct,
        
        CASE 
            WHEN total_assets > 0 THEN investment_balance / total_assets
            ELSE 0
        END as investment_pct,
        
        -- Debt-to-asset ratio
        CASE 
            WHEN total_assets > 0 THEN total_liabilities / total_assets
            ELSE NULL
        END as debt_to_asset_ratio,
        
        -- Extract date parts
        EXTRACT(YEAR FROM date) as year,
        EXTRACT(MONTH FROM date) as month,
        EXTRACT(DAY FROM date) as day,
        EXTRACT(QUARTER FROM date) as quarter,
        EXTRACT(DAYOFWEEK FROM date) as day_of_week
        
    FROM daily_net_worth
),

-- Add rolling windows and period-over-period changes
rolling_metrics AS (
    SELECT 
        *,
        -- 7-day rolling averages
        AVG(net_worth) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as net_worth_7d_avg,
        
        AVG(net_worth_change) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as net_worth_change_7d_avg,
        
        -- 30-day rolling averages
        AVG(net_worth) OVER (
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as net_worth_30d_avg,
        
        AVG(net_worth_change) OVER (
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as net_worth_change_30d_avg,
        
        -- 90-day rolling averages
        AVG(net_worth) OVER (
            ORDER BY date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as net_worth_90d_avg,
        
        AVG(net_worth_change) OVER (
            ORDER BY date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as net_worth_change_90d_avg,
        
        -- Previous day values for change calculations
        LAG(net_worth, 1) OVER (ORDER BY date) as prev_day_net_worth,
        LAG(net_worth, 7) OVER (ORDER BY date) as prev_week_net_worth,
        LAG(net_worth, 30) OVER (ORDER BY date) as prev_month_net_worth,
        LAG(net_worth, 365) OVER (ORDER BY date) as prev_year_net_worth
        
    FROM derived_metrics
),

-- Calculate period-over-period changes
period_changes AS (
    SELECT 
        *,
        -- Day-over-day change
        CASE 
            WHEN prev_day_net_worth IS NOT NULL THEN net_worth - prev_day_net_worth
            ELSE NULL
        END as net_worth_dod_change,
        
        -- Week-over-week change
        CASE 
            WHEN prev_week_net_worth IS NOT NULL THEN net_worth - prev_week_net_worth
            ELSE NULL
        END as net_worth_wow_change,
        
        -- Month-over-month change
        CASE 
            WHEN prev_month_net_worth IS NOT NULL THEN net_worth - prev_month_net_worth
            ELSE NULL
        END as net_worth_mom_change,
        
        -- Year-over-year change
        CASE 
            WHEN prev_year_net_worth IS NOT NULL THEN net_worth - prev_year_net_worth
            ELSE NULL
        END as net_worth_yoy_change,
        
        -- Percentage changes
        CASE 
            WHEN prev_day_net_worth > 0 THEN (net_worth - prev_day_net_worth) / prev_day_net_worth * 100
            ELSE NULL
        END as net_worth_dod_change_pct,
        
        CASE 
            WHEN prev_week_net_worth > 0 THEN (net_worth - prev_week_net_worth) / prev_week_net_worth * 100
            ELSE NULL
        END as net_worth_wow_change_pct,
        
        CASE 
            WHEN prev_month_net_worth > 0 THEN (net_worth - prev_month_net_worth) / prev_month_net_worth * 100
            ELSE NULL
        END as net_worth_mom_change_pct,
        
        CASE 
            WHEN prev_year_net_worth > 0 THEN (net_worth - prev_year_net_worth) / prev_year_net_worth * 100
            ELSE NULL
        END as net_worth_yoy_change_pct
        
    FROM rolling_metrics
)

SELECT 
    date,
    year,
    month,
    day,
    quarter,
    day_of_week,
    
    -- Core net worth metrics
    net_worth,
    opening_net_worth,
    net_worth_change,
    
    -- Asset breakdown
    total_assets,
    opening_assets,
    checking_balance,
    savings_balance,
    investment_balance,
    
    -- Liability breakdown
    total_liabilities,
    opening_liabilities,
    credit_balance,
    
    -- Allocation percentages
    checking_pct,
    savings_pct,
    investment_pct,
    debt_to_asset_ratio,
    
    -- Account counts
    asset_accounts,
    liability_accounts,
    total_accounts,
    
    -- Rolling averages
    net_worth_7d_avg,
    net_worth_change_7d_avg,
    net_worth_30d_avg,
    net_worth_change_30d_avg,
    net_worth_90d_avg,
    net_worth_change_90d_avg,
    
    -- Period-over-period changes
    net_worth_dod_change,
    net_worth_wow_change,
    net_worth_mom_change,
    net_worth_yoy_change,
    net_worth_dod_change_pct,
    net_worth_wow_change_pct,
    net_worth_mom_change_pct,
    net_worth_yoy_change_pct,
    
    -- Previous period values
    prev_day_net_worth,
    prev_week_net_worth,
    prev_month_net_worth,
    prev_year_net_worth,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM period_changes
