-- Forecasts fact table
-- Generates expense and income forecasts using time series methods

WITH monthly_totals AS (
    SELECT 
        month,
        EXTRACT(YEAR FROM month::date) as year,
        EXTRACT(MONTH FROM month::date) as month_num,
        -- Total income and expenses
        SUM(CASE WHEN is_income THEN amount_usd ELSE 0 END) as total_income,
        SUM(CASE WHEN is_expense THEN ABS(amount_usd) ELSE 0 END) as total_expenses,
        -- By category
        category_std,
        SUM(CASE WHEN is_expense THEN ABS(amount_usd) ELSE 0 END) as category_expenses
    FROM {{ ref('stg_transactions') }}
    WHERE posted_at >= CURRENT_DATE - INTERVAL '24 months'  -- Use 2 years of data
    GROUP BY month, category_std
),

-- Prepare time series data for forecasting
time_series_data AS (
    SELECT 
        month,
        year,
        month_num,
        total_income,
        total_expenses,
        category_std,
        category_expenses,
        -- Create time index
        ROW_NUMBER() OVER (ORDER BY month) as time_index,
        -- Calculate moving averages
        AVG(total_income) OVER (
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as income_3m_avg,
        AVG(total_expenses) OVER (
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as expenses_3m_avg,
        AVG(category_expenses) OVER (
            PARTITION BY category_std 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as category_3m_avg,
        -- Calculate trends
        LAG(total_income, 1) OVER (ORDER BY month) as prev_income,
        LAG(total_expenses, 1) OVER (ORDER BY month) as prev_expenses,
        LAG(category_expenses, 1) OVER (PARTITION BY category_std ORDER BY month) as prev_category_expenses
    FROM monthly_totals
),

-- Calculate trend coefficients
trend_calculation AS (
    SELECT 
        *,
        -- Income trend
        CASE 
            WHEN prev_income > 0 THEN (total_income - prev_income) / prev_income
            ELSE 0
        END as income_trend,
        -- Expense trend
        CASE 
            WHEN prev_expenses > 0 THEN (total_expenses - prev_expenses) / prev_expenses
            ELSE 0
        END as expenses_trend,
        -- Category trend
        CASE 
            WHEN prev_category_expenses > 0 THEN (category_expenses - prev_category_expenses) / prev_category_expenses
            ELSE 0
        END as category_trend
    FROM time_series_data
),

-- Generate forecasts for next 3 months
forecast_generation AS (
    SELECT 
        month,
        year,
        month_num,
        total_income,
        total_expenses,
        category_std,
        category_expenses,
        time_index,
        income_3m_avg,
        expenses_3m_avg,
        category_3m_avg,
        income_trend,
        expenses_trend,
        category_trend,
        
        -- Generate forecasts for months +1, +2, +3
        -- Month +1
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                income_3m_avg * (1 + income_trend)
            ELSE NULL
        END as income_forecast_1m,
        
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                expenses_3m_avg * (1 + expenses_trend)
            ELSE NULL
        END as expenses_forecast_1m,
        
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                category_3m_avg * (1 + category_trend)
            ELSE NULL
        END as category_forecast_1m,
        
        -- Month +2
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                income_3m_avg * (1 + income_trend) * (1 + income_trend)
            ELSE NULL
        END as income_forecast_2m,
        
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                expenses_3m_avg * (1 + expenses_trend) * (1 + expenses_trend)
            ELSE NULL
        END as expenses_forecast_2m,
        
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                category_3m_avg * (1 + category_trend) * (1 + category_trend)
            ELSE NULL
        END as category_forecast_2m,
        
        -- Month +3
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                income_3m_avg * (1 + income_trend) * (1 + income_trend) * (1 + income_trend)
            ELSE NULL
        END as income_forecast_3m,
        
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                expenses_3m_avg * (1 + expenses_trend) * (1 + expenses_trend) * (1 + expenses_trend)
            ELSE NULL
        END as expenses_forecast_3m,
        
        CASE 
            WHEN time_index = (SELECT MAX(time_index) FROM trend_calculation) THEN
                category_3m_avg * (1 + category_trend) * (1 + category_trend) * (1 + category_trend)
            ELSE NULL
        END as category_forecast_3m
        
    FROM trend_calculation
),

-- Calculate confidence intervals and error metrics
confidence_calculation AS (
    SELECT 
        *,
        -- Calculate historical forecast accuracy (simplified)
        STDDEV(income_trend) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as income_volatility,
        STDDEV(expenses_trend) OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as expenses_volatility,
        STDDEV(category_trend) OVER (PARTITION BY category_std ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as category_volatility,
        
        -- Calculate confidence levels (simplified)
        CASE 
            WHEN ABS(income_trend) < 0.1 AND income_volatility < 0.2 THEN 0.85
            WHEN ABS(income_trend) < 0.2 AND income_volatility < 0.3 THEN 0.70
            WHEN ABS(income_trend) < 0.3 AND income_volatility < 0.4 THEN 0.55
            ELSE 0.40
        END as income_confidence,
        
        CASE 
            WHEN ABS(expenses_trend) < 0.1 AND expenses_volatility < 0.2 THEN 0.85
            WHEN ABS(expenses_trend) < 0.2 AND expenses_volatility < 0.3 THEN 0.70
            WHEN ABS(expenses_trend) < 0.3 AND expenses_volatility < 0.4 THEN 0.55
            ELSE 0.40
        END as expenses_confidence,
        
        CASE 
            WHEN ABS(category_trend) < 0.1 AND category_volatility < 0.2 THEN 0.85
            WHEN ABS(category_trend) < 0.2 AND category_volatility < 0.3 THEN 0.70
            WHEN ABS(category_trend) < 0.3 AND category_volatility < 0.4 THEN 0.55
            ELSE 0.40
        END as category_confidence
        
    FROM forecast_generation
),

-- Generate forecast records
forecast_records AS (
    SELECT 
        -- Month +1 forecasts
        month + INTERVAL '1 month' as forecast_date,
        '1_month' as forecast_horizon,
        'total' as forecast_type,
        'Income' as category_name,
        income_forecast_1m as forecast_amount,
        income_forecast_1m * (1 - income_confidence) as lower_bound,
        income_forecast_1m * (1 + income_confidence) as upper_bound,
        income_confidence as confidence_level,
        income_volatility as volatility
    FROM confidence_calculation
    WHERE income_forecast_1m IS NOT NULL
    
    UNION ALL
    
    SELECT 
        month + INTERVAL '1 month' as forecast_date,
        '1_month' as forecast_horizon,
        'total' as forecast_type,
        'Expenses' as category_name,
        expenses_forecast_1m as forecast_amount,
        expenses_forecast_1m * (1 - expenses_confidence) as lower_bound,
        expenses_forecast_1m * (1 + expenses_confidence) as upper_bound,
        expenses_confidence as confidence_level,
        expenses_volatility as volatility
    FROM confidence_calculation
    WHERE expenses_forecast_1m IS NOT NULL
    
    UNION ALL
    
    SELECT 
        month + INTERVAL '1 month' as forecast_date,
        '1_month' as forecast_horizon,
        'category' as forecast_type,
        category_std as category_name,
        category_forecast_1m as forecast_amount,
        category_forecast_1m * (1 - category_confidence) as lower_bound,
        category_forecast_1m * (1 + category_confidence) as upper_bound,
        category_confidence as confidence_level,
        category_volatility as volatility
    FROM confidence_calculation
    WHERE category_forecast_1m IS NOT NULL
    
    UNION ALL
    
    -- Month +2 forecasts
    SELECT 
        month + INTERVAL '2 months' as forecast_date,
        '2_months' as forecast_horizon,
        'total' as forecast_type,
        'Income' as category_name,
        income_forecast_2m as forecast_amount,
        income_forecast_2m * (1 - income_confidence) as lower_bound,
        income_forecast_2m * (1 + income_confidence) as upper_bound,
        income_confidence * 0.9 as confidence_level,  -- Reduce confidence for longer horizon
        income_volatility as volatility
    FROM confidence_calculation
    WHERE income_forecast_2m IS NOT NULL
    
    UNION ALL
    
    SELECT 
        month + INTERVAL '2 months' as forecast_date,
        '2_months' as forecast_horizon,
        'total' as forecast_type,
        'Expenses' as category_name,
        expenses_forecast_2m as forecast_amount,
        expenses_forecast_2m * (1 - expenses_confidence) as lower_bound,
        expenses_forecast_2m * (1 + expenses_confidence) as upper_bound,
        expenses_confidence * 0.9 as confidence_level,
        expenses_volatility as volatility
    FROM confidence_calculation
    WHERE expenses_forecast_2m IS NOT NULL
    
    UNION ALL
    
    SELECT 
        month + INTERVAL '2 months' as forecast_date,
        '2_months' as forecast_horizon,
        'category' as forecast_type,
        category_std as category_name,
        category_forecast_2m as forecast_amount,
        category_forecast_2m * (1 - category_confidence) as lower_bound,
        category_forecast_2m * (1 + category_confidence) as upper_bound,
        category_confidence * 0.9 as confidence_level,
        category_volatility as volatility
    FROM confidence_calculation
    WHERE category_forecast_2m IS NOT NULL
    
    UNION ALL
    
    -- Month +3 forecasts
    SELECT 
        month + INTERVAL '3 months' as forecast_date,
        '3_months' as forecast_horizon,
        'total' as forecast_type,
        'Income' as category_name,
        income_forecast_3m as forecast_amount,
        income_forecast_3m * (1 - income_confidence) as lower_bound,
        income_forecast_3m * (1 + income_confidence) as upper_bound,
        income_confidence * 0.8 as confidence_level,  -- Further reduce confidence
        income_volatility as volatility
    FROM confidence_calculation
    WHERE income_forecast_3m IS NOT NULL
    
    UNION ALL
    
    SELECT 
        month + INTERVAL '3 months' as forecast_date,
        '3_months' as forecast_horizon,
        'total' as forecast_type,
        'Expenses' as category_name,
        expenses_forecast_3m as forecast_amount,
        expenses_forecast_3m * (1 - expenses_confidence) as lower_bound,
        expenses_forecast_3m * (1 + expenses_confidence) as upper_bound,
        expenses_confidence * 0.8 as confidence_level,
        expenses_volatility as volatility
    FROM confidence_calculation
    WHERE expenses_forecast_3m IS NOT NULL
    
    UNION ALL
    
    SELECT 
        month + INTERVAL '3 months' as forecast_date,
        '3_months' as forecast_horizon,
        'category' as forecast_type,
        category_std as category_name,
        category_forecast_3m as forecast_amount,
        category_forecast_3m * (1 - category_confidence) as lower_bound,
        category_forecast_3m * (1 + category_confidence) as upper_bound,
        category_confidence * 0.8 as confidence_level,
        category_volatility as volatility
    FROM confidence_calculation
    WHERE category_forecast_3m IS NOT NULL
)

SELECT 
    forecast_date,
    forecast_horizon,
    forecast_type,
    category_name,
    forecast_amount,
    lower_bound,
    upper_bound,
    confidence_level,
    volatility,
    
    -- Calculate forecast range
    upper_bound - lower_bound as forecast_range,
    
    -- Calculate forecast precision
    CASE 
        WHEN forecast_amount > 0 THEN (upper_bound - lower_bound) / forecast_amount
        ELSE 1
    END as forecast_precision,
    
    -- Forecast quality assessment
    CASE 
        WHEN confidence_level >= 0.8 THEN 'high'
        WHEN confidence_level >= 0.6 THEN 'medium'
        WHEN confidence_level >= 0.4 THEN 'low'
        ELSE 'very_low'
    END as forecast_quality,
    
    -- Risk assessment
    CASE 
        WHEN volatility < 0.1 THEN 'low_risk'
        WHEN volatility < 0.2 THEN 'medium_risk'
        WHEN volatility < 0.3 THEN 'high_risk'
        ELSE 'very_high_risk'
    END as risk_level,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM forecast_records
ORDER BY forecast_date, forecast_type, category_name
