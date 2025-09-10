-- Recurring transactions fact table
-- Detects and tracks recurring bills, subscriptions, and regular payments

WITH transaction_patterns AS (
    SELECT 
        merchant_clean,
        category_std,
        account_id,
        amount_usd,
        posted_at,
        DATE(posted_at) as transaction_date,
        EXTRACT(DAY FROM posted_at) as day_of_month,
        EXTRACT(MONTH FROM posted_at) as month,
        EXTRACT(YEAR FROM posted_at) as year,
        EXTRACT(DAYOFWEEK FROM posted_at) as day_of_week,
        -- Calculate days between transactions for the same merchant
        LAG(DATE(posted_at)) OVER (
            PARTITION BY merchant_clean, account_id 
            ORDER BY posted_at
        ) as prev_transaction_date
    FROM {{ ref('stg_transactions') }}
    WHERE is_expense = true
        AND merchant_clean IS NOT NULL
        AND merchant_clean != ''
),

-- Calculate intervals between transactions
interval_calculation AS (
    SELECT 
        *,
        CASE 
            WHEN prev_transaction_date IS NOT NULL THEN 
                DATE_PART('day', transaction_date - prev_transaction_date)
            ELSE NULL
        END as days_since_prev,
        
        -- Group transactions by merchant and account
        ROW_NUMBER() OVER (
            PARTITION BY merchant_clean, account_id 
            ORDER BY transaction_date
        ) as transaction_sequence
    FROM transaction_patterns
),

-- Detect recurring patterns
recurring_detection AS (
    SELECT 
        merchant_clean,
        category_std,
        account_id,
        amount_usd,
        transaction_date,
        day_of_month,
        month,
        year,
        day_of_week,
        days_since_prev,
        transaction_sequence,
        
        -- Calculate average interval
        AVG(days_since_prev) OVER (
            PARTITION BY merchant_clean, account_id
        ) as avg_interval_days,
        
        -- Calculate standard deviation of intervals
        STDDEV(days_since_prev) OVER (
            PARTITION BY merchant_clean, account_id
        ) as interval_stddev,
        
        -- Count total transactions for this merchant/account
        COUNT(*) OVER (
            PARTITION BY merchant_clean, account_id
        ) as total_transactions,
        
        -- Calculate amount consistency
        AVG(amount_usd) OVER (
            PARTITION BY merchant_clean, account_id
        ) as avg_amount,
        
        STDDEV(amount_usd) OVER (
            PARTITION BY merchant_clean, account_id
        ) as amount_stddev,
        
        -- Calculate coefficient of variation for amount consistency
        CASE 
            WHEN AVG(amount_usd) OVER (PARTITION BY merchant_clean, account_id) > 0 THEN
                STDDEV(amount_usd) OVER (PARTITION BY merchant_clean, account_id) / 
                AVG(amount_usd) OVER (PARTITION BY merchant_clean, account_id)
            ELSE NULL
        END as amount_cv,
        
        -- Calculate coefficient of variation for interval consistency
        CASE 
            WHEN AVG(days_since_prev) OVER (PARTITION BY merchant_clean, account_id) > 0 THEN
                STDDEV(days_since_prev) OVER (PARTITION BY merchant_clean, account_id) / 
                AVG(days_since_prev) OVER (PARTITION BY merchant_clean, account_id)
            ELSE NULL
        END as interval_cv
        
    FROM interval_calculation
),

-- Classify recurring patterns
recurring_classification AS (
    SELECT 
        *,
        -- Determine if this is a recurring transaction
        CASE 
            WHEN total_transactions >= 3 
                AND avg_interval_days BETWEEN 25 AND 35 
                AND interval_cv < 0.3 
                AND amount_cv < 0.2
            THEN 'monthly'
            WHEN total_transactions >= 3 
                AND avg_interval_days BETWEEN 6 AND 8 
                AND interval_cv < 0.3 
                AND amount_cv < 0.2
            THEN 'weekly'
            WHEN total_transactions >= 3 
                AND avg_interval_days BETWEEN 80 AND 95 
                AND interval_cv < 0.3 
                AND amount_cv < 0.2
            THEN 'quarterly'
            WHEN total_transactions >= 3 
                AND avg_interval_days BETWEEN 360 AND 375 
                AND interval_cv < 0.3 
                AND amount_cv < 0.2
            THEN 'yearly'
            WHEN total_transactions >= 2 
                AND avg_interval_days BETWEEN 25 AND 35 
                AND interval_cv < 0.5 
                AND amount_cv < 0.3
            THEN 'likely_monthly'
            WHEN total_transactions >= 2 
                AND avg_interval_days BETWEEN 6 AND 8 
                AND interval_cv < 0.5 
                AND amount_cv < 0.3
            THEN 'likely_weekly'
            ELSE 'irregular'
        END as recurring_type,
        
        -- Calculate confidence score
        CASE 
            WHEN total_transactions >= 6 
                AND interval_cv < 0.2 
                AND amount_cv < 0.1
            THEN 0.95
            WHEN total_transactions >= 4 
                AND interval_cv < 0.3 
                AND amount_cv < 0.2
            THEN 0.85
            WHEN total_transactions >= 3 
                AND interval_cv < 0.4 
                AND amount_cv < 0.3
            THEN 0.70
            WHEN total_transactions >= 2 
                AND interval_cv < 0.5 
                AND amount_cv < 0.4
            THEN 0.50
            ELSE 0.20
        END as confidence_score
        
    FROM recurring_detection
),

-- Calculate next expected transaction
next_transaction_calc AS (
    SELECT 
        *,
        -- Calculate next expected date
        CASE 
            WHEN recurring_type IN ('monthly', 'likely_monthly') THEN
                transaction_date + INTERVAL '1 month'
            WHEN recurring_type IN ('weekly', 'likely_weekly') THEN
                transaction_date + INTERVAL '1 week'
            WHEN recurring_type = 'quarterly' THEN
                transaction_date + INTERVAL '3 months'
            WHEN recurring_type = 'yearly' THEN
                transaction_date + INTERVAL '1 year'
            ELSE NULL
        END as next_expected_date,
        
        -- Calculate days until next expected transaction
        CASE 
            WHEN recurring_type IN ('monthly', 'likely_monthly') THEN
                DATE_PART('day', (transaction_date + INTERVAL '1 month') - CURRENT_DATE)
            WHEN recurring_type IN ('weekly', 'likely_weekly') THEN
                DATE_PART('day', (transaction_date + INTERVAL '1 week') - CURRENT_DATE)
            WHEN recurring_type = 'quarterly' THEN
                DATE_PART('day', (transaction_date + INTERVAL '3 months') - CURRENT_DATE)
            WHEN recurring_type = 'yearly' THEN
                DATE_PART('day', (transaction_date + INTERVAL '1 year') - CURRENT_DATE)
            ELSE NULL
        END as days_until_next,
        
        -- Calculate amount range
        avg_amount - amount_stddev as expected_min_amount,
        avg_amount + amount_stddev as expected_max_amount
        
    FROM recurring_classification
),

-- Aggregate by merchant/account for summary
merchant_summary AS (
    SELECT 
        merchant_clean,
        category_std,
        account_id,
        recurring_type,
        confidence_score,
        total_transactions,
        avg_interval_days,
        interval_stddev,
        interval_cv,
        avg_amount,
        amount_stddev,
        amount_cv,
        expected_min_amount,
        expected_max_amount,
        MAX(transaction_date) as last_transaction_date,
        MIN(transaction_date) as first_transaction_date,
        MAX(next_expected_date) as next_expected_date,
        MAX(days_until_next) as days_until_next,
        
        -- Calculate periodicity score
        CASE 
            WHEN interval_cv < 0.1 THEN 'very_regular'
            WHEN interval_cv < 0.2 THEN 'regular'
            WHEN interval_cv < 0.3 THEN 'somewhat_regular'
            WHEN interval_cv < 0.5 THEN 'irregular'
            ELSE 'highly_irregular'
        END as periodicity_score,
        
        -- Calculate amount stability
        CASE 
            WHEN amount_cv < 0.05 THEN 'very_stable'
            WHEN amount_cv < 0.1 THEN 'stable'
            WHEN amount_cv < 0.2 THEN 'somewhat_stable'
            WHEN amount_cv < 0.3 THEN 'variable'
            ELSE 'highly_variable'
        END as amount_stability
        
    FROM next_transaction_calc
    GROUP BY 
        merchant_clean, category_std, account_id, recurring_type, 
        confidence_score, total_transactions, avg_interval_days, 
        interval_stddev, interval_cv, avg_amount, amount_stddev, amount_cv,
        expected_min_amount, expected_max_amount
)

SELECT 
    merchant_clean as merchant_name,
    category_std as category_name,
    account_id,
    recurring_type,
    confidence_score,
    total_transactions,
    
    -- Interval metrics
    avg_interval_days,
    interval_stddev,
    interval_cv,
    periodicity_score,
    
    -- Amount metrics
    avg_amount,
    amount_stddev,
    amount_cv,
    expected_min_amount,
    expected_max_amount,
    amount_stability,
    
    -- Date metrics
    first_transaction_date,
    last_transaction_date,
    next_expected_date,
    days_until_next,
    
    -- Status indicators
    CASE 
        WHEN days_until_next < 0 THEN 'overdue'
        WHEN days_until_next <= 7 THEN 'due_soon'
        WHEN days_until_next <= 30 THEN 'upcoming'
        ELSE 'future'
    END as status,
    
    CASE 
        WHEN recurring_type IN ('monthly', 'weekly', 'quarterly', 'yearly') THEN true
        ELSE false
    END as is_confirmed_recurring,
    
    CASE 
        WHEN recurring_type LIKE 'likely_%' THEN true
        ELSE false
    END as is_likely_recurring,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM merchant_summary
WHERE recurring_type != 'irregular'
ORDER BY confidence_score DESC, total_transactions DESC
