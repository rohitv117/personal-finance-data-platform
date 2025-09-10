{{
  config(
    materialized='incremental',
    unique_key="merchant_norm || '_' || category_norm"
  )
}}

WITH recurring_patterns AS (
    SELECT 
        merchant_norm,
        category_norm,
        COUNT(*) as occurrence_count,
        AVG(ABS(amount)) as average_amount,
        STDDEV(ABS(amount)) as amount_stddev,
        MIN(posted_at) as first_seen,
        MAX(posted_at) as last_seen,
        -- Calculate frequency based on time between transactions
        AVG(EXTRACT(EPOCH FROM (posted_at - LAG(posted_at) OVER (
            PARTITION BY merchant_norm, category_norm 
            ORDER BY posted_at
        ))) / 86400) as avg_days_between
    FROM {{ ref('stg_transactions') }}
    WHERE is_recurring_guess = TRUE
    {% if is_incremental() %}
        AND posted_at > (SELECT MAX(last_seen) FROM {{ this }})
    {% endif %}
    GROUP BY merchant_norm, category_norm
    HAVING COUNT(*) >= 2  -- At least 2 occurrences to be considered recurring
),

recurring_status AS (
    SELECT 
        rp.*,
        CASE 
            WHEN rp.avg_days_between <= 7 THEN 'Weekly'
            WHEN rp.avg_days_between <= 31 THEN 'Monthly'
            WHEN rp.avg_days_between <= 90 THEN 'Quarterly'
            ELSE 'Annual'
        END as frequency,
        CASE 
            WHEN rp.last_seen >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
            WHEN rp.last_seen >= CURRENT_DATE - INTERVAL '90 days' THEN 'Recent'
            ELSE 'Inactive'
        END as status,
        -- Predict next due date
        rp.last_seen + INTERVAL '1 day' * rp.avg_days_between as next_due_date
    FROM recurring_patterns rp
)

SELECT 
    merchant_norm,
    category_norm,
    occurrence_count,
    ROUND(average_amount, 2) as average_amount,
    ROUND(amount_stddev, 2) as amount_stddev,
    first_seen,
    last_seen,
    next_due_date,
    frequency,
    status,
    CURRENT_TIMESTAMP as created_at
FROM recurring_status
ORDER BY occurrence_count DESC, average_amount DESC 