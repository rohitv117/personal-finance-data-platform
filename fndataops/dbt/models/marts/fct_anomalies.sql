-- Anomalies fact table
-- Detects and scores anomalous transactions using multiple methods

WITH transaction_stats AS (
    SELECT 
        txn_id,
        merchant_clean,
        category_std,
        account_id,
        amount_usd,
        posted_at,
        DATE(posted_at) as transaction_date,
        EXTRACT(MONTH FROM posted_at) as month,
        EXTRACT(YEAR FROM posted_at) as year,
        EXTRACT(DAYOFWEEK FROM posted_at) as day_of_week,
        -- Calculate rolling statistics for anomaly detection
        AVG(ABS(amount_usd)) OVER (
            PARTITION BY category_std 
            ORDER BY posted_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as category_avg_amount_30d,
        
        STDDEV(ABS(amount_usd)) OVER (
            PARTITION BY category_std 
            ORDER BY posted_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as category_std_amount_30d,
        
        AVG(ABS(amount_usd)) OVER (
            PARTITION BY merchant_clean 
            ORDER BY posted_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as merchant_avg_amount_30d,
        
        STDDEV(ABS(amount_usd)) OVER (
            PARTITION BY merchant_clean 
            ORDER BY posted_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as merchant_std_amount_30d,
        
        -- Count transactions for this merchant in the last 30 days
        COUNT(*) OVER (
            PARTITION BY merchant_clean 
            ORDER BY posted_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as merchant_transaction_count_30d,
        
        -- Count transactions for this category in the last 30 days
        COUNT(*) OVER (
            PARTITION BY category_std 
            ORDER BY posted_at 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as category_transaction_count_30d
        
    FROM {{ ref('stg_transactions') }}
    WHERE is_expense = true
),

-- Calculate Z-scores and anomaly scores
anomaly_scoring AS (
    SELECT 
        *,
        -- Z-score for amount within category
        CASE 
            WHEN category_std_amount_30d > 0 THEN 
                (ABS(amount_usd) - category_avg_amount_30d) / category_std_amount_30d
            ELSE 0
        END as category_z_score,
        
        -- Z-score for amount within merchant
        CASE 
            WHEN merchant_std_amount_30d > 0 THEN 
                (ABS(amount_usd) - merchant_avg_amount_30d) / merchant_std_amount_30d
            ELSE 0
        END as merchant_z_score,
        
        -- Amount ratio to category average
        CASE 
            WHEN category_avg_amount_30d > 0 THEN 
                ABS(amount_usd) / category_avg_amount_30d
            ELSE 1
        END as category_amount_ratio,
        
        -- Amount ratio to merchant average
        CASE 
            WHEN merchant_avg_amount_30d > 0 THEN 
                ABS(amount_usd) / merchant_avg_amount_30d
            ELSE 1
        END as merchant_amount_ratio
        
    FROM transaction_stats
),

-- Detect different types of anomalies
anomaly_detection AS (
    SELECT 
        *,
        -- Statistical outliers (Z-score > 2)
        CASE 
            WHEN ABS(category_z_score) > 2 OR ABS(merchant_z_score) > 2 THEN true
            ELSE false
        END as is_statistical_outlier,
        
        -- Amount spikes (ratio > 3x average)
        CASE 
            WHEN category_amount_ratio > 3 OR merchant_amount_ratio > 3 THEN true
            ELSE false
        END as is_amount_spike,
        
        -- Novel merchants (first time seeing this merchant)
        CASE 
            WHEN merchant_transaction_count_30d = 1 THEN true
            ELSE false
        END as is_novel_merchant,
        
        -- Unusual timing (transactions on unusual days)
        CASE 
            WHEN day_of_week IN (1, 7) AND category_std IN ('Business', 'Professional Services') THEN true
            WHEN day_of_week IN (2, 3, 4, 5, 6) AND category_std IN ('Entertainment', 'Recreation') THEN false
            ELSE false
        END as is_unusual_timing,
        
        -- High frequency (too many transactions for this merchant)
        CASE 
            WHEN merchant_transaction_count_30d > 10 THEN true
            ELSE false
        END as is_high_frequency,
        
        -- Category mismatch (unusual category for this merchant)
        CASE 
            WHEN merchant_clean ILIKE '%restaurant%' AND category_std NOT IN ('Food & Dining', 'Restaurants') THEN true
            WHEN merchant_clean ILIKE '%gas%' AND category_std NOT IN ('Transportation', 'Gas') THEN true
            WHEN merchant_clean ILIKE '%grocery%' AND category_std NOT IN ('Food & Dining', 'Groceries') THEN true
            ELSE false
        END as is_category_mismatch
        
    FROM anomaly_scoring
),

-- Calculate composite anomaly scores
composite_scoring AS (
    SELECT 
        *,
        -- Calculate composite anomaly score (0-100)
        (
            CASE WHEN is_statistical_outlier THEN 25 ELSE 0 END +
            CASE WHEN is_amount_spike THEN 20 ELSE 0 END +
            CASE WHEN is_novel_merchant THEN 15 ELSE 0 END +
            CASE WHEN is_unusual_timing THEN 10 ELSE 0 END +
            CASE WHEN is_high_frequency THEN 15 ELSE 0 END +
            CASE WHEN is_category_mismatch THEN 10 ELSE 0 END +
            -- Additional scoring based on Z-scores
            LEAST(ABS(category_z_score) * 5, 15) +
            LEAST(ABS(merchant_z_score) * 5, 15)
        ) as anomaly_score,
        
        -- Determine severity level
        CASE 
            WHEN (
                CASE WHEN is_statistical_outlier THEN 25 ELSE 0 END +
                CASE WHEN is_amount_spike THEN 20 ELSE 0 END +
                CASE WHEN is_novel_merchant THEN 15 ELSE 0 END +
                CASE WHEN is_unusual_timing THEN 10 ELSE 0 END +
                CASE WHEN is_high_frequency THEN 15 ELSE 0 END +
                CASE WHEN is_category_mismatch THEN 10 ELSE 0 END +
                LEAST(ABS(category_z_score) * 5, 15) +
                LEAST(ABS(merchant_z_score) * 5, 15)
            ) >= 70 THEN 'high'
            WHEN (
                CASE WHEN is_statistical_outlier THEN 25 ELSE 0 END +
                CASE WHEN is_amount_spike THEN 20 ELSE 0 END +
                CASE WHEN is_novel_merchant THEN 15 ELSE 0 END +
                CASE WHEN is_unusual_timing THEN 10 ELSE 0 END +
                CASE WHEN is_high_frequency THEN 15 ELSE 0 END +
                CASE WHEN is_category_mismatch THEN 10 ELSE 0 END +
                LEAST(ABS(category_z_score) * 5, 15) +
                LEAST(ABS(merchant_z_score) * 5, 15)
            ) >= 40 THEN 'medium'
            WHEN (
                CASE WHEN is_statistical_outlier THEN 25 ELSE 0 END +
                CASE WHEN is_amount_spike THEN 20 ELSE 0 END +
                CASE WHEN is_novel_merchant THEN 15 ELSE 0 END +
                CASE WHEN is_unusual_timing THEN 10 ELSE 0 END +
                CASE WHEN is_high_frequency THEN 15 ELSE 0 END +
                CASE WHEN is_category_mismatch THEN 10 ELSE 0 END +
                LEAST(ABS(category_z_score) * 5, 15) +
                LEAST(ABS(merchant_z_score) * 5, 15)
            ) >= 20 THEN 'low'
            ELSE 'minimal'
        END as severity,
        
        -- Generate anomaly type description
        CASE 
            WHEN is_statistical_outlier AND is_amount_spike THEN 'Statistical Outlier + Amount Spike'
            WHEN is_statistical_outlier THEN 'Statistical Outlier'
            WHEN is_amount_spike THEN 'Amount Spike'
            WHEN is_novel_merchant THEN 'Novel Merchant'
            WHEN is_unusual_timing THEN 'Unusual Timing'
            WHEN is_high_frequency THEN 'High Frequency'
            WHEN is_category_mismatch THEN 'Category Mismatch'
            ELSE 'Multiple Factors'
        END as anomaly_type,
        
        -- Generate driver explanation
        CASE 
            WHEN is_statistical_outlier AND is_amount_spike THEN 
                'Transaction amount is ' || ROUND(category_amount_ratio, 1) || 'x the category average and ' || 
                ROUND(merchant_amount_ratio, 1) || 'x the merchant average'
            WHEN is_statistical_outlier THEN 
                'Transaction amount is ' || ROUND(ABS(category_z_score), 1) || ' standard deviations from category average'
            WHEN is_amount_spike THEN 
                'Transaction amount is ' || ROUND(category_amount_ratio, 1) || 'x the category average'
            WHEN is_novel_merchant THEN 
                'First transaction with this merchant in the last 30 days'
            WHEN is_unusual_timing THEN 
                'Transaction occurred at an unusual time for this category'
            WHEN is_high_frequency THEN 
                'High frequency of transactions with this merchant (' || merchant_transaction_count_30d || ' in 30 days)'
            WHEN is_category_mismatch THEN 
                'Category classification may be incorrect for this merchant'
            ELSE 'Multiple anomaly factors detected'
        END as driver,
        
        -- Generate remediation hints
        CASE 
            WHEN is_statistical_outlier AND is_amount_spike THEN 
                'Review transaction details and verify amount. Consider if this is a legitimate large purchase.'
            WHEN is_novel_merchant THEN 
                'Verify this is a legitimate merchant. Check for potential fraud or data entry errors.'
            WHEN is_category_mismatch THEN 
                'Review and correct category classification for this merchant.'
            WHEN is_high_frequency THEN 
                'Monitor for potential duplicate transactions or subscription billing issues.'
            WHEN is_unusual_timing THEN 
                'Verify transaction timing and merchant legitimacy.'
            ELSE 'Review transaction details and verify all information is correct.'
        END as remediation_hint
        
    FROM anomaly_detection
),

-- Filter to only actual anomalies
anomaly_filtering AS (
    SELECT *
    FROM composite_scoring
    WHERE anomaly_score >= 20  -- Only include transactions with significant anomaly scores
)

SELECT 
    txn_id,
    merchant_clean as merchant_name,
    category_std as category_name,
    account_id,
    amount_usd,
    posted_at,
    transaction_date,
    month,
    year,
    day_of_week,
    
    -- Anomaly detection results
    anomaly_score,
    severity,
    anomaly_type,
    driver,
    remediation_hint,
    
    -- Individual anomaly flags
    is_statistical_outlier,
    is_amount_spike,
    is_novel_merchant,
    is_unusual_timing,
    is_high_frequency,
    is_category_mismatch,
    
    -- Statistical metrics
    category_z_score,
    merchant_z_score,
    category_amount_ratio,
    merchant_amount_ratio,
    category_avg_amount_30d,
    category_std_amount_30d,
    merchant_avg_amount_30d,
    merchant_std_amount_30d,
    
    -- Transaction counts
    merchant_transaction_count_30d,
    category_transaction_count_30d,
    
    -- Status and metadata
    CASE 
        WHEN severity = 'high' THEN 'requires_attention'
        WHEN severity = 'medium' THEN 'review_recommended'
        WHEN severity = 'low' THEN 'monitor'
        ELSE 'acknowledged'
    END as status,
    
    false as acknowledged,
    NULL as acknowledged_at,
    NULL as acknowledged_by,
    
    -- Metadata
    CURRENT_TIMESTAMP as flagged_at,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM anomaly_filtering
ORDER BY anomaly_score DESC, posted_at DESC
