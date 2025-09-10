{{
  config(
    materialized='incremental',
    unique_key='id'
  )
}}

WITH daily_category_stats AS (
    SELECT 
        DATE(posted_at) as date,
        category_norm,
        AVG(ABS(amount)) as avg_amount,
        STDDEV(ABS(amount)) as std_amount,
        COUNT(*) as transaction_count
    FROM {{ ref('stg_transactions') }}
    WHERE amount < 0  -- Only expenses for anomaly detection
    {% if is_incremental() %}
        AND DATE(posted_at) > (SELECT MAX(flagged_at::date) FROM {{ this }})
    {% endif %}
    GROUP BY DATE(posted_at), category_norm
    HAVING COUNT(*) >= 3  -- Need at least 3 transactions for statistical analysis
),

anomaly_candidates AS (
    SELECT 
        t.txn_id,
        t.posted_at,
        t.merchant_norm,
        t.category_norm,
        t.amount,
        t.abs_amount,
        dcs.avg_amount,
        dcs.std_amount,
        dcs.transaction_count,
        -- Z-score calculation
        CASE 
            WHEN dcs.std_amount > 0 THEN 
                (t.abs_amount - dcs.avg_amount) / dcs.std_amount
            ELSE 0 
        END as z_score,
        -- IQR calculation (simplified)
        CASE 
            WHEN dcs.std_amount > 0 AND t.abs_amount > dcs.avg_amount + (2 * dcs.std_amount) THEN TRUE
            ELSE FALSE
        END as is_outlier
    FROM {{ ref('stg_transactions') }} t
    JOIN daily_category_stats dcs ON DATE(t.posted_at) = dcs.date AND t.category_norm = dcs.category_norm
    WHERE t.amount < 0  -- Only expenses
),

novel_merchants AS (
    SELECT 
        t.txn_id,
        t.posted_at,
        t.merchant_norm,
        t.category_norm,
        t.amount,
        'Novel Merchant' as anomaly_type,
        'Medium' as severity,
        'Merchant not seen in last 90 days' as driver,
        'Review transaction and categorize appropriately' as remediation_hint
    FROM {{ ref('stg_transactions') }} t
    WHERE t.merchant_norm IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_transactions') }} t2
        WHERE t2.merchant_norm = t.merchant_norm
        AND t2.posted_at < t.posted_at
        AND t2.posted_at >= t.posted_at - INTERVAL '90 days'
    )
    {% if is_incremental() %}
        AND t.posted_at > (SELECT MAX(flagged_at) FROM {{ this }})
    {% endif %}
),

statistical_anomalies AS (
    SELECT 
        txn_id,
        posted_at,
        merchant_norm,
        category_norm,
        amount,
        CASE 
            WHEN ABS(z_score) > 3 THEN 'High'
            WHEN ABS(z_score) > 2 THEN 'Medium'
            ELSE 'Low'
        END as severity,
        'Statistical Outlier' as anomaly_type,
        CASE 
            WHEN z_score > 0 THEN 'Unusually high expense'
            ELSE 'Unusually low expense'
        END as driver,
        CASE 
            WHEN z_score > 0 THEN 'Verify transaction amount and necessity'
            ELSE 'Check for missing transactions or refunds'
        END as remediation_hint
    FROM anomaly_candidates
    WHERE is_outlier = TRUE OR ABS(z_score) > 2
)

SELECT 
    nextval('marts.mart_anomalies_id_seq') as id,
    txn_id,
    anomaly_type,
    severity,
    driver,
    remediation_hint,
    CURRENT_TIMESTAMP as flagged_at
FROM (
    SELECT * FROM statistical_anomalies
    UNION ALL
    SELECT * FROM novel_merchants
) all_anomalies
ORDER BY posted_at DESC 