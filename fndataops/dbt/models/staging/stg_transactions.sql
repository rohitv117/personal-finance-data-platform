-- Normalize the existing raw schema; do not infer balances absent from exports.
{{ config(alias='transactions') }}
WITH normalized AS (
    SELECT r.*,
        COALESCE(NULLIF(TRIM(merchant_raw), ''), 'Unknown') AS merchant_clean,
        COALESCE(NULLIF(category_raw, ''), 'Uncategorized') AS category_std,
        CASE WHEN institution ILIKE '%Express%' THEN 'card' ELSE 'bank' END AS source,
        DATE_TRUNC('month', posted_at)::date AS month,
        amount AS amount_usd,
        NULL::numeric AS balance_after
    FROM {{ source('raw', 'transactions') }} r
    WHERE currency = 'USD'
)
SELECT *, merchant_clean AS merchant_norm, category_std AS category_norm,
    ABS(amount) AS abs_amount,
    amount > 0 AS is_income, amount < 0 AS is_expense,
    category_std = 'Transfers' AS is_transfer,
    category_std = 'Investments' AS is_investment,
    category_std IN ('Utilities', 'Entertainment') AS is_recurring_guess
FROM normalized
