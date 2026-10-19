-- Fail instead of silently excluding transactions needing FX conversion.
SELECT txn_id FROM {{ source('raw', 'transactions') }} WHERE currency <> 'USD' OR currency IS NULL
