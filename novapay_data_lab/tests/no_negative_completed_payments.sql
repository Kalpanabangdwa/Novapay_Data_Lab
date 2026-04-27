SELECT *
FROM {{ ref('stg_txn__payments') }}
WHERE payment_status = 'completed'
  AND payment_amount < 0