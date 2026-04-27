SELECT *
FROM {{ ref('stg_txn_accounts') }}
WHERE account_status = 'closed'
  AND closing_date IS NULL