{{ config(materialized='table', schema='DIMENSIONAL') }}

SELECT
    account_sk,
    account_id,
    customer_id,
    account_type,
    account_status,

    opening_date,
    closing_date,

    current_balance,
    credit_limit,

    currency_code,
    risk_tier,
    kyc_status,
    last_activity_date

FROM {{ ref('stg_txn_accounts') }}