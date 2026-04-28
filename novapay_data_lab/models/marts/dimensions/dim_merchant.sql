SELECT
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_country,
    merchant_state,
    merchant_city,
    mcc_code,
    is_active,
    onboarding_date,
    risk_rating
FROM {{ ref('stg_txn_merchants') }}