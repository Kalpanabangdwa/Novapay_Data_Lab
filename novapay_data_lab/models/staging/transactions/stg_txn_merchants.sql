SELECT
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_country,
    merchant_state,
    merchant_city,
    mcc_code,
    TRY_TO_BOOLEAN(is_active) AS is_active,
    TRY_TO_DATE(onboarding_date) AS onboarding_date,
    risk_rating,
    _LOADED_AT

FROM {{ source('transaction_sources', 'merchants') }}