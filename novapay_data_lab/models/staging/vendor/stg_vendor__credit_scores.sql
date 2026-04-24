SELECT
    customer_id,
    TRY_TO_DATE(score_date) AS score_date,
    TRY_TO_NUMBER(credit_score) AS credit_score,
    score_provider,
    risk_category,
    TRY_TO_BOOLEAN(delinquency_flag) AS delinquency_flag,
    TRY_TO_BOOLEAN(bankruptcy_flag) AS bankruptcy_flag,
    TRY_TO_NUMBER(inquiry_count) AS inquiry_count,
    TRY_TO_NUMBER(total_accounts) AS total_accounts,
    _LOADED_AT

FROM {{ source('vendor_sources', 'credit_scores') }}