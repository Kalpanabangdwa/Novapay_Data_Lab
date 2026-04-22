WITH source AS (

    SELECT *
    FROM {{ source('transaction_sources', 'accounts') }}

),

cleaned AS (

    SELECT
        account_id,
        customer_id,
        account_type,
        account_status,

        TRY_TO_DATE(opening_date) AS opening_date,
        TRY_TO_DATE(closing_date) AS closing_date,
        TRY_TO_NUMBER(current_balance) AS current_balance,
        TRY_TO_NUMBER(credit_limit) AS credit_limit,

        currency_code,
        risk_tier,
        kyc_status,
        TRY_TO_DATE(last_activity_date) AS last_activity_date,

        _LOADED_AT

    FROM source

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_sk,
        *
    FROM cleaned

)

SELECT * FROM final