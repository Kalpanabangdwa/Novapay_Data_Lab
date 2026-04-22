{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('transaction_sources', 'payments') }}

    {% if is_incremental() %}
        WHERE _LOADED_AT >= (
            SELECT DATEADD(day, -3, MAX(_LOADED_AT)) FROM {{ this }}
        )
    {% endif %}

),

typed AS (

    SELECT
        payment_id,
        customer_id,
        account_id,
        merchant_id,

        TRY_TO_NUMBER(payment_amount) AS payment_amount,
        payment_currency,
        payment_status,
        payment_method,

        TRY_TO_DATE(payment_date) AS payment_date,
        TRY_TO_TIMESTAMP(payment_timestamp) AS payment_timestamp,

        merchant_category,
        reference_number,

        TRY_TO_NUMBER(fee_amount) AS fee_amount,
        TRY_TO_BOOLEAN(is_international) AS is_international,
        TRY_TO_NUMBER(risk_score) AS risk_score,

        _LOADED_AT,
        _SOURCE_FILE

    FROM source

),

deduplicated AS (

    SELECT *
    FROM typed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payment_id
        ORDER BY _LOADED_AT DESC
    ) = 1

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['payment_id']) }} AS payment_sk,
        *
    FROM deduplicated

)

SELECT * FROM final