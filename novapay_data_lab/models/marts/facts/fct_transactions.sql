WITH source AS (

    SELECT *
    FROM {{ ref('stg_txn__payments') }}

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payment_id
        ORDER BY _LOADED_AT DESC
    ) = 1

),

final AS (

    SELECT
        payment_id AS txn_id,

        -- Foreign keys
        customer_id,
        account_id,
        merchant_id,

        -- Date key
        payment_date AS date_key,

        -- Metrics
        payment_amount AS amount,
        fee_amount,

        -- Derived fields
        CASE 
            WHEN payment_amount < 0 THEN 'refund'
            ELSE 'payment'
        END AS txn_type,

        payment_status,
        payment_method,
        is_international,
        risk_score,

        -- Audit
        CURRENT_TIMESTAMP AS loaded_at

    FROM deduplicated

)

SELECT * FROM final