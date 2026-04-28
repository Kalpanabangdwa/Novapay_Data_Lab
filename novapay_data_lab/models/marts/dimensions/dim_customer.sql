WITH customers AS (

    SELECT *
    FROM {{ ref('stg_crm__customers') }}

),

latest_credit_score AS (

    SELECT
        customer_id,
        credit_score,
        score_provider
    FROM {{ ref('stg_vendor__credit_scores') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY _LOADED_AT DESC
    ) = 1

),

final AS (

    SELECT
        c.customer_sk,
        c.customer_id,

        c.first_name,
        c.last_name,
        CONCAT(c.first_name, ' ', c.last_name) AS full_name,

        c.email,
        c.phone,

        c.date_of_birth,

        c.city,
        c.state,
        c.country,
        c.zip_code,

        c.customer_since,
        c.customer_tier,
        c.is_active,

        cs.credit_score,
        cs.score_provider

    FROM customers c
    LEFT JOIN latest_credit_score cs
        ON c.customer_id = cs.customer_id

)

SELECT * FROM final