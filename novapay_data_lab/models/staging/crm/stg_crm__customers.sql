WITH source AS (

    SELECT *
    FROM {{ source('crm_sources', 'customers') }}

),

flattened AS (

    SELECT
        raw_data:customer_id::STRING AS customer_id,
        raw_data:first_name::STRING AS first_name,
        raw_data:last_name::STRING AS last_name,
        raw_data:email::STRING AS email,
        raw_data:phone::STRING AS phone,
        raw_data:date_of_birth::DATE AS date_of_birth,

        raw_data:address:city::STRING AS city,
        raw_data:address:state::STRING AS state,
        raw_data:address:country::STRING AS country,
        raw_data:address:zip_code::STRING AS zip_code,

        raw_data:customer_since::DATE AS customer_since,
        raw_data:customer_tier::STRING AS customer_tier,
        raw_data:is_active::BOOLEAN AS is_active,

        _LOADED_AT

    FROM source

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
        *
    FROM flattened

)

SELECT * FROM final