{{ config(
    materialized='incremental',
    unique_key='interaction_id',
    incremental_strategy='merge'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('crm_sources', 'customer_interactions') }}

    {% if is_incremental() %}
        WHERE _LOADED_AT >= (
            SELECT DATEADD(day, -3, MAX(_LOADED_AT)) FROM {{ this }}
        )
    {% endif %}

),

flattened AS (

    SELECT
        raw_data:interaction_id::STRING AS interaction_id,
        raw_data:customer_id::STRING AS customer_id,
        raw_data:interaction_type::STRING AS interaction_type,
        raw_data:interaction_date::DATE AS interaction_date,
        raw_data:subject::STRING AS subject,
        raw_data:status::STRING AS status,
        raw_data:priority::STRING AS priority,
        raw_data:resolution_time_hours::FLOAT AS resolution_time_hours,
        raw_data:csat_score::FLOAT AS csat_score,
        raw_data:agent_id::STRING AS agent_id,

        _LOADED_AT,
        _SOURCE_FILE

    FROM source

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['interaction_id']) }} AS interaction_sk,
        *
    FROM flattened

)

SELECT * FROM final