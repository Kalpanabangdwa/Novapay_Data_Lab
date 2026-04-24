SELECT
    country_code_2,
    country_code_3,
    country_name,
    region,
    sub_region,
    _LOADED_AT

FROM {{ source('reference_sources', 'country_codes') }}