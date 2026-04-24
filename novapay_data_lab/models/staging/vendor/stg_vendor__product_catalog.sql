SELECT
    product_id,
    product_name,
    product_category,
    TRY_TO_NUMBER(monthly_fee) AS monthly_fee,
    TRY_TO_NUMBER(transaction_fee) AS transaction_fee,
    TRY_TO_BOOLEAN(is_active) AS is_active,
    TRY_TO_DATE(launch_date) AS launch_date,
    TRY_TO_DATE(retirement_date) AS retirement_date,
    _LOADED_AT

FROM {{ source('vendor_sources', 'product_catalog') }}