{{ config(
    materialized='incremental',
    schema='DIMENSIONAL',
    unique_key='payment_id',
    incremental_strategy='merge'
) }}

with source as (

    select *
    from {{ ref('int_payments_enriched') }}

    {% if is_incremental() %}
        where payment_date >= (
            select dateadd(day, -3, max(payment_date)) from {{ this }}
        )
    {% endif %}

)

select
    payment_id,
    customer_id,
    account_id,
    merchant_id,
    payment_date as date_key,
    payment_amount,
    fee_amount,
    net_amount,
    is_refund
from source