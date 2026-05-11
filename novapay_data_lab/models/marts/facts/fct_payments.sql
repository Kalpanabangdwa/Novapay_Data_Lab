{{ config(
    materialized='incremental',
    schema='DIMENSIONAL',
    unique_key='payment_id',
    incremental_strategy='merge'
) }}

with max_date as (

    {% if is_incremental() %}

        select
            dateadd(day, -3, max(date_key)) as max_payment_date
        from {{ this }}

    {% else %}

        select null as max_payment_date

    {% endif %}

),

source as (

    select *
    from {{ ref('int_payments_enriched') }}

    {% if is_incremental() %}

        where payment_date >= (
            select max_payment_date
            from max_date
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