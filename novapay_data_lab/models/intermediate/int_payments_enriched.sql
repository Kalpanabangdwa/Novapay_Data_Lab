{{ config(
    materialized='view'
) }}

with source as (

    select *
    from {{ ref('stg_txn__payments') }}

),

final as (

    select
        payment_id,
        customer_id,
        account_id,
        merchant_id,
        payment_date,
        payment_amount,
        fee_amount,
        payment_amount - fee_amount as net_amount,
        case 
            when payment_amount < 0 then true 
            else false 
        end as is_refund
    from source

)

select * from final