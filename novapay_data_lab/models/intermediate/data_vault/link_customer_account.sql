{{ config(
    materialized='table',
    schema='DATA_VAULT'
) }}

{{ automate_dv.link(
    src_pk='CUSTOMER_ACCOUNT_HK',
    src_fk=[
        'CUSTOMER_HK',
        'ACCOUNT_HK'
    ],
    src_ldts='LOADED_AT',
    src_source='RECORD_SOURCE',
    source_model=[
        'stg_dv_customers',
        'stg_dv_accounts'
    ]
) }}