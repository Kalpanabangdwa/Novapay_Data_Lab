{{ config(
    materialized='table',
    schema='DATA_VAULT'
) }}

{{ automate_dv.sat(
    src_pk='account_hk',
    src_hashdiff='hashdiff',
    src_payload=[
        'account_type',
        'account_status',
        'current_balance',
        'opened_date',
        'closed_date'
    ],
    src_ldts='loaded_at',
    src_source='record_source',
    source_model='stg_dv_accounts'
) }}