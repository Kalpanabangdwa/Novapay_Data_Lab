{{ config(
    materialized='table',
    schema='DATA_VAULT'
) }}

{{ automate_dv.hub(
    src_pk='account_hk',
    src_nk='account_id',
    src_ldts='_loaded_at',
    src_source='record_source',
    source_model='stg_dv_accounts'
) }}