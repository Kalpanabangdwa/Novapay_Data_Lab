{{ config(materialized='view') }}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model={
        'transaction_sources': 'accounts'
    },
    derived_columns={
        'RECORD_SOURCE': '!TRANSACTIONS_SYSTEM',
        'LOAD_DATETIME': 'current_timestamp()',
        'LOADED_AT': '_LOADED_AT',
        'OPENED_DATE': 'OPENING_DATE',
        'CLOSED_DATE': 'CLOSING_DATE'
    },
    hashed_columns={
        'ACCOUNT_HK': 'ACCOUNT_ID',
        'CUSTOMER_HK': 'CUSTOMER_ID',
        'CUSTOMER_ACCOUNT_HK': {
            'columns': ['CUSTOMER_ID', 'ACCOUNT_ID']
        },
        'HASHDIFF': {
            'is_hashdiff': true,
            'columns': [
                'ACCOUNT_ID',
                'CUSTOMER_ID',
                'ACCOUNT_TYPE',
                'ACCOUNT_STATUS',
                'CURRENT_BALANCE',
                'OPENED_DATE',
                'CLOSED_DATE'
            ]
        }
    }
) }}