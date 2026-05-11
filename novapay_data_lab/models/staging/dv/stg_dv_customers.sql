{{ config(materialized='view') }}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model={
        'crm_sources': 'customers'
    },
    derived_columns={
        'RECORD_SOURCE': '!CRM_SYSTEM',
        'LOAD_DATETIME': 'current_timestamp()',
        'LOADED_AT': '_LOADED_AT',
        'CUSTOMER_ID': 'raw_data:customer_id::varchar',
        'ACCOUNT_ID': 'raw_data:account_id::varchar',
        'FIRST_NAME': 'raw_data:first_name::varchar',
        'LAST_NAME': 'raw_data:last_name::varchar',
        'EMAIL': 'raw_data:email::varchar',
        'PHONE_NUMBER': 'raw_data:phone::varchar',
        'BIRTH_DATE': 'raw_data:date_of_birth::date',
        'CITY': 'raw_data:address:city::varchar',
        'COUNTRY_CODE': 'raw_data:address:country::varchar',
        'JOINED_DATE': 'raw_data:customer_since::date',
        'IS_ACTIVE_FLAG': 'raw_data:is_active::boolean'
    },
    hashed_columns={
        'CUSTOMER_HK': 'CUSTOMER_ID',
        'ACCOUNT_HK': 'ACCOUNT_ID',
        'CUSTOMER_ACCOUNT_HK': {
            'columns': ['CUSTOMER_ID', 'ACCOUNT_ID']
        },
        'HASHDIFF': {
            'is_hashdiff': true,
            'columns': [
                'CUSTOMER_ID',
                'FIRST_NAME',
                'LAST_NAME',
                'EMAIL',
                'PHONE_NUMBER',
                'BIRTH_DATE',
                'CITY',
                'COUNTRY_CODE',
                'JOINED_DATE',
                'IS_ACTIVE_FLAG'
            ]
        }
    }
) }}