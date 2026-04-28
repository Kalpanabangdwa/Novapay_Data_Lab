{% snapshot customer_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_tier',
            'city',
            'state',
            'country',
            'zip_code',
            'is_active'
        ]
    )
}}

select
    customer_id,

    -- Name fields
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,

    -- Address fields
    city,
    state,
    country,
    zip_code,
    concat(city, ', ', state, ', ', country) as full_address,

    -- Business attributes
    customer_tier,
    is_active

from {{ ref('stg_crm__customers') }}

{% endsnapshot %}