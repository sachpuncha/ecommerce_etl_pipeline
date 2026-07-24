with source as (
    select * from {{ source('bronze', 'staging_raw_data') }}
)

select * from source