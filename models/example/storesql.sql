with source_store_sales as (
    select * from {{source('snowflake_sample_data', 'store_sales')}}
)
 
select * from source_store_sales