{{
    config
    (
        materialized = 'table'
    )
}}

with customer_src as (
    select 
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    COUNTRY,
    CREATED_AT ,
    Current_TimeStamp as insert_dts 
    from {{source('customer','CUSTOMER_SRC')}}
)

SELECT 
*
FROM customer_src