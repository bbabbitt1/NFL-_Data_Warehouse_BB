{{
    config
    (
        materialized = 'view'
    )
}}

select * 
from {{ ref('customer') }}
where FIRST_NAME in ('Barry','Sarah','Bridget','Paul','Cindy','Anna')
