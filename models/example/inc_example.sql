{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}

with inc_example_src as(
    SELECT
    id,
    name,
    created_at,
    current_timestamp as insert_dts
    from {{source('inc_example','example_src')}}

    {% if is_incremental()%}
    where created_at > (SELECT MAX(insert_dts) FROM {{this}})
    {% endif %}
)

SELECT 
*
FROM inc_example_src
