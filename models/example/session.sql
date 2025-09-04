

with session_src as (
    select
        session_id,
        user_id,
        browser,
        device_type,
        s.country_code,
        c.country_name,
        c.continent,
        c.currency,
        browse_length_min,
        pages_visited,
        CURRENT_TIMESTAMP as insert_dts
    from dbt_db.public.session_src s
    left join dbt_db.public.country_code c
    on s.country_code = c.country_code
)
Select * From session_src