{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key='team_id'
    )
}}

with dim_teams as(
    SELECT
        team_id,
        team_name,
        conference,
        division,
        created_at,
        current_timestamp as insert_dts,
        current_timestamp as update_dts

    FROM {{source("dim_teams","src_team")}}

    {%if is_incremental()%}
    where created_at > (select max(insert_dts) from {{this}})
    {% endif %}
)

SELECT
    team_id,
    team_name,
    concat(conference,'-',division) as conference_division,
    created_at,
    insert_dts
FROM dim_teams