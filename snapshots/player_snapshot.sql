{% snapshot player_snapshot %}

{{
    config
    (
        strategy = 'check',
        unique_key = 'player_id',
        check_cols = ['player_position','team']
    )
}}


select * from {{source('players','src_players')}}


{% endsnapshot %}