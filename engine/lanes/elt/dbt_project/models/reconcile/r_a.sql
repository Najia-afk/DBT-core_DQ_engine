<< push('_wr_a', index='source_id, source_key') >>
{#- Ensemble affecté A = graines + membres des stagings actuels des graines. -#}
select source_id, source_key from {{ ref('r_seeds') }}
union
select source_id, source_key from {{ ref('r_stg_members') }}
