<< push('_wr_r', index='source_id, source_key') >>
{#- Ensemble rescoré R = nœuds du delta + membres des blocs basculés. -#}
select source_id, source_key from {{ ref('r_d') }}
union
select source_id, source_key from {{ ref('r_extra') }}
