<< push('_wr_seeds', index='source_id, source_key') >>
{#- Graines du reclustering : nœuds du delta + nœuds dont le statut `match` a changé. -#}
select source_id, source_key from {{ ref('r_d') }}
union
select source_id, source_key from {{ ref('r_match_touched') }}
