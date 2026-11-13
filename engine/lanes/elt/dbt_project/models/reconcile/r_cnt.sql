{#- Nombre de paires éligibles d'un nœud avec chaque source (règle de décision par source). -#}
select source_id, source_key, other_source, count(*) as n from (
  select left_source_id as source_id, left_key as source_key, right_source_id as other_source
  from {{ ref('r_elig_all') }}
  union all
  select right_source_id, right_key, left_source_id from {{ ref('r_elig_all') }}
) group by all
