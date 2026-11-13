{#- Arêtes `match` du graphe sur A : paires stockées inchangées + décisions `match` recalculées. -#}
select s.left_source_id, s.left_key, s.right_source_id, s.right_key
from {{ ref('r_apairs') }} s
where not exists (
  select 1 from {{ ref('r_old_all') }} o
  where o.left_source_id = s.left_source_id and o.left_key = s.left_key
    and o.right_source_id = s.right_source_id and o.right_key = s.right_key
)
union all
select left_source_id, left_key, right_source_id, right_key
from {{ ref('r_redo') }} n
where decision = 'match' and (
  exists (select 1 from {{ ref('r_a') }} a where a.source_id = n.left_source_id and a.source_key = n.left_key)
  or exists (select 1 from {{ ref('r_a') }} a where a.source_id = n.right_source_id and a.source_key = n.right_key)
)
