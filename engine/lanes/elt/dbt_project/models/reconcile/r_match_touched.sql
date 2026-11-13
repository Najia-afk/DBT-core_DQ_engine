{#- Nœuds dont le statut `match` d'au moins une paire a changé : graines du reclustering. -#}
with diff as (
  select
    coalesce(o.left_source_id, n.left_source_id) as ls, coalesce(o.left_key, n.left_key) as lk,
    coalesce(o.right_source_id, n.right_source_id) as rs, coalesce(o.right_key, n.right_key) as rk
  from {{ ref('r_old_all') }} o
  full join {{ ref('r_redo') }} n
    on o.left_source_id = n.left_source_id and o.left_key = n.left_key
   and o.right_source_id = n.right_source_id and o.right_key = n.right_key
  where coalesce(o.decision = 'match', false) <> coalesce(n.decision = 'match', false)
)
select ls as source_id, lk as source_key from diff
union
select rs, rk from diff
