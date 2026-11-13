{#- Membres des blocs expansés APRÈS le delta : anciens membres hors delta + nœuds du delta
    qui entrent dans ces blocs. -#}
select m.block_name, m.block_value, m.source_id, m.source_key
from {{ ref('r_old_members') }} m
where not exists (
  select 1 from {{ ref('r_d') }} d where d.source_id = m.source_id and d.source_key = m.source_key
)
union all
select nb.block_name, nb.block_value, nb.source_id, nb.source_key
from {{ ref('r_new_blocks') }} nb
join {{ ref('r_expand') }} x on x.block_name = nb.block_name and x.block_value = nb.block_value
