{#- Taille des blocs touchés avant/après le delta (après = avant - sortants + entrants). -#}
with o as (
  select block_name, block_value, count(*) as k from {{ ref('r_old_blocks') }} group by all
),
nw as (
  select block_name, block_value, count(*) as k from {{ ref('r_new_blocks') }} group by all
)
select
  c.block_name, c.block_value, c.n as before_n,
  c.n - coalesce(o.k, 0) + coalesce(nw.k, 0) as after_n,
  c.n > <<spec.limit>> as before_big,
  c.n - coalesce(o.k, 0) + coalesce(nw.k, 0) > <<spec.limit>> as after_big,
  nw.k is not null as entered
from {{ ref('r_cnt1') }} c
left join o on o.block_name = c.block_name and o.block_value = c.block_value
left join nw on nw.block_name = c.block_name and nw.block_value = c.block_value
