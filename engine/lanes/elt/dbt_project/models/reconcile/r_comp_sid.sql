{{ config(materialized='table') }}
{#- Identifiant de staging de chaque composante : le plus ancien parmi les stagings dont elle est
    l'héritière ; sinon un nouvel id alloué dans l'ordre du plus petit nœud. -#}
with comps as (select distinct comp from {{ ref('r_node_comp') }}),
cand as (select comp, min(old_sid) as keep_sid from {{ ref('r_heir') }} group by comp),
fresh as (
  select comp, cast(row_number() over (order by comp) - 1 as bigint) as rk
  from comps where comp not in (select comp from cand)
)
select
  c.comp,
  coalesce(cand.keep_sid, (select next_id from {{ ref('r_meta') }}) + f.rk) as staging_id,
  cand.keep_sid is null as is_new
from comps c
left join cand on cand.comp = c.comp
left join fresh f on f.comp = c.comp
