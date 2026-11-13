{{ config(materialized='table') }}
{#- Paires stockées touchant K\R (et aucun nœud de R) : leurs décisions sont recalculées. -#}
-- depends_on: {{ ref('r_kc') }}
select p.* from (
  select * from {% call pg_q() %}
    select mp.* from <<spec.schema>>.match_pair mp
    join <<spec.schema>>._wr_kc k on k.source_id = mp.left_source_id and k.source_key = mp.left_key
    union
    select mp.* from <<spec.schema>>.match_pair mp
    join <<spec.schema>>._wr_kc k on k.source_id = mp.right_source_id and k.source_key = mp.right_key
  {% endcall %}
) p
where not exists (
    select 1 from {{ ref('r_r') }} r where r.source_id = p.left_source_id and r.source_key = p.left_key
  )
  and not exists (
    select 1 from {{ ref('r_r') }} r where r.source_id = p.right_source_id and r.source_key = p.right_key
  )
