{{ config(materialized='table') }}
{#- Paires `match` stockées touchant A (le reste du graphe n'est pas lu). -#}
-- depends_on: {{ ref('r_a') }}
select * from {% call pg_q() %}
  select mp.* from <<spec.schema>>.match_pair mp
  join <<spec.schema>>._wr_a a on a.source_id = mp.left_source_id and a.source_key = mp.left_key
  where mp.decision = 'match'
  union
  select mp.* from <<spec.schema>>.match_pair mp
  join <<spec.schema>>._wr_a a on a.source_id = mp.right_source_id and a.source_key = mp.right_key
  where mp.decision = 'match'
{% endcall %}
