{{ config(materialized='table') }}
-- depends_on: {{ ref('r_r') }}
select * from {% call pg_q() %}
  select mp.* from <<spec.schema>>.match_pair mp
  join <<spec.schema>>._wr_r r on r.source_id = mp.left_source_id and r.source_key = mp.left_key
  union
  select mp.* from <<spec.schema>>.match_pair mp
  join <<spec.schema>>._wr_r r on r.source_id = mp.right_source_id and r.source_key = mp.right_key
{% endcall %}
