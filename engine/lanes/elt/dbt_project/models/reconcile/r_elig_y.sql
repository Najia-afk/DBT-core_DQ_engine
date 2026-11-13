{{ config(materialized='table') }}
-- depends_on: {{ ref('r_y') }}
select * from {% call pg_q() %}
  select mp.* from <<spec.schema>>.match_pair mp
  join <<spec.schema>>._wr_y y on y.source_id = mp.left_source_id and y.source_key = mp.left_key
  where mp.eligible
  union
  select mp.* from <<spec.schema>>.match_pair mp
  join <<spec.schema>>._wr_y y on y.source_id = mp.right_source_id and y.source_key = mp.right_key
  where mp.eligible
{% endcall %}
