{{ config(materialized='table') }}
-- depends_on: {{ ref('r_touched1') }}
select * from {% call pg_q() %}
  select t.block_name, t.block_value, count(b.source_id) as n
  from <<spec.schema>>._wr_touched1 t
  left join <<spec.schema>>.block_index b
    on b.block_name = t.block_name and b.block_value = t.block_value
  group by t.block_name, t.block_value
{% endcall %}
