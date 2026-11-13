{{ config(materialized='table') }}
-- depends_on: {{ ref('r_extra') }}
select * from {% call pg_q() %}
  select b.block_name, b.block_value, b.source_id, b.source_key
  from <<spec.schema>>.block_index b
  join <<spec.schema>>._wr_extra e on e.source_id = b.source_id and e.source_key = b.source_key
{% endcall %}
