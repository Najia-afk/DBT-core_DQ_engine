{{ config(materialized='table') }}
-- depends_on: {{ ref('r_expand') }}
select * from {% call pg_q() %}
  select b.block_name, b.block_value, b.source_id, b.source_key
  from <<spec.schema>>.block_index b
  join <<spec.schema>>._wr_expand x
    on x.block_name = b.block_name and x.block_value = b.block_value
{% endcall %}
