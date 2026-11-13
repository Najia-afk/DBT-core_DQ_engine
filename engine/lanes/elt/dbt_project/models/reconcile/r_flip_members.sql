{{ config(materialized='table') }}
-- depends_on: {{ ref('r_flipped') }}
select * from {% call pg_q() %}
  select b.block_name, b.block_value, b.source_id, b.source_key
  from <<spec.schema>>.block_index b
  join <<spec.schema>>._wr_flipped f
    on f.block_name = b.block_name and f.block_value = b.block_value
{% endcall %}
