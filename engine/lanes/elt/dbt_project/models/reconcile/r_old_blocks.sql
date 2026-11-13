{{ config(materialized='table') }}
-- depends_on: {{ ref('r_d_rec') }}
select * from {% call pg_q() %}
  select b.block_name, b.block_value, b.source_id, b.source_key
  from <<spec.schema>>.block_index b
  join <<spec.schema>>.pending_delta p
    on p.source_id = b.source_id and p.source_key = b.source_key
{% endcall %}
