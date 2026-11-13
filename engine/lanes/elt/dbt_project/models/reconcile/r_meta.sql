{{ config(materialized='table') }}
select * from {% call pg_q() %}
  select value as next_id from <<spec.schema>>.meta where key = 'next_staging_id'
{% endcall %}
