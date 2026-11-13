{{ config(materialized='table') }}
-- depends_on: {{ ref('r_seeds') }}
select * from {% call pg_q() %}
  select sm.staging_id, sm.source_id, sm.source_key
  from <<spec.schema>>.staging_member sm
  join <<spec.schema>>._wr_seeds s on s.source_id = sm.source_id and s.source_key = sm.source_key
{% endcall %}
