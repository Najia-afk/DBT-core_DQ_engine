{{ config(materialized='table') }}
-- depends_on: {{ ref('r_arest') }}
select * from {% call pg_q() %}
  select sr.* from <<spec.schema>>.source_record sr
  join <<spec.schema>>._wr_arest a on a.source_id = sr.source_id and a.source_key = sr.source_key
{% endcall %}
