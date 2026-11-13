{{ config(materialized='table') }}
-- depends_on: {{ ref('r_pnodes') }}
select * from {% call pg_q() %}
  select sr.source_id, sr.source_key<% for a in spec.compare_attrs %>, sr.<<a.q>><% endfor %>

  from <<spec.schema>>.source_record sr
  join <<spec.schema>>._wr_pnodes p on p.source_id = sr.source_id and p.source_key = sr.source_key
{% endcall %}
