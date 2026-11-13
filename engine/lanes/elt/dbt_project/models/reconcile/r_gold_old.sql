{{ config(materialized='table') }}
-- depends_on: {{ ref('r_gids') }}
select * from {% call pg_q() %}
  select g.* from <<spec.schema>>.golden g
  join <<spec.schema>>._wr_gids i on i.golden_id = g.golden_id
{% endcall %}
