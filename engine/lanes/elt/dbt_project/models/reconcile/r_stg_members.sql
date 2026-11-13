{{ config(materialized='table') }}
{#- Tous les membres des stagings actuels des graines : seuls ces stagings sont relus. -#}
-- depends_on: {{ ref('r_oldids') }}
select * from {% call pg_q() %}
  select sm.staging_id, sm.source_id, sm.source_key
  from <<spec.schema>>.staging_member sm
  join <<spec.schema>>._wr_oldids o on o.staging_id = sm.staging_id
{% endcall %}
