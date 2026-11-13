{{ config(materialized='table') }}
{#- Enregistrements du delta en attente (lignes complètes), lus côté PostgreSQL : seulement
    les nœuds touchés quittent la base d'état. -#}
select * from {% call pg_q() %}
  select p.seq as _seq, sr.*
  from <<spec.schema>>.source_record sr
  join <<spec.schema>>.pending_delta p
    on p.source_id = sr.source_id and p.source_key = sr.source_key
{% endcall %}
