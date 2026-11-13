{#- État courant de la source (colonnes étroites) : base de la détection de delta. -#}
select * from {% call pg_q() %}
  select source_key, row_hash, status from <<spec.schema>>.source_record
  where source_id = <<lit(src.id)>>
{% endcall %}