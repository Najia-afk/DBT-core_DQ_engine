{#- Clés présentes dans le lot (y compris les lignes rejetées) : base de la complétude. -#}
select distinct source_key from {{ ref('i_keys') }} where source_key is not null