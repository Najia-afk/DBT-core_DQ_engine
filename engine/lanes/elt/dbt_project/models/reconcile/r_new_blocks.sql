{{ config(materialized='table') }}
{#- Blocs (clé de blocage, valeur) des nœuds actifs du delta ; une partie nulle = pas de clé. -#}
select * from (
<% for name, expr in spec.block_values_sql('d') %>
  select <<lit(name)>> as block_name, <<expr>> as block_value, d.source_id, d.source_key
  from {{ ref('r_d_rec') }} d where d.status = 'active'
<% if not loop.last %>
  union all
<% endif %>
<% endfor %>
) where block_value is not null
