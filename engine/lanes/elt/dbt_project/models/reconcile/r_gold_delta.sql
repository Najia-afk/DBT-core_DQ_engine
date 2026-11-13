<< push('_wr_gold_delta', analyze=False) >>
<# Une ligne par attribut dont la valeur change (golden non bloqué), sans auto-jointure. #>
<% set B = spec.blocked_sql('n') %>
select '{{ var("run_id") }}' as run_id, golden_id, attribute, old_value, new_value, winning_source
from (
<% for a in spec.attrs %>
  select
    n.staging_id as golden_id, <<lit(a.name)>> as attribute,
    <<canon('o.' ~ a.q, a.type)>> as old_value, <<canon('n.' ~ a.q, a.type)>> as new_value,
    n.<<a.qsrc>> as winning_source
  from {{ ref('r_gold_new') }} n
  left join {{ ref('r_gold_old') }} o on o.golden_id = n.staging_id
  where not <<B>> and n.<<a.q>> is distinct from o.<<a.q>>
<% if not loop.last %>
  union all
<% endif %>
<% endfor %>
)
