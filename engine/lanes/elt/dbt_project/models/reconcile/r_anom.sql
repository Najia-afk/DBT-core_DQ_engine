<< push('_wr_anom', analyze=False) >>
<# Anomalies : blocs trop gros (entrés ou basculés) et goldens bloqués. #>
<% set B = spec.blocked_sql('n') %>
select '{{ var("run_id") }}' as run_id, 'oversized_block' as kind,
  replace(block_name || '=' || block_value, chr(31), '|') as subject,
  'size=' || cast(after_n as varchar) || ' max=<<spec.limit>>' as detail
from {{ ref('r_blk_after') }}
where after_big and (entered or before_big <> after_big)
union all
select '{{ var("run_id") }}', 'golden_blocked', <<spec.blocked_names_sql('n')>>,
  'golden_id=' || cast(n.staging_id as varchar)
from {{ ref('r_gold_new') }} n
where <<B>>
