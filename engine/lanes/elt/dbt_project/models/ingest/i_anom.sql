<< push('_wi_anom_' ~ src.id, analyze=False) >>
select '{{ var("run_id") }}' as run_id, 'batch_blocked' as kind, '<<src.id>>' as subject,
  'ratio=' || printf('%.4f', ratio) || ' min=<<min_ratio_text>>' as detail
from {{ ref('i_status') }} where blocked