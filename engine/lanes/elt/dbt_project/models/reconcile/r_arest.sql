<< push('_wr_arest', index='source_id, source_key') >>
select a.source_id, a.source_key
from {{ ref('r_a') }} a
where not exists (
  select 1 from {{ ref('r_d') }} d where d.source_id = a.source_id and d.source_key = a.source_key
)
