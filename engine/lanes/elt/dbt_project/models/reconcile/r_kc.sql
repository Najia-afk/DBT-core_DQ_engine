<< push('_wr_kc', index='source_id, source_key') >>
select k.source_id, k.source_key
from {{ ref('r_k') }} k
where not exists (
  select 1 from {{ ref('r_r') }} r where r.source_id = k.source_id and r.source_key = k.source_key
)
