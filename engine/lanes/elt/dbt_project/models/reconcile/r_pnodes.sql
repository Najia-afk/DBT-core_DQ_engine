<< push('_wr_pnodes', index='source_id, source_key') >>
select distinct m.source_id, m.source_key
from {{ ref('r_members_after') }} m
where not exists (
  select 1 from {{ ref('r_d') }} d where d.source_id = m.source_id and d.source_key = m.source_key
)
