<< push('_wr_sm_del', analyze=False) >>
select o.source_id, o.source_key
from {{ ref('r_stg_members') }} o
where not exists (
  select 1 from {{ ref('r_sm_new') }} n where n.source_id = o.source_id and n.source_key = o.source_key
)
