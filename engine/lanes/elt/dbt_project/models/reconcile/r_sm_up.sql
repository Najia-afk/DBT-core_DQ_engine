<< push('_wr_sm_up', analyze=False) >>
select n.staging_id, n.source_id, n.source_key
from {{ ref('r_sm_new') }} n
left join {{ ref('r_stg_members') }} o on o.source_id = n.source_id and o.source_key = n.source_key
where o.staging_id is distinct from n.staging_id
