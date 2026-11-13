<< push('_wr_stg_new', analyze=False) >>
select staging_id, 'active' as status, '{{ var("run_id") }}' as created_run
from {{ ref('r_comp_sid') }} where is_new
