<< push('_wr_gold_ret', analyze=False) >>
select staging_id as golden_id from {{ ref('r_stg_ret') }}
