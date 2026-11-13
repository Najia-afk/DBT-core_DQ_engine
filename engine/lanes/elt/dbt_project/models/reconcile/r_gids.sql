<< push('_wr_gids', index='golden_id') >>
select staging_id as golden_id from {{ ref('r_comp_sid') }}
union
select staging_id from {{ ref('r_stg_ret') }}
