<< push('_wr_oldids', index='staging_id') >>
select distinct staging_id from {{ ref('r_seed_stg') }}
