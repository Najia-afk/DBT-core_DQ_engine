<< push('_wr_meta', analyze=False) >>
select (select next_id from {{ ref('r_meta') }})
  + (select count(*) from {{ ref('r_comp_sid') }} where is_new) as next_id
