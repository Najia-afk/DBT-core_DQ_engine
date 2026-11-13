<< push('_wr_t2rest', index='block_name, block_value') >>
select t.block_name, t.block_value
from {{ ref('r_t2') }} t
where not exists (
  select 1 from {{ ref('r_blk_after') }} a
  where a.block_name = t.block_name and a.block_value = t.block_value
)
