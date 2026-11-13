<< push('_wr_touched1', index='block_name, block_value') >>
select block_name, block_value from {{ ref('r_old_blocks') }}
union
select block_name, block_value from {{ ref('r_new_blocks') }}
