<< push('_wr_blk_new', analyze=False) >>
select block_name, block_value, source_id, source_key from {{ ref('r_new_blocks') }}
