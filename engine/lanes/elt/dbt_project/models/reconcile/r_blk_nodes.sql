<< push('_wr_blk_nodes', analyze=False) >>
select source_id, source_key from {{ ref('r_d') }}
