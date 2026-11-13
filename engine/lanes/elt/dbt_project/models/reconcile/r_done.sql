<< push('_wr_done', analyze=False) >>
select source_id, source_key, seq from {{ ref('r_d') }}
