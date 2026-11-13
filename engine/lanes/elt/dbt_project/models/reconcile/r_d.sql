select source_id, source_key, _seq as seq, status from {{ ref('r_d_rec') }}
