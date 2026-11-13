<< push('_wi_delta_' ~ src.id, analyze=False) >>
select run_id, source_id, source_key, kind, old_hash, new_hash from {{ ref('i_delta') }}