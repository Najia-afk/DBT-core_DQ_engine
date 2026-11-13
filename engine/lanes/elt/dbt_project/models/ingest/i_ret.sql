<< push('_wi_ret_' ~ src.id, analyze=False) >>
select source_key from {{ ref('i_delta') }} where kind = 'D'