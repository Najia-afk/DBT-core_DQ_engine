{#- Taille (après delta) des blocs des nœuds rescorés. -#}
select a.block_name, a.block_value, a.after_n as size_n
from {{ ref('r_blk_after') }} a
join {{ ref('r_t2') }} t on t.block_name = a.block_name and t.block_value = a.block_value
union all
select block_name, block_value, n as size_n from {{ ref('r_cnt2') }}
