select block_name, block_value from {{ ref('r_new_blocks') }}
union
select block_name, block_value from {{ ref('r_extra_blocks') }}
