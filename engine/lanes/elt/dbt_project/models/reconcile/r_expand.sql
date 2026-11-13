<< push('_wr_expand', index='block_name, block_value') >>
{#- Blocs expansés en paires : seulement ceux de taille <= max_block_size (jamais tronqués). -#}
select block_name, block_value from {{ ref('r_sizes') }} where size_n <= <<spec.limit>>
