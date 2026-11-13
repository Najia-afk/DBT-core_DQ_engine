<< push('_wr_flipped', index='block_name, block_value') >>
{#- Blocs dont le statut « trop gros » a basculé : leurs membres sont rescorés. -#}
select block_name, block_value from {{ ref('r_blk_after') }} where before_big <> after_big
