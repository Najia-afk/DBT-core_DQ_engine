<< push('_wr_stg_ret', analyze=False) >>
{#- Stagings sans composante héritière ou absorbés : terminaux (leurs ids ne sont jamais réutilisés). -#}
select staging_id from {{ ref('r_oldids') }}
where staging_id not in (select staging_id from {{ ref('r_comp_sid') }})
