{{ config(materialized='table') }}
{#- Doublons de clé : la plus grande revision (nulle = plus ancienne), puis la dernière ligne. -#}
select
  _ord, source_key, rev,
  row_number() over (
    partition by source_key order by (rev is not null) desc, rev desc, _ord desc
  ) as rn
from {{ ref('i_keys') }}
where source_key is not null and not rev_bad