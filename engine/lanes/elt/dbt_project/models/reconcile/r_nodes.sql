{{ config(materialized='table') }}
{#- Nœuds actifs de A, numérotés dans l'ordre (source_id, source_key) : nid = rang du nœud. -#}
select
  cast(row_number() over (order by source_id, source_key) as bigint) as nid, source_id, source_key
from {{ ref('r_arec') }}
where status = 'active'
