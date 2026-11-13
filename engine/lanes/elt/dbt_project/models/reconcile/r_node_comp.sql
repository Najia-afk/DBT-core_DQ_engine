{{ config(materialized='table') }}
{#- Rattachement : composante, ancien staging du nœud. -#}
select n.nid, n.source_id, n.source_key, c.lab as comp, m.staging_id as old_sid
from {{ ref('r_nodes') }} n
join {{ ref('r_comp') }} c on c.nid = n.nid
left join {{ ref('r_stg_members') }} m on m.source_id = n.source_id and m.source_key = n.source_key
