{#- Paires éligibles après le delta pour les nœuds de K ∪ Y (nouvelles + stockées non retirées). -#}
select left_source_id, left_key, right_source_id, right_key from {{ ref('r_pairs_new') }}
where eligible
union
select left_source_id, left_key, right_source_id, right_key from {{ ref('r_pairs_kc') }}
where eligible
union
select e.left_source_id, e.left_key, e.right_source_id, e.right_key
from {{ ref('r_elig_y') }} e
where not exists (
    select 1 from {{ ref('r_r') }} r where r.source_id = e.left_source_id and r.source_key = e.left_key
  )
  and not exists (
    select 1 from {{ ref('r_r') }} r where r.source_id = e.right_source_id and r.source_key = e.right_key
  )
