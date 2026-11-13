{#- Invariant du contrat : une composante `match` ne franchit jamais la frontière de A. -#}
select case when n > 0 then error('invariant broken: a match edge leaves the affected set') else n end as bad
from (
  select count(*) as n from {{ ref('r_edges_pairs') }} e
  where not exists (
      select 1 from {{ ref('r_a') }} a where a.source_id = e.left_source_id and a.source_key = e.left_key
    )
    or not exists (
      select 1 from {{ ref('r_a') }} a where a.source_id = e.right_source_id and a.source_key = e.right_key
    )
)
