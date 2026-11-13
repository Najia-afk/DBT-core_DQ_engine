{#- Contrôle de complétude d'un snapshot complet : sous le ratio minimal, le lot est bloqué. -#}
with hit as (
  select count(*) as n_active, count(p.source_key) as n_hit
  from {{ ref('i_current') }} c
  left join {{ ref('i_present') }} p on p.source_key = c.source_key
  where c.status = 'active'
)
select
  <<'TRUE' if src.snapshot == 'full' else 'FALSE'>> as is_full,
  n_active, n_hit,
  case when n_active = 0 then null else cast(n_hit as double) / n_active end as ratio,
  (<<'TRUE' if src.snapshot == 'full' else 'FALSE'>> and n_active > 0
   and cast(n_hit as double) / n_active < <<min_ratio>>) as blocked
from hit