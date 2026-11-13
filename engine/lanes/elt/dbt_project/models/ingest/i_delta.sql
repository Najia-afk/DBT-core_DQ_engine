{{ config(materialized='table') }}
{#- MERGE vs état courant : I (nouvelle clé ou réapparue), U (hash différent), D (absente d'un
    snapshot complet). Rien si le lot est bloqué. -#}
with cur as (select * from {{ ref('i_current') }}),
ins as (
  select v.source_key, cast(null as varchar) as old_hash, v.row_hash as new_hash, 'I' as kind
  from {{ ref('i_valid') }} v left join cur c on c.source_key = v.source_key
  where c.source_key is null or c.status = 'retired'
),
upd as (
  select v.source_key, c.row_hash as old_hash, v.row_hash as new_hash, 'U' as kind
  from {{ ref('i_valid') }} v join cur c on c.source_key = v.source_key
  where c.status = 'active' and c.row_hash <> v.row_hash
),
del as (
  select c.source_key, c.row_hash as old_hash, cast(null as varchar) as new_hash, 'D' as kind
  from cur c
  where <<'TRUE' if src.snapshot == 'full' else 'FALSE'>> and c.status = 'active'
    and not exists (select 1 from {{ ref('i_present') }} p where p.source_key = c.source_key)
)
select
  '{{ var("run_id") }}' as run_id, '<<src.id>>' as source_id, source_key, kind, old_hash, new_hash
from (select * from ins union all select * from upd union all select * from del)
where not (select blocked from {{ ref('i_status') }})