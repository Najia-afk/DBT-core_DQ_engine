{#- Compteurs du lot (IngestResult de la référence + volumes). -#}
with st as (select blocked, ratio from {{ ref('i_status') }}),
d as (select kind, count(*) as n from {{ ref('i_delta') }} group by kind)
select
  (select count(*) from {{ ref('i_keys') }}) as rows_raw,
  (select count(*) from {{ ref('i_keys') }} where source_key is null) as key_null,
  (select count(*) from {{ ref('i_keys') }} where source_key is not null and rev_bad) as revision_bad,
  (select count(*) from {{ ref('i_ranked') }} where rn > 1) as duplicates,
  (select count(*) from {{ ref('i_winners') }}) as winners,
  (select count(*) from {{ ref('i_valid') }}) as valid,
  coalesce((select n from d where kind = 'I'), 0) as inserted,
  coalesce((select n from d where kind = 'U'), 0) as updated,
  coalesce((select n from d where kind = 'D'), 0) as deleted,
  (select count(*) from {{ ref('i_valid') }}) - coalesce((select n from d where kind = 'I'), 0)
    - coalesce((select n from d where kind = 'U'), 0) as unchanged_raw,
  (select count(*) from {{ ref('i_winners') }}) - (select count(*) from {{ ref('i_valid') }}) as rejected_raw,
  (select blocked from st) as blocked,
  (select count(*) from {{ ref('i_rejects') }}) as reject_rows