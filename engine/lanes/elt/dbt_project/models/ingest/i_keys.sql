{{ config(materialized='table') }}
{#- Clés, révision et ordinal de chaque ligne brute (colonnes étroites seulement). -#}
select k.* exclude (rev_present), (k.rev_present and k.rev is null) as rev_bad
from (
  select
    s._ord,
    nullif(cast(<<key_raw>> as varchar), '') as source_key,
    <<rev_sql>> as rev,
    <<rev_present_sql>> as rev_present
  from read_parquet('{{ var("raw_glob") }}') s
) k
