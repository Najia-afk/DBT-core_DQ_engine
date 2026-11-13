{{ config(materialized='table') }}
{#- Conversion, normalisation et contrôles DQ de toutes les colonnes, en un seul passage vectorisé.
    Niveaux : c (conversion) -> n (normalisation, vide = NULL) -> k (indicateurs d'échec). -#}
with src as (
  select s.* from read_parquet('{{ var("raw_glob") }}') s
  semi join {{ ref('i_winners') }} w on w._ord = s._ord
),
c as (
  select
    s._ord,
    nullif(cast(<<key_raw>> as varchar), '') as source_key,
    <<rev_sql>> as rev<% for c in cols %>,
    <<c.cast_sql>> as <<c.cq>><% if c.can_fail %>,
    <<c.raw_q>> is not null as <<c.rpq>><% endif %><% endfor %>

  from src s
),
n as (
  select
    c._ord, c.source_key, c.rev<% for c in cols %>,
    <<c.norm_sql('c')>> as <<c.nq>><% if c.can_fail %>,
    (c.<<c.rpq>> and c.<<c.cq>> is null) as <<c.cfq>><% endif %><% endfor %>

  from c
),
k as (
  select
    n.*<% for c in cols %><% for chk in c.checks %>,
    (n.<<c.nq>> is not null and not (<<c.pass_sql(chk, 'n')>>)<% if chk.stops %> and not (<<chk.stops>>)<% endif %>) as <<chk.fq>><% endfor %><% endfor %>

  from n
)
select
  k._ord, k.source_key, k.rev<% for c in cols %>,
  <<c.final_sql>> as <<c.vq>><% if c.can_fail %>,
  k.<<c.cfq>><% endif %><% for chk in c.checks %>,
  k.<<chk.fq>><% endfor %><% endfor %>

from k
