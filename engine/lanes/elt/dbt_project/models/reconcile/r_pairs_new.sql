{{ config(materialized='table') }}
{#- Paires candidates (jointure par hachage sur les blocs) dont un nœud est rescoré, puis score
    pondéré, contradiction et éligibilité avec la sémantique du contrat (§3). -#}
<% set active, total, contra = spec.score_sql('a', 'b') %>
with mr as (
  select m.block_name, m.block_value, m.source_id, m.source_key
  from {{ ref('r_members_after') }} m
  join {{ ref('r_r') }} r on r.source_id = m.source_id and r.source_key = m.source_key
),
cand as (
  select distinct
    a.source_id as a_sid, a.source_key as a_key, b.source_id as b_sid, b.source_key as b_key
  from mr a
  join {{ ref('r_members_after') }} b
    on a.block_name = b.block_name and a.block_value = b.block_value
  where a.source_id <> b.source_id or a.source_key <> b.source_key
),
ordered as (
  select distinct
    case when (a_sid, a_key) < (b_sid, b_key) then a_sid else b_sid end as left_source_id,
    case when (a_sid, a_key) < (b_sid, b_key) then a_key else b_key end as left_key,
    case when (a_sid, a_key) < (b_sid, b_key) then b_sid else a_sid end as right_source_id,
    case when (a_sid, a_key) < (b_sid, b_key) then b_key else a_key end as right_key
  from cand
),
s as (
  select o.*, (<<active>>) as w, (<<total>>) as t, (<<contra>>) as contradiction
  from ordered o
  join {{ ref('r_vals') }} a on a.source_id = o.left_source_id and a.source_key = o.left_key
  join {{ ref('r_vals') }} b on b.source_id = o.right_source_id and b.source_key = o.right_key
),
sc as (
  select
    left_source_id, left_key, right_source_id, right_key, contradiction,
    case when w = 0.0 or w < <<spec.min_evidence>> then null else t / w end as score
  from s
)
select
  left_source_id, left_key, right_source_id, right_key, score, contradiction,
  (score is not null and score >= <<spec.accept>> and not contradiction) as eligible
from sc
