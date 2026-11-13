{{ config(materialized='table') }}
{#- Décision de chaque paire recalculée (contrat §3 + règle « un candidat éligible par source »),
    comparée à sa version stockée : `sig_changed` = à (ré)écrire. -#}
with pool as (
  select left_source_id, left_key, right_source_id, right_key, score, contradiction, eligible
  from {{ ref('r_pairs_new') }}
  union all
  select left_source_id, left_key, right_source_id, right_key, score, contradiction, eligible
  from {{ ref('r_pairs_kc') }}
),
c as (
  select p.*, coalesce(cl.n, 0) as n_l, coalesce(cr.n, 0) as n_r
  from pool p
  left join {{ ref('r_cnt') }} cl
    on cl.source_id = p.left_source_id and cl.source_key = p.left_key
   and cl.other_source = p.right_source_id
  left join {{ ref('r_cnt') }} cr
    on cr.source_id = p.right_source_id and cr.source_key = p.right_key
   and cr.other_source = p.left_source_id
),
d as (
  select c.*,
    case
      when score is null then 'none'
      when eligible then case when n_l = 1 and n_r = 1 then 'match' else 'review' end
      when score >= <<spec.review>> then 'review'
      else 'none'
    end as decision,
    case
      when score is null then ''
      when eligible then case when n_l = 1 and n_r = 1 then '' else 'multiple_candidates' end
      when score >= <<spec.review>> then case when contradiction then 'contradiction' else 'score_band' end
      else ''
    end as reason
  from c
),
j as (
  select d.*,
    (o.left_key is null
      or o.decision is distinct from d.decision or o.reason is distinct from d.reason
      or o.score is distinct from d.score
      or o.contradiction is distinct from d.contradiction) as sig_changed,
    o.run_id as old_run_id
  from d
  left join {{ ref('r_old_all') }} o
    on o.left_source_id = d.left_source_id and o.left_key = d.left_key
   and o.right_source_id = d.right_source_id and o.right_key = d.right_key
)
select
  left_source_id, left_key, right_source_id, right_key, score, contradiction, eligible,
  decision, reason, sig_changed,
  case when sig_changed then '{{ var("run_id") }}' else old_run_id end as run_id
from j
