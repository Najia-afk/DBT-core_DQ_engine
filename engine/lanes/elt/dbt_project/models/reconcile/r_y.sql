<< push('_wr_y', index='source_id, source_key') >>
{#- Autres extrémités (hors K) des paires éligibles de K\R : leurs compteurs ne changent pas mais
    sont nécessaires à la décision. -#}
select y.source_id, y.source_key from (
  select left_source_id as source_id, left_key as source_key
  from {{ ref('r_pairs_kc') }} where eligible
  union
  select right_source_id, right_key from {{ ref('r_pairs_kc') }} where eligible
) y
where not exists (
  select 1 from {{ ref('r_k') }} k where k.source_id = y.source_id and k.source_key = y.source_key
)
