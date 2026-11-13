{#- Nœuds dont le voisinage éligible change : R + extrémités des paires éligibles retirées/créées. -#}
select source_id, source_key from {{ ref('r_r') }}
union
select left_source_id, left_key from {{ ref('r_old_pairs_r') }} where eligible
union
select right_source_id, right_key from {{ ref('r_old_pairs_r') }} where eligible
union
select left_source_id, left_key from {{ ref('r_pairs_new') }} where eligible
union
select right_source_id, right_key from {{ ref('r_pairs_new') }} where eligible
