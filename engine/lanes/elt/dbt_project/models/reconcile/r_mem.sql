{{ config(materialized='table') }}
{#- Membres actifs des stagings recalculés : `rn` classe les membres d'un staging (revision la plus
    récente d'abord, nulle = la plus ancienne, puis nœud croissant) ; `src_no` = indice de la source. -#}
select
  s.staging_id, a.source_id, a.source_key<% for a in spec.attrs %>, a.<<a.q>><% endfor %>,
  cast(case a.source_id<% for s in spec.sources %> when <<lit(s.id)>> then <<spec.source_index[s.id]>><% endfor %> end as smallint) as src_no,
  row_number() over (
    partition by s.staging_id order by (a.revision is null), a.revision desc, n.nid asc
  ) as rn
from {{ ref('r_node_comp') }} n
join {{ ref('r_comp_sid') }} s on s.comp = n.comp
join {{ ref('r_arec') }} a on a.source_id = n.source_id and a.source_key = n.source_key
