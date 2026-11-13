{#- Valeurs de blocage/comparaison des nœuds candidats (delta + partenaires). -#}
select source_id, source_key<% for a in spec.compare_attrs %>, <<a.q>><% endfor %>

from {{ ref('r_d_rec') }} where status = 'active'
union all
select source_id, source_key<% for a in spec.compare_attrs %>, <<a.q>><% endfor %>

from {{ ref('r_pvals') }}
