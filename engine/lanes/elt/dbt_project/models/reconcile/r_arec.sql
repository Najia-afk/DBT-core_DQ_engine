{{ config(materialized='table') }}
{#- Enregistrements (valeurs) de A : delta déjà lu + autres membres des stagings touchés. -#}
select source_id, source_key, revision, status<% for a in spec.attrs %>, <<a.q>><% endfor %>

from {{ ref('r_d_rec') }}
union all
select source_id, source_key, revision, status<% for a in spec.attrs %>, <<a.q>><% endfor %>

from {{ ref('r_a_rec') }}
