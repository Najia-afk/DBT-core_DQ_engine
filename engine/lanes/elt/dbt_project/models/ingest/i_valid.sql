{{ config(materialized='table') }}
{#- Enregistrements valides (aucun rejet `reject_record`) avec leur row_hash. -#}
select
  '<<src.id>>' as source_id,
  n.source_key,
  <<row_hash>> as row_hash,
  n.rev as revision<% for a in spec.attrs %>,
  <<value_of[a.name]>> as <<a.q>><% endfor %>

from {{ ref('i_norm') }} n
where not (<<reject_any>>)