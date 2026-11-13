<< push('_wi_up_' ~ src.id, analyze=False) >>
{#- Lignes à écrire dans source_record (I et U), colonnes dans l'ordre de la table. -#}
select
  '<<src.id>>' as source_id, v.source_key, v.row_hash, v.revision, 'active' as status,
  '{{ var("run_id") }}' as first_run, '{{ var("run_id") }}' as last_change_run<% for a in spec.attrs %>,
  v.<<a.q>><% endfor %>

from {{ ref('i_valid') }} v
join {{ ref('i_delta') }} d on d.source_key = v.source_key and d.kind in ('I', 'U')