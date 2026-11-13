{{ config(materialized='table') }}
{#- Survie par attribut sans auto-jointure : un seul GROUP BY avec, par attribut, deux agrégats
    spécialisés (valeur et indice de la source gagnante) restreints aux valeurs non nulles. Les clés
    de tri sont des entiers : les agrégats sur structures/HUGEINT, générés par DuckDB avec des états
    `Value`, s'effondrent (x35) dès que le nombre de groupes x attributs devient grand. -#}
with g as (
  select m.staging_id<% for a in spec.attrs %>,
    arg_min(m.<<a.q>>, <<spec.survive_key(a)>>) filter (where m.<<a.q>> is not null) as <<a.q>>,
    arg_min(m.src_no, <<spec.survive_key(a)>>) filter (where m.<<a.q>> is not null) as <<q('n_' ~ a.name)>><% endfor %>

  from {{ ref('r_mem') }} m
  group by m.staging_id
)
select staging_id<% for a in spec.attrs %>,
  <<a.q>>,
  list_extract(<<spec.source_array>>, <<q('n_' ~ a.name)>> + 1) as <<a.qsrc>><% endfor %>

from g
