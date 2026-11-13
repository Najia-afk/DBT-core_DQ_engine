<< push('_wr_gold_up', analyze=False) >>
<# Golden à écrire seulement s'il change (statut, stale, valeur ou source gagnante). Un golden
   bloqué conserve ses anciennes valeurs et devient périmé s'il avait déjà été publié. #>
<% set B = spec.blocked_sql('n') %>
<% set once = "coalesce(o.status = 'published' or (o.status = 'blocked' and o.stale), false)" %>
select golden_id, status, stale<% for a in spec.attrs %>, <<a.q>><% if spec.provenance %>, <<a.qsrc>><% endif %><% endfor %>

from (
  select
    n.staging_id as golden_id,
    case when <<B>> then 'blocked' else 'published' end as status,
    case when <<B>> then <<once>> else false end as stale<% for a in spec.attrs %>,
    case when <<B>> then o.<<a.q>> else n.<<a.q>> end as <<a.q>><% if spec.provenance %>,
    case when <<B>> then o.<<a.qsrc>> else n.<<a.qsrc>> end as <<a.qsrc>><% endif %><% endfor %>,
    (o.golden_id is null or case
      when <<B>> then (o.status is distinct from 'blocked' or o.stale is distinct from <<once>>)
      else (o.status is distinct from 'published' or o.stale<% for a in spec.attrs %>
        or n.<<a.q>> is distinct from o.<<a.q>><% if spec.provenance %>
        or n.<<a.qsrc>> is distinct from o.<<a.qsrc>><% endif %><% endfor %>)
    end) as _changed
  from {{ ref('r_gold_new') }} n
  left join {{ ref('r_gold_old') }} o on o.golden_id = n.staging_id
)
where _changed
