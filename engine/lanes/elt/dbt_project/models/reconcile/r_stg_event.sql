<< push('_wr_stg_event', analyze=False) >>
{#- Événements : merge (id conservé, id absorbé), split (parent, nouvel id), create. -#}
select '{{ var("run_id") }}' as run_id, 'merge' as kind, k.keep_sid as staging_id,
  h.old_sid as related_id
from {{ ref('r_heir') }} h
join (select comp, min(old_sid) as keep_sid from {{ ref('r_heir') }} group by comp) k
  on k.comp = h.comp
where h.old_sid <> k.keep_sid
union all
select '{{ var("run_id") }}', case when p.parent is null then 'create' else 'split' end,
  case when p.parent is null then s.staging_id else p.parent end,
  case when p.parent is null then cast(null as bigint) else s.staging_id end
from {{ ref('r_comp_sid') }} s
left join (
  select comp, old_sid as parent from (
    select comp, old_sid, row_number() over (partition by comp order by n desc, old_sid asc) as rn
    from {{ ref('r_overlap') }}
  ) where rn = 1
) p on p.comp = s.comp
where s.is_new
