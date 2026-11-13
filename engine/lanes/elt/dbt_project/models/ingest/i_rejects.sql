<< push('_wi_rej_' ~ src.id, analyze=False) >>
{#- Rejets : clé nulle, revision invalide, doublons, échecs de conversion et de contrôles. -#}
with r as (
  select '' as source_key, '' as attribute, 'key_null' as rule, 'reject_record' as action
  from {{ ref('i_keys') }} where source_key is null
  union all
  select source_key, '', 'revision', 'reject_record'
  from {{ ref('i_keys') }} where source_key is not null and rev_bad
  union all
  select source_key, '', 'duplicate_key', 'reject_record'
  from {{ ref('i_ranked') }} where rn > 1<% for c in cols %><% if c.can_fail %>
  union all
  select source_key, <<lit(c.a.name)>>, 'cast', 'reject_record'
  from {{ ref('i_norm') }} where <<c.cfq>><% endif %><% for chk in c.checks %>
  union all
  select source_key, <<lit(c.a.name)>>, <<lit(chk.rule)>>, <<lit(chk.action)>>
  from {{ ref('i_norm') }} where <<chk.fq>><% endfor %><% endfor %>

)
select '{{ var("run_id") }}' as run_id, '<<src.id>>' as source_id,
  source_key, attribute, rule, action
from r where not (select blocked from {{ ref('i_status') }})