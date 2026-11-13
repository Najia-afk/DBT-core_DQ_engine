<< push('_wr_extra', index='source_id, source_key') >>
{#- Membres de blocs basculés hors du delta : rescorés eux aussi. -#}
select distinct m.source_id, m.source_key
from {{ ref('r_flip_members') }} m
where not exists (
  select 1 from {{ ref('r_d') }} d where d.source_id = m.source_id and d.source_key = m.source_key
)
