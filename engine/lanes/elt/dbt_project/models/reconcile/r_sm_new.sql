select s.staging_id, n.source_id, n.source_key
from {{ ref('r_node_comp') }} n
join {{ ref('r_comp_sid') }} s on s.comp = n.comp
