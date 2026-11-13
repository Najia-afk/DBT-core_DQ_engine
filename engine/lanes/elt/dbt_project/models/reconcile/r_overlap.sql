select old_sid, comp, count(*) as n from {{ ref('r_node_comp') }}
where old_sid is not null
group by all
