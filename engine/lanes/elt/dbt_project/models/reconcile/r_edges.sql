{{ config(materialized='table') }}
-- depends_on: {{ ref('r_check') }}
select a.nid as a, b.nid as b
from {{ ref('r_edges_pairs') }} e
join {{ ref('r_nodes') }} a on a.source_id = e.left_source_id and a.source_key = e.left_key
join {{ ref('r_nodes') }} b on b.source_id = e.right_source_id and b.source_key = e.right_key
union all
select b.nid as a, a.nid as b
from {{ ref('r_edges_pairs') }} e
join {{ ref('r_nodes') }} a on a.source_id = e.left_source_id and a.source_key = e.left_key
join {{ ref('r_nodes') }} b on b.source_id = e.right_source_id and b.source_key = e.right_key
