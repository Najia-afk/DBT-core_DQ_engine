<< push('_wr_pair_del', analyze=False) >>
select o.left_source_id, o.left_key, o.right_source_id, o.right_key
from {{ ref('r_old_all') }} o
where not exists (
  select 1 from {{ ref('r_redo') }} n
  where n.left_source_id = o.left_source_id and n.left_key = o.left_key
    and n.right_source_id = o.right_source_id and n.right_key = o.right_key
    and (n.decision <> 'none' or <<'TRUE' if spec.keep_none_pairs else 'FALSE'>>)
)
