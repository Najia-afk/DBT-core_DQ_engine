{#- Anciennes versions stockées des paires recalculées (touchant R ou K\R). -#}
select * from {{ ref('r_old_pairs_r') }}
union all
select * from {{ ref('r_pairs_kc') }}
