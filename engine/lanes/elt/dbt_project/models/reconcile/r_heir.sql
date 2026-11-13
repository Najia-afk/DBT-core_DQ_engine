{#- Héritier de chaque ancien staging : la composante qui contient le plus de ses membres
    (égalité : la composante au plus petit nœud). -#}
select old_sid, comp from (
  select old_sid, comp, row_number() over (partition by old_sid order by n desc, comp asc) as rn
  from {{ ref('r_overlap') }}
) where rn = 1
