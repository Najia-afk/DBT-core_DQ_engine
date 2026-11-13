<< push('_wr_pair_up', analyze=False) >>
{#- Paires à écrire (nouvelles ou modifiées). Les paires `none` ne sont pas persistées (sauf option
    MDM_ELT_KEEP_NONE_PAIRS) : elles n'influencent aucune décision et sont recalculables. -#}
select
  left_source_id, left_key, right_source_id, right_key, score, decision, contradiction, eligible,
  reason, <<spec.rule_version>> as rule_version, run_id
from {{ ref('r_redo') }}
where sig_changed and (decision <> 'none' or <<'TRUE' if spec.keep_none_pairs else 'FALSE'>>)
