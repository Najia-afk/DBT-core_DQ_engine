<# Compteurs du run : ReconcileResult de la référence + preuves « delta seulement » (lignes lues
   dans / écrites vers l'état PostgreSQL). #>
select
  (select count(*) from {{ ref('r_d') }}) as delta_nodes,
  (select count(*) from {{ ref('r_r') }}) as rescored_nodes,
  (select count(*) from {{ ref('r_pairs_new') }}) as pairs,
  (select count(*) from {{ ref('r_a') }}) as affected_nodes,
  (select count(*) from {{ ref('r_comp_sid') }}) as stagings,
  (select count(*) from {{ ref('r_stg_event') }} where kind = 'merge') as merges,
  (select count(*) from {{ ref('r_stg_event') }} where kind = 'split') as splits,
  (select count(*) from {{ ref('r_stg_event') }} where kind = 'create') as creates,
  (select count(*) from {{ ref('r_gold_delta') }}) as golden_changes,
  (select count(*) from {{ ref('r_d_rec') }}) + (select count(*) from {{ ref('r_pvals') }})
    + (select count(*) from {{ ref('r_a_rec') }}) as read_source_record,
  (select count(*) from {{ ref('r_old_pairs_r') }}) + (select count(*) from {{ ref('r_pairs_kc') }})
    + (select count(*) from {{ ref('r_elig_y') }}) + (select count(*) from {{ ref('r_apairs') }})
    as read_match_pair,
  (select count(*) from {{ ref('r_old_blocks') }}) + (select count(*) from {{ ref('r_flip_members') }})
    + (select count(*) from {{ ref('r_extra_blocks') }}) + (select count(*) from {{ ref('r_old_members') }})
    as read_block_index,
  (select count(*) from {{ ref('r_seed_stg') }}) + (select count(*) from {{ ref('r_stg_members') }})
    as read_staging_member,
  (select count(*) from {{ ref('r_gold_old') }}) as read_golden,
  (select count(*) from {{ ref('r_pair_up') }}) + (select count(*) from {{ ref('r_pair_del') }})
    as written_match_pair,
  (select count(*) from {{ ref('r_sm_up') }}) + (select count(*) from {{ ref('r_sm_del') }})
    as written_staging_member,
  (select count(*) from {{ ref('r_gold_up') }}) + (select count(*) from {{ ref('r_gold_ret') }})
    as written_golden,
  (select count(*) from {{ ref('r_stg_new') }}) as written_staging,
  (select count(*) from {{ ref('r_anom') }}) as anomalies,
  (select count(*) from {{ ref('r_pair_up') }}) as pairs_written,
  (select count(*) from {{ ref('r_blk_nodes') }}) + (select count(*) from {{ ref('r_blk_new') }})
    + (select count(*) from {{ ref('r_done') }}) + (select count(*) from {{ ref('r_meta_out') }})
    as written_other
