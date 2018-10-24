CREATE OR REPLACE TYPE obj_edit_offr_line FORCE AS OBJECT
(
  status                 NUMBER, --0 no lock 1 unit lock 2 full lock
  mrkt_id                NUMBER,
  offr_perd_id           NUMBER,
  offr_lock              NUMBER,
  offr_lock_user         VARCHAR2(35),
  offr_sku_line_id       NUMBER,
  veh_id                 NUMBER,
  brchr_plcmnt_id        NUMBER,
  brchr_sctn_nm          VARCHAR2(100),
  enrgy_chrt_postn_id    NUMBER,
  web_postn_id           NUMBER,
  pg_nr                  NUMBER,
  ctgry_id               NUMBER,
  brnd_id                NUMBER, --BRND_FMLY_ID
  sgmt_id                NUMBER,
  form_id                NUMBER,
  form_grp_id            NUMBER,
  prfl_cd                NUMBER,
  sku_id                 NUMBER,
  fsc_cd                 VARCHAR2(8),
  prod_typ_id            NUMBER,
  gender_id              NUMBER,
  sls_cls_cd             VARCHAR2(5),
  pp_sls_cls_cd          VARCHAR2(5),
  item_sls_cls_cd        VARCHAR2(5),
  offr_desc_txt          VARCHAR2(254),
  offr_notes_txt         VARCHAR2(1000),
  offr_lyot_cmnts_txt    VARCHAR2(3000),
  featrd_side_cd         VARCHAR2(5),
  concept_featrd_side_cd VARCHAR2(5),
  micr_ncpsltn_ind       CHAR(1),
  scntd_pg_typ_id        NUMBER,
  cnsmr_invstmt_bdgt_id  NUMBER,
  pymt_typ               VARCHAR2(5),
  promtn_id              NUMBER,
  promtn_clm_id          NUMBER,
--  cmbntn_offr_typ        NUMBER,
  spndng_lvl             VARCHAR2(20),
  comsn_typ              VARCHAR2(5),
  tax_type_id            NUMBER,
  wsl_ind                CHAR(1),
  offr_sku_set_id        NUMBER,
  cmpnt_qty              NUMBER,
  nr_for_qty             NUMBER,
  nta_factor             NUMBER,
  sku_cost               NUMBER,
  lv_nta                 NUMBER,
  lv_sp                  NUMBER,
  lv_rp                  NUMBER,
  lv_discount            NUMBER,
  lv_units               NUMBER,
  lv_total_cost          NUMBER,
  lv_gross_sales         NUMBER,
  lv_dp_cash             NUMBER,
  lv_dp_percent          NUMBER,
  ver_id                 NUMBER,
  sls_prc_amt            NUMBER,
  reg_prc_amt            NUMBER,
  line_nr                VARCHAR2(8),
  unit_qty               NUMBER,
  dltd_ind               CHAR(1),
  created_ts             DATE,
  created_user_id        VARCHAR2(35),
  last_updt_ts           DATE,
  last_updt_user_id      VARCHAR2(35),
  intrnl_offr_id         NUMBER,
  mrkt_veh_perd_sctn_id  NUMBER,
  prfl_nm                VARCHAR2(100),
  sku_nm                 VARCHAR2(100),
  comsn_typ_desc_txt     VARCHAR2(100),
  tax_typ_desc_txt       VARCHAR2(100),
  offr_sku_set_nm        VARCHAR2(100),
  sls_typ                NUMBER,
  pc_sp_py               NUMBER,
  pc_rp                  NUMBER,
  pc_sp                  NUMBER,
  pc_vsp                 NUMBER,
  pc_hit                 NUMBER,
  pg_wght                NUMBER(8,3),
  pp_pg_wght             NUMBER(8,3),
  sprd_nr                NUMBER,
  offr_prfl_prcpt_id     NUMBER,
  has_unit_qty           NUMBER,
  offr_typ               VARCHAR2(5),
  forcasted_units        NUMBER,
  forcasted_date         DATE,
  offr_cls_id            NUMBER,
  spcl_ordr_ind          CHAR(1),
  offr_ofs_nr            NUMBER,
  pp_ofs_nr              NUMBER,
  impct_catgry_id        NUMBER,
  hero_ind               CHAR(1),
  smplg_ind              CHAR(1),
  mltpl_ind              CHAR(1),
  cmltv_ind              CHAR(1),
  use_instrctns_ind      CHAR(1),
  pg_typ_id              NUMBER,
  featrd_prfl_ind        CHAR(1),
  fxd_pg_wght_ind        CHAR(1),
  prod_endrsmt_id        NUMBER,
  frc_mtch_mthd_id       NUMBER,
  wghtd_avg_cost_amt     NUMBER,
  incntv_id              NUMBER,
  intrdctn_perd_id       NUMBER,
  on_stus_perd_id        NUMBER,
  dspostn_perd_id        NUMBER,
  scnrio_id              NUMBER
);
/
