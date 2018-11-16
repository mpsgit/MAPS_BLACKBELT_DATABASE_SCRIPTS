CREATE OR REPLACE TYPE obj_pivot_line force AS OBJECT
(
  mrkt_id                NUMBER,
  mrkt_nm                VARCHAR2(100),
  offr_perd_id           NUMBER,
  sls_perd_id            NUMBER,
  offr_link_id           NUMBER,
  offr_sku_line_link_id  NUMBER,
  offr_sku_line_id       NUMBER,
  veh_id                 NUMBER,
  veh_desc_txt           VARCHAR2(100), --new offer api select * from veh;
  brchr_plcmnt_id        NUMBER,
  brchr_plcmnt_nm        VARCHAR2(100), --new offer api select * from brchr_plcmt;
  brchr_sctn_nm          VARCHAR2(100),
  sctn_seq_nr            NUMBER,        --new offer api select * from mrkt_veh_perd_sctn;
  enrgy_chrt_postn_id    NUMBER,
  enrgy_chrt_postn_nm    VARCHAR2(100), --new offer api select * from enrgy_chrt_postn;
  pg_nr                  NUMBER,
  pp_pg_nr               NUMBER,
  ctgry_id               NUMBER,
  ctgry_nm               VARCHAR2(100), --new offer api select * from catgry;
  brnd_id                NUMBER,        --BRND_FMLY_ID
  brnd_nm                VARCHAR2(100), --new offer api select * from brnd;
  sgmt_id                NUMBER,
  sgmt_nm                VARCHAR2(100), --new offer api select * from sgmt;
  form_id                NUMBER,
  form_desc_txt          VARCHAR2(100), --new offer api select * from form;
  form_grp_id            NUMBER,
  form_grp_desc_txt      VARCHAR2(100), --new offer api select * from form_grp;
  prfl_cd                NUMBER,
  sku_id                 NUMBER,
  fsc_cd                 VARCHAR2(8),
  prod_typ_id            NUMBER,
  prod_typ_desc_txt      VARCHAR2(100), --new offer api select * from prod_typ
  gender_id              NUMBER,
  gender_desc            VARCHAR2(10),  --new offer api 1 male 2 female 3 unisex
  sls_cls_cd             VARCHAR2(5),
  sls_cls_desc_txt       VARCHAR2(100), --new offer api select * from sls_cls
  offr_desc_txt          VARCHAR2(254),
  offr_notes_txt         VARCHAR2(1000),
  offr_lyot_cmnts_txt    VARCHAR2(3000),
  featrd_side_cd         VARCHAR2(5),
  featrd_side_desc       VARCHAR2(10),  --new offer api 0 left 1 right 2 both
  concept_featrd_side_cd VARCHAR2(5),
  concept_featrd_side_desc VARCHAR2(10), --new offer api select * from concept; 0left 1 right 2both
  micr_ncpsltn_ind       CHAR(1),
  cnsmr_invstmt_bdgt_id  NUMBER,
  cnsmr_invstmt_bdgt_desc_Txt  VARCHAR2(100), --new offer api select * from cnsmr_invstmt_bdgt;
  pymt_typ               VARCHAR2(5),
  pymt_typ_desc_txt      VARCHAR2(100),       --new offer api select * from pymt_typ;
  promtn_id              NUMBER,
  promtn_desc_txt        VARCHAR2(100),       --new offer api select * from promtn;
  promtn_clm_id          NUMBER,
  promtn_clm_desc_txt    VARCHAR2(254),       --new offer api select * from promtn_clm;
  comsn_typ              VARCHAR2(5),
  tax_type_id            NUMBER,
  wsl_ind                CHAR(1),
  offr_sku_set_id        NUMBER,
  cmpnt_qty              NUMBER,
  nr_for_qty             NUMBER,
  nta_factor             NUMBER,
  actual_nta             NUMBER,
  sku_cost               NUMBER,
  ver_id                 NUMBER,
  ver_desc_txt           VARCHAR2(35),       --new offer api select * from ver;
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
  sls_typ_nm             VARCHAR2(100),      --new offer api select * from sls_typ
  pg_wght                NUMBER(8,3),
  sprd_nr                NUMBER,
  offr_prfl_prcpt_id     NUMBER,
  trgt_cnsmr_grp_id      NUMBER,
  trgt_cnsmr_grp_desc_txt VARCHAR2(100),
  MICR_NCPSLTN_DESC_TXT  VARCHAR2(150),
  SSNL_EVNT_ID           NUMBER,
  SSNL_EVNT_DESC_TXT     VARCHAR2(100),
  pp_pg_wght_pct         NUMBER(8,3),
  brchr_sub_sctn_strtg_pg NUMBER,
  brchr_sub_Sctn_end_pg   NUMBER,
  bus_id                  NUMBER,
  bus_nm                  VARCHAR2(100),
  scnrio_id               NUMBER,
  scnrio_desc_txt         VARCHAR2(100),
  offr_typ                VARCHAR2(5)
);
/
