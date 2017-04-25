create or replace TYPE OBJ_CASH_VAL_MANTNC_LINE as object
( SLS_PERD_ID number(8),
  CASH_VAL number(15,2),
  SCT_R_FACTOR number(19,4),
  UPDT_IND varchar2(35),
  LAST_UPDT_TS date,
  LAST_UPDT_USER_ID varchar2(35)
);

create or replace TYPE OBJ_CASH_VAL_MANTNC_TABLE IS TABLE OF OBJ_CASH_VAL_MANTNC_LINE;

create or replace TYPE OBJ_DLY_BILNG_ADJSTMNT_LINE as object
( DLY_BILNG_ID NUMBER,
  FSC_CD number(8),
  FSC_DESC varchar2(100),
  SKU_ID number(8),
  LOCAL_SKU_NM varchar2(150),
  SLS_PRC_AMT number(16,8),
  NR_FOR_QTY number(4,0),
  UNT_PRC_AMT number(16,8),
  ACT_IND char(1),
  PLN_IND char(1),
  BI24_UNT number(9,0),
  ADJ_BI24_UNT number(9,0),
  LAST_UPDT_TS date,
  LAST_UPDT_USER_ID varchar2(35)
);

create or replace TYPE OBJ_DLY_BILNG_ADJSTMNT_TABLE IS TABLE OF OBJ_DLY_BILNG_ADJSTMNT_LINE;

create or replace TYPE OBJ_MANL_TREND_ADJSTMNT_LINE as object
( FSC_CD number(8),
  FSC_DESC varchar2(100),
  SKU_ID number(8),
  LCL_SKU_NM varchar2(150),
  BI24_UNIT_QTY number(10,0),
  SCT_UNIT_QTY number(9,0),
  LAST_UPDT_TS date,
  LAST_UPDT_USER_ID varchar2(35)
);

create or replace TYPE OBJ_MANL_TREND_ADJSTMNT_TABLE IS TABLE OF OBJ_MANL_TREND_ADJSTMNT_LINE;

create or replace TYPE OBJ_SKU_BIAS_MANTNC_LINE as object
( FSC_CD number(8),
  PROD_DESC_TXT varchar2(100),
  SKU_ID number(8),
  LCL_SKU_NM varchar2(150),
  ACT_IND char(1),
  PLN_IND char(1),
  BIAS_PCT number(6,2),
  LAST_UPDT_TS date,
  LAST_UPDT_USER_ID varchar2(35)
);

create or replace TYPE OBJ_SKU_BIAS_MANTNC_TABLE IS TABLE OF OBJ_SKU_BIAS_MANTNC_LINE;

create or replace TYPE OBJ_TA_CONFIG_LINE AS OBJECT
(
  src_sls_typ_id    NUMBER,
  x_src_sls_typ_id  NUMBER,
  offst_lbl_id      NUMBER,
  sls_typ_lbl_id    NUMBER,
  x_sls_typ_lbl_id  NUMBER,
  src_sls_perd_id   NUMBER,
  trgt_sls_perd_id  NUMBER,
  src_offr_perd_id  NUMBER,
  trgt_offr_perd_id NUMBER,
  r_factor          NUMBER
);

create or replace TYPE OBJ_TA_CONFIG_TABLE is table of OBJ_TA_CONFIG_LINE;

create or replace type OBJ_TREND_ALLOC_VIEW_LINE as object 
( NOT_PLANND_UNTS NUMBER,
  has_save VARCHAR2(1),
  LAST_RUN date,
  IS_STARTED VARCHAR2(1),
  IS_COMPLETE varchar2(1)
);
	
create or replace type obj_trend_alloc_view_table is table of OBJ_TREND_ALLOC_VIEW_LINE;

create or replace TYPE OBJ_TREND_TYPE_LINE AS OBJECT
( SLS_TYP_ID number,
  SLS_TYP_NM varchar2(100)
);
 
create or replace TYPE OBJ_TREND_TYPE_TABLE IS TABLE OF OBJ_TREND_TYPE_LINE;

create or replace TYPE PA_MANL_TREND_ADJSTMNT_LINE as object
( FSC_CD number(8),
  FSC_DESC varchar2(100),
  SKU_ID number(8),
  LCL_SKU_NM varchar2(150),
  OFFST_LBL_ID number,
  BI24_UNIT_QTY number(10,0),
  SCT_UNIT_QTY number(9,0),
  LAST_UPDT_TS date,
  LAST_UPDT_USER_ID varchar2(35)
);

create or replace type PA_MANL_TREND_ADJSTMNT_TABLE is table of PA_MANL_TREND_ADJSTMNT_LINE;

create or replace TYPE PA_SKU_BIAS_MANTNC_LINE as object
( FSC_CD number(8),
  FSC_DESC varchar2(100),
  SKU_ID number(8),
  LOCAL_SKU_NM varchar2(150),
  ACT_IND char(1),
  PLN_IND CHAR(1),
  sls_typ_id number,
  BIAS_PCT number(6,2),
  LAST_UPDT_TS date,
  LAST_UPDT_USER_ID VARCHAR2(35)
);

create or replace TYPE PA_SKU_BIAS_MANTNC_TABLE IS TABLE OF PA_SKU_BIAS_MANTNC_LINE;

create or replace type OBJ_PA_TREND_ALLOC_HIST_DT_LN as object
( sku_id NUMBER,
  catgry_id NUMBER,
  brnd_id NUMBER,
  sgmt_id NUMBER,
  form_id NUMBER,
  prfl_cd NUMBER(7,0),
  promtn_id NUMBER,
  promtn_clm_id NUMBER,
  sls_cls_cd VARCHAR2(5),
  veh_id NUMBER,
  fsc_cd VARCHAR2(8),
  offr_id NUMBER,
  offst_lbl_id NUMBER,
  cash_value NUMBER,
  r_factor NUMBER,
  sls_typ_lbl_id NUMBER,
  units NUMBER,
  sales NUMBER
);

create or replace type OBJ_PA_TREND_ALLOC_HIST_DT_TBL is table of OBJ_PA_TREND_ALLOC_HIST_DT_LN;

create or replace type OBJ_PA_TREND_ALLOC_HIST_HD_LN as object
(
  SLS_PERD_ID      NUMBER,
  TRG_PERD_ID      NUMBER,
  BILNG_DAY        DATE,
  OFFST_LBL_ID     NUMBER,
  CASH_VALUE       NUMBER,
  R_FACTOR         NUMBER,
  SLS_TYP_LBL_ID   NUMBER,
  UNITS            NUMBER,
  SALES            NUMBER
);

create or replace type OBJ_PA_TREND_ALLOC_HIST_HD_TBL is table of OBJ_PA_TREND_ALLOC_HIST_HD_LN;

create or replace type sct_dly_updt_rpt_line as object 
(
  rpt_date DATE,
  trgt_cmpgn_sls_perd_id NUMBER,
  sls_typ_id NUMBER,
  cash_value NUMBER,
  sls_to_date NUMBER,
  r_factor NUMBER
);

create or replace type sct_dly_updt_rpt_table is table of sct_dly_updt_rpt_line;

create or replace type sct_trend_check_rpt_line as object 
( 
  rpt_date DATE,
  fsc_code VARCHAR2(8),
  FSC_NM varchar2(100),
  bst_sc_trgt_perd_id number,
  bst_sc_sls_perd_id number,
  BST_SC_UNITS number,
  EST_SC_SLS_PERD_ID number,
  est_sc_trgt_perd_id NUMBER,
  EST_SC_UNITS number,
  NXT_EST_SC_SLS_PERD_ID number,
  nxt_est_sc_trgt_perd_id NUMBER,
  nxt_esc_sc_units NUMBER
);

create or replace type sct_trend_check_rpt_table is table of sct_trend_check_rpt_line;

CREATE OR REPLACE TYPE P94_RPT_HEAD_LINE AS OBJECT
(
  mrkt_id         NUMBER,
  sls_perd_id     NUMBER,
  trgt_perd_id    NUMBER,
  sls_typ_nm      VARCHAR2(100),
  totl_cash_frcst NUMBER,
  sales           NUMBER,
  r_factor        NUMBER,
  prcsng_dt       DATE,
  offr_perd_id_on NUMBER,
  off_perd_id_off NUMBER
);

CREATE OR REPLACE TYPE P94_RPT_HEAD_TABLE AS TABLE OF P94_RPT_HEAD_LINE;

CREATE OR REPLACE TYPE P94_RPT_DTLS_LINE AS OBJECT
(
  mrkt_id           NUMBER,
  sls_perd_id       NUMBER,
  trgt_perd_id      NUMBER,
  sls_typ_id        NUMBER,
  fsc_cd            VARCHAR2(4000),
  fsc_desc          VARCHAR2(4000),
  veh_id            NUMBER,
  pg_no             NUMBER,
  totl_estimt       NUMBER,
  totl_op_est       NUMBER,
  totl_op_est_on    NUMBER,
  totl_op_est_off   NUMBER,
  totl_units        NUMBER,
  totl_bst          NUMBER,
  totl_bst_on       NUMBER,
  totl_bst_off      NUMBER,
  totl_bst_pct      NUMBER,
  bias              NUMBER,
  variation         NUMBER,
  variation_pct     NUMBER,
  intrdctn_perd_id  NUMBER,
  stat_dt           NUMBER,
  catgry_nm         VARCHAR2(100),
  sgmt_nm           VARCHAR2(100),
  form_desc_txt     VARCHAR2(100),
  form_grp_desc_txt VARCHAR2(100),
  brnd_nm           VARCHAR2(100),
  itemid            NUMBER,
  conceptid         NUMBER,
  sls_cls_desc_txt  VARCHAR2(100)
);

CREATE OR REPLACE TYPE P94_RPT_DTLS_TABLE AS TABLE OF P94_RPT_DTLS_LINE;

CREATE OR REPLACE TYPE OBJ_CUST_GRP_MANTNC_LINE AS OBJECT
(
  rulid    NUMBER,
  rulnm    VARCHAR2(20),
  ruldesc  VARCHAR2(200),
  skuid    NUMBER,
  skunm    VARCHAR2(100),
  catgryid NUMBER,
  brandid  NUMBER,
  fsccd    NUMBER,
  fscdesc  VARCHAR2(100)
);

CREATE OR REPLACE TYPE OBJ_CUST_GRP_MANTNC_TABLE AS TABLE OF OBJ_CUST_GRP_MANTNC_LINE;

CREATE OR REPLACE TYPE OBJ_CUST_GRP_MANTNC_GA_LINE AS OBJECT
(
  rulid      NUMBER,
  trgtperdid NUMBER
);

CREATE OR REPLACE TYPE OBJ_CUST_GRP_MANTNC_GA_TABLE AS TABLE OF OBJ_CUST_GRP_MANTNC_GA_LINE;

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_CRRNT_LINE AS OBJECT
(
  OFFST_LBL_ID NUMBER,
  CATGRY_ID    NUMBER,
  SLS_CLS_CD   VARCHAR2(5),
  VEH_ID       NUMBER,
  PERD_PART    NUMBER,
  SKU_ID       NUMBER,
  UNITS_BI24   NUMBER,
  SALES_BI24   NUMBER
);

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_CRRNT_TABLE
AS TABLE OF OBJ_PA_TREND_ALLOC_CRRNT_LINE;

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_RULES_LINE AS OBJECT
(
  RUL_ID          NUMBER,
  RUL_NM          VARCHAR2(50),
  OFFST_LBL_ID    NUMBER,
  CATGRY_ID       NUMBER,
  SLS_CLS_CD      VARCHAR2(5),
  VEH_ID          NUMBER,
  PERD_PART       NUMBER,
  SKU_LIST        VARCHAR2(2048),
  PRIRTY          NUMBER,
  PERIOD_LIST     VARCHAR2(2048),
  R_FACTOR        NUMBER,
  R_FACTOR_MANUAL NUMBER
);

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_RULES_TABLE
AS TABLE OF OBJ_PA_TREND_ALLOC_RULES_LINE;

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_SGMNT_LINE AS OBJECT
(
  RUL_NM       VARCHAR2(50),
  SLS_PERD_ID  NUMBER,
  BILNG_DAY    DATE,
  ISSELECTED   CHAR(1),
  UNITS_BI24   NUMBER,
  SALES_BI24   NUMBER,
  UNITS_ACTUAL NUMBER,
  SALES_ACTUAL NUMBER
);

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_SGMNT_TABLE
AS TABLE OF OBJ_PA_TREND_ALLOC_SGMNT_LINE;

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_VIEW_LINE AS OBJECT
( 
  NOT_PLANND_UNTS      NUMBER,
  HAS_SAVE             CHAR(1),
  LAST_RUN             DATE,
  IS_STARTED           CHAR(1),
  IS_COMPLETE          CHAR(1),
  IS_SAVED             CHAR(1),
  USE_OFFERS_ON_SCHED  CHAR(1),
  USE_OFFERS_OFF_SCHED CHAR(1)
);

CREATE OR REPLACE TYPE OBJ_PA_TREND_ALLOC_VIEW_TABLE
AS TABLE OF OBJ_PA_TREND_ALLOC_VIEW_LINE;
