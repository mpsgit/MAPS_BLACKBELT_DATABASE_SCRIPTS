create or replace TYPE OBJ_CASH_VAL_MANTNC_LINE as object
( SLS_PERD_ID number(8),
     CASH_VAL number(15,2),
     SCT_R_FACTOR number(19,4),
     UPDT_IND varchar2(35),
     LAST_UPDT_TS date,
     LAST_UPDT_USER_ID varchar2(35)
);

create or replace TYPE OBJ_CASH_VAL_MANTNC_TABLE IS TABLE OF OBJ_CASH_VAL_MANTNC_LINE;

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
     BI24_UNT number(9,0),,
     ADJ_BI24_UNT number(9,0)
     LAST_UPDT_TS date,
     LAST_UPDT_USER_ID varchar2(35)
);

CREATE OR REPLACE TYPE OBJ_DLY_BILNG_ADJSTMNT_TABLE AS TABLE OF OBJ_DLY_BILNG_ADJSTMNT_LINE;

create or replace TYPE OBJ_TREND_TYPE_LINE AS OBJECT
    ( SLS_TYP_ID number,
      SLS_TYP_NM varchar2(100)
    );
 
create or replace TYPE OBJ_TREND_TYPE_TABLE IS TABLE OF OBJ_TREND_TYPE_LINE;

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

CREATE OR REPLACE TYPE OBJ_MANL_TREND_ADJSTMNT_TABLE AS TABLE OF OBJ_MANL_TREND_ADJSTMNT_LINE;

create or replace TYPE OBJ_TREND_TYPE_LINE AS OBJECT
    ( SLS_TYP_ID number,
      SLS_TYP_NM varchar2(100)
    );
	
create or replace TYPE OBJ_TREND_TYPE_TABLE AS TABLE OF OBJ_TREND_TYPE_LINE;

create or replace type OBJ_TREND_ALLOC_VIEW_LINE as object 
	( NOT_PLANND_UNTS NUMBER,
	  has_save VARCHAR2(1),
	  LAST_RUN date,
	  IS_STARTED VARCHAR2(1),
	  IS_COMPLETE varchar2(1)
	);
	
create or replace type obj_trend_alloc_view_table as table of OBJ_TREND_ALLOC_VIEW_LINE;

create or replace TYPE obj_trend_alloc_hist_head_aggr AS OBJECT
(
  sls_perd_id       NUMBER,
  sku_id            NUMBER,
  bi24_unts_on      NUMBER,
  bi24_sls_on       NUMBER,
  bi24_unts_off     NUMBER,
  bi24_sls_off      NUMBER,
  trend_unts_on     NUMBER,
  trend_sls_on      NUMBER,
  trend_unts_off    NUMBER,
  trend_sls_off     NUMBER,
  estimate_unts_on  NUMBER,
  estimate_sls_on   NUMBER,
  estimate_unts_off NUMBER,
  estimate_sls_off  NUMBER,
  actual_unts_on    NUMBER,
  actual_sls_on     NUMBER,
  actual_unts_off   NUMBER,
  actual_sls_off    NUMBER
)
;

create or replace TYPE OBJ_TREND_ALLOC_HIST_HEAD_LINE AS OBJECT
(
  sls_perd_id number,
  trg_perd_id number,
  bilng_day   date,
  bi24_unts_on   number,
  bi24_sls_on    number,
  bi24_unts_off   number,
  bi24_sls_off    number,
  cash_vlu    number,
  r_factor    number,
  estmt_units_on number,
  estmt_sls_on   number,
  estmt_units_off number,
  estmt_sls_off   number,
  trend_units_on number,
  trend_sls_on   number,
  trend_units_off number,
  trend_sls_off   number,
  actual_units_on number,
  actual_sls_on   number,
  actual_units_off number,
  actual_sls_off   number
)
;

create or replace type obj_trend_alloc_hist_hd_table as table of OBJ_TREND_ALLOC_HIST_HEAD_LINE;

------------------------------------------------

create or replace TYPE T_FS_BNCHMRK AS OBJECT
    ( BNCHMRK_PRFL_CD number(7),
      DFALT_IND char(1)
    );
 
create or replace TYPE T_TBL_FS_BNCHMRK IS TABLE OF T_FS_BNCHMRK;
