create or replace type OBJ_PA_TREND_ALLOC_HIST_DT_LN FORCE as object
( SKU_ID NUMBER,
  CATGRY_ID NUMBER,
  BRND_ID NUMBER,
  SGMT_ID NUMBER,
  FORM_ID NUMBER,
  PRFL_CD NUMBER(7,0),
  PROMTN_ID NUMBER,
  PROMTN_CLM_ID NUMBER,
  SLS_CLS_CD VARCHAR2(5),
  VEH_ID NUMBER,
  FSC_CD VARCHAR2(8),
  OFFR_ID NUMBER,
  OFFST_LBL_ID NUMBER,
  CASH_VALUE NUMBER,
  R_FACTOR NUMBER,
  SLS_TYP_LBL_ID NUMBER,
  UNITS NUMBER,
  SALES NUMBER
);
/
show error
