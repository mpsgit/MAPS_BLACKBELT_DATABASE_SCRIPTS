create or replace TYPE OBJ_PA_TREND_ALLOC_PRD_DTL_LN FORCE AS OBJECT
(
  TRGT_PERD_ID   NUMBER,
  INTR_PERD_ID   NUMBER,
  DISC_PERD_ID   NUMBER,
  SLS_CLS_CD     VARCHAR2(5),
  FSC_CD         VARCHAR2(8),
  BIAS           NUMBER,
  BI24_ADJ       NUMBER,
  SLS_PRC_AMT    NUMBER,
  NR_FOR_QTY     NUMBER,
  OFFST_LBL_ID   NUMBER,
  SLS_TYP_LBL_ID NUMBER,
  UNITS          NUMBER,
  SALES          NUMBER
);
/
show error
