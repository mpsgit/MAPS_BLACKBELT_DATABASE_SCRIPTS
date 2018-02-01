create or replace type OBJ_PA_TREND_ALLOC_HIST_HD_LN FORCE as object
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
/
show error

