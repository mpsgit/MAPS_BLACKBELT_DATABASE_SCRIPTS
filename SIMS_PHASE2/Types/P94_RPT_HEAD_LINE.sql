create or replace type P94_RPT_HEAD_LINE FORCE as object 
  (MRKT_ID NUMBER 
  , SLS_PERD_ID NUMBER
  , TRGT_PERD_ID number 
  , SLS_TYP_NM VARCHAR2(100) 
  , TOTL_CASH_FRCST NUMBER 
  , SALES NUMBER 
  , R_FACTOR NUMBER 
  , PRCSNG_DT DATE 
  , OFFR_PERD_ID_ON NUMBER 
  , OFF_PERD_ID_OFF NUMBER 
);
/
show error