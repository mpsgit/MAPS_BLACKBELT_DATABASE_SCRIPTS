create or replace type PA_MANL_TREND_ADJSTMNT_LINE FORCE as object
  (FSC_CD number(8),
   FSC_DESC varchar2(100),
   SKU_ID number(8),
   LCL_SKU_NM varchar2(150),
   OFFST_LBL_ID number,
   BI24_UNIT_QTY number,
   SCT_UNIT_QTY number,
   LAST_UPDT_TS date,
   LAST_UPDT_USER_ID varchar2(35)
);
/
show error
