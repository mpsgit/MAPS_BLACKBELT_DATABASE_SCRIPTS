create or replace type OBJ_DLY_BILNG_ADJSTMNT_LINE FORCE as object
  (DLY_BILNG_ID NUMBER,
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
/
show error
