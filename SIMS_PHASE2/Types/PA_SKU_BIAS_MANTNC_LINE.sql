create or replace type PA_SKU_BIAS_MANTNC_LINE FORCE as object
  (FSC_CD number(8),
   FSC_DESC varchar2(100),
   SKU_ID number(8),
   LOCAL_SKU_NM varchar2(150),
   ACT_IND char(1),
   PLN_IND CHAR(1),
   SLS_TYP_ID number,
   BIAS_PCT number(6,2),
   LAST_UPDT_TS date,
   LAST_UPDT_USER_ID VARCHAR2(35)
);
/
show error


