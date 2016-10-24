create or replace TYPE OBJ_CASH_VAL_MANTNC_LINE as object
( SLS_PERD_ID number(8),
     CASH_VAL number(15,2),
     SCT_R_FACTOR number(6,4),
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
     CREAT_TS date,
     CREAT_USER_ID varchar2(35),
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
     BI24_UNT number(9,0)
);

CREATE OR REPLACE TYPE OBJ_DLY_BILNG_ADJSTMNT_TABLE AS TABLE OF OBJ_DLY_BILNG_ADJSTMNT_LINE

create or replace TYPE T_FS_BNCHMRK AS OBJECT
    ( BNCHMRK_PRFL_CD number(7),
      DFALT_IND char(1)
    );
 
create or replace TYPE T_TBL_FS_BNCHMRK IS TABLE OF T_FS_BNCHMRK;
