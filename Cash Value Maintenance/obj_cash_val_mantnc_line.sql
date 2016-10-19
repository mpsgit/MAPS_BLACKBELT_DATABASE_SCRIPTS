create or replace 
TYPE OBJ_CASH_VAL_MANTNC_LINE as object
( SLS_PERD_ID number(8),
     CASH_VAL number(15,2),
     SCT_R_FACTOR number(6,4),
     UPDT_IND CHAR(1),
     LAST_UPDT_TS date,
     LAST_UPDT_USER_ID varchar2(35)
);
