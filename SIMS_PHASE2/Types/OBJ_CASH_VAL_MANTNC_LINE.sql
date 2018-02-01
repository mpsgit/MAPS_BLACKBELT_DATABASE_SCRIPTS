create or replace type OBJ_CASH_VAL_MANTNC_LINE FORCE as object
  (SLS_PERD_ID number(8),
   CASH_VAL number(15,2),
   SCT_R_FACTOR number(6,4),
   UPDT_IND varchar2(35),
   LAST_UPDT_TS date,
   LAST_UPDT_USER_ID varchar2(35)
);
/
show error
