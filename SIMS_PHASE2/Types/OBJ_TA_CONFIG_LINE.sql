create or replace type OBJ_TA_CONFIG_LINE FORCE as object
(
  SRC_SLS_TYP_ID    number,
  X_SRC_SLS_TYP_ID  number,
  OFFST_LBL_ID      number,
  SLS_TYP_LBL_ID    number,
  X_SLS_TYP_LBL_ID  number,
  SRC_SLS_PERD_ID   number,
  TRGT_SLS_PERD_ID  number,
  SRC_OFFR_PERD_ID  number,
  TRGT_OFFR_PERD_ID number,
  R_FACTOR          number
);
/
show error
