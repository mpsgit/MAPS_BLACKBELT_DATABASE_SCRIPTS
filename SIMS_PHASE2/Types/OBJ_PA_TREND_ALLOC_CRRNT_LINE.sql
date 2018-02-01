create or replace TYPE OBJ_PA_TREND_ALLOC_CRRNT_LINE FORCE as object
(
  offst_lbl_id NUMBER,
  catgry_id    NUMBER,
  sls_cls_cd   VARCHAR2(5),
  veh_id       NUMBER,
  perd_part    NUMBER,
  sku_id       NUMBER,
  units_bi24   NUMBER,
  sales_bi24   NUMBER
);
/
show error
