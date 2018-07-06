CREATE OR REPLACE TYPE obj_manl_trend_upl_save_line FORCE AS OBJECT
(
  mrkt_id       NUMBER,
  sls_perd_id   NUMBER,
  sls_typ_id    NUMBER,
  fsc_cd        VARCHAR2(5),
  trnd_unit_qty NUMBER
);
/
