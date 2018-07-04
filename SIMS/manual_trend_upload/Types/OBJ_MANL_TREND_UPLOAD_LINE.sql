CREATE OR REPLACE TYPE obj_manl_trend_upload_line FORCE AS OBJECT
(
  status                       NUMBER,
  mrkt_id                      NUMBER,
  sls_perd_id                  NUMBER,
  last_updt_ts                 DATE,
  last_updt_user_id            VARCHAR2(35),
  fsc_cd                       VARCHAR2(5),
  desc_txt                     VARCHAR2(100),
  day                          NUMBER,
  actual_day                   DATE,
  dly_unit_qty                 NUMBER,
  total_dly_bilng_unit_qty     NUMBER,
  trnd_unit_qty                NUMBER,
  r_factor                     NUMBER,
  sls_typ_id                   NUMBER
);
/
