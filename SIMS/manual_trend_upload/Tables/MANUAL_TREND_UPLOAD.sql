DROP TABLE manual_trend_upload;

CREATE TABLE manual_trend_upload
(
  mrkt_id             NUMBER       NOT NULL,
  sls_perd_id         NUMBER       NOT NULL,
  sls_typ_id          NUMBER       NOT NULL,
  fsc_cd              VARCHAR2(5)  NOT NULL,
  unit_qty            NUMBER       NOT NULL,
  created_user_id     VARCHAR2(35) NOT NULL,
  created_ts          DATE         NOT NULL,
  last_updt_user_id   VARCHAR2(35) NOT NULL,
  last_updt_ts        DATE         NOT NULL,
  CONSTRAINT pk_mnl_trnd_upl PRIMARY KEY (mrkt_id, sls_perd_id, sls_typ_id, fsc_cd)
);
