-- Create table
create table TREND_ALLOC_HIST_DTLS
(
  mrkt_id        NUMBER not null,
  sls_perd_id    NUMBER not null,
  sls_typ_id     NUMBER not null,
  sls_typ_grp_nm VARCHAR2(32) not null,
  bilng_day      DATE not null,
  sku_id         NUMBER,
  veh_id         NUMBER,
  offr_id        NUMBER,
  promtn_id      NUMBER,
  promtn_clm_id  NUMBER,
  sls_cls_cd     VARCHAR2(5),
  offst_lbl_id   NUMBER,
  cash_value     NUMBER,
  r_factor       NUMBER,
  sls_typ_lbl_id NUMBER,
  units          NUMBER,
  sales          NUMBER,
  perd_part      NUMBER
)
tablespace &data_tablespace_name
  pctfree 0
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate indexes 
create index IX_TREND_ALLOC_HIST_DTLS on TREND_ALLOC_HIST_DTLS (MRKT_ID, SLS_PERD_ID, SLS_TYP_ID, BILNG_DAY)
  tablespace &index_tablespace_name
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
