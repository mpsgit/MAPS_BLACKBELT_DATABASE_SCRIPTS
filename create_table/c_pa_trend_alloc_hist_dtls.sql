-- Create table
create table PA_TREND_ALLOC_HIST_DTLS
(
  mrkt_id                  NUMBER not null,
  sls_typ_id               NUMBER not null,
  dbt_bilng_day            DATE not null,
  sls_typ_id_from_config   VARCHAR2(50) not null,
  dbt_on_sls_perd_id       NUMBER not null,
  sku_id                   NUMBER not null,
  veh_id                   NUMBER  not null,
  offr_desc_txt            VARCHAR2(254) not null,
  promtn_id                NUMBER not null,
  promtn_clm_id            NUMBER not null,
  sls_cls_cd               VARCHAR2(5) not null,
  bi24_unts_on             NUMBER,
  bi24_sls_on              NUMBER,
  bi24_unts_off            NUMBER,
  bi24_sls_off             NUMBER,
  estimate_unts_on         NUMBER,
  estimate_sls_on          NUMBER,
  estimate_unts_off        NUMBER,
  estimate_sls_off         NUMBER,
  trend_unts_on            NUMBER,
  trend_sls_on             NUMBER,
  trend_unts_off           NUMBER,
  trend_sls_off            NUMBER,
  actual_unts_on           NUMBER,
  actual_sls_on            NUMBER,
  actual_unts_off          NUMBER,
  actual_sls_off           NUMBER
)
tablespace &data_tablespace_name
  pctfree 0
  pctused 40
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table PA_TREND_ALLOC_HIST_DTLS
  add constraint PK_PA_TREND_ALLOC_HIST_DTLS primary key (mrkt_id, dbt_on_sls_perd_id, sls_typ_id, dbt_bilng_day, sls_typ_id_from_config, sku_id, veh_id, offr_desc_txt, promtn_id, promtn_clm_id, sls_cls_cd)
  using index 
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
    pctincrease 0
  );

