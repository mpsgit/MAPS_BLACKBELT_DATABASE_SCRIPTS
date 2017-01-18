-- Create table
create table PA_TREND_ALLOC_HIST_DTLS_LOG
(
  mrkt_id                  NUMBER not null,
  dbt_on_sls_perd_id       NUMBER not null,
  sls_typ_id               NUMBER not null,
  dbt_bilng_day            DATE not null,
  sls_typ_id_from_config   VARCHAR2(50) not null,
  last_updt_ts             DATE not null
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
alter table PA_TREND_ALLOC_HIST_DTLS_LOG
  add constraint PK_PA_TREND_ALLOC_HISTDTLS_LOG primary key (mrkt_id, dbt_on_sls_perd_id, sls_typ_id, dbt_bilng_day, sls_typ_id_from_config)
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

