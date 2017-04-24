-- Create table
create table TREND_ALLOC_HIST_DTLS_LOG
(
  mrkt_id        NUMBER not null,
  sls_perd_id    NUMBER not null,
  sls_typ_id     NUMBER not null,
  bilng_day      DATE not null,
  sls_typ_grp_nm VARCHAR2(32) not null,
  last_updt_ts   DATE not null
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
  )
compress for all operations;
-- Create/Recreate primary, unique and foreign key constraints 
alter table TREND_ALLOC_HIST_DTLS_LOG
  add constraint PK_TREND_ALLOC_HIST_DTLS_LOG primary key (MRKT_ID, SLS_PERD_ID, SLS_TYP_ID, BILNG_DAY, SLS_TYP_GRP_NM)
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
  );
