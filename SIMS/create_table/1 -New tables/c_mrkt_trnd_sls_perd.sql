-- Create table
create table MRKT_TRND_SLS_PERD
(
  mrkt_id                   NUMBER not null,
  trnd_sls_perd_id          NUMBER not null,
  trnd_aloctn_auto_user_id  VARCHAR2(35),
  trnd_aloctn_auto_strt_ts  DATE,
  trnd_aloctn_auto_end_ts   DATE,
  creat_unplnd_offr_user_id VARCHAR2(35),
  creat_unplnd_offr_strt_ts DATE,
  creat_unplnd_offr_end_ts  DATE,
  creat_user_id             VARCHAR2(35) default USER not null,
  creat_ts                  DATE default SYSDATE not null,
  last_updt_user_id         VARCHAR2(35) default USER not null,
  last_updt_ts              DATE default SYSDATE not null
)
tablespace &data_tablespace_name
  pctfree 10
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
-- Add comments to the columns 
comment on column MRKT_TRND_SLS_PERD.trnd_aloctn_auto_user_id
  is 'Trend allocation auto user id.
The user id that last ran the trend allocation auto processing for this market and sales period.';
comment on column MRKT_TRND_SLS_PERD.trnd_aloctn_auto_strt_ts
  is 'Trend allocation auto start time.
The last time that the trend allocation auto process was started.';
comment on column MRKT_TRND_SLS_PERD.trnd_aloctn_auto_end_ts
  is 'Trend allocation auto end time.
The last time that the trend allocation auto process ended.';
-- Create/Recreate primary, unique and foreign key constraints 
alter table MRKT_TRND_SLS_PERD
  add constraint PK_MRKT_TRND_SLS_PERD primary key (MRKT_ID, TRND_SLS_PERD_ID)
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
alter table MRKT_TRND_SLS_PERD
  add constraint FK_MRKTPERD_MRKTTRNDSLSPERD foreign key (MRKT_ID, TRND_SLS_PERD_ID)
  references MRKT_PERD (MRKT_ID, PERD_ID);

