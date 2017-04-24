-- Create table
create table MRKT_PERD_SKU_BIAS
(
  mrkt_id           NUMBER not null,
  sls_perd_id       NUMBER not null,
  sku_id            NUMBER not null,
  bias_pct          NUMBER(6,2) not null,
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35) default USER not null,
  last_updt_ts      DATE default SYSDATE not null,
  sls_typ_id        NUMBER
)
tablespace &data_tablespace_name
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 40K
    next 40K
    minextents 1
    maxextents unlimited
  )
compress for all operations;
-- Add comments to the table 
comment on table MRKT_PERD_SKU_BIAS
  is 'Market Period SKU BIAS';
-- Add comments to the columns 
comment on column MRKT_PERD_SKU_BIAS.mrkt_id
  is 'Market ID';
comment on column MRKT_PERD_SKU_BIAS.sls_perd_id
  is 'Target Sales Campaign';
comment on column MRKT_PERD_SKU_BIAS.sku_id
  is 'SKU ID';
comment on column MRKT_PERD_SKU_BIAS.bias_pct
  is 'Bias percentage to be applied';
comment on column MRKT_PERD_SKU_BIAS.creat_user_id
  is 'The user who inserted the record';
comment on column MRKT_PERD_SKU_BIAS.creat_ts
  is 'Date/timestamp of record creation';
comment on column MRKT_PERD_SKU_BIAS.last_updt_user_id
  is 'The user who last changed the record';
comment on column MRKT_PERD_SKU_BIAS.last_updt_ts
  is 'Date/timestamp of latest change';
comment on column MRKT_PERD_SKU_BIAS.sls_typ_id
  is 'Sales Type to which the BIAS applies';
-- Create/Recreate indexes 
create index FK_MRKTPERD_MRKTPERDSKUBIAS on MRKT_PERD_SKU_BIAS (MRKT_ID, SLS_PERD_ID)
  tablespace &index_tablespace_name
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 40K
    next 40K
    minextents 1
    maxextents unlimited
  );
create index FK_MRKTSKU_MRKTPERDSKUBIAS on MRKT_PERD_SKU_BIAS (MRKT_ID, SKU_ID)
  tablespace &index_tablespace_name
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 40K
    next 40K
    minextents 1
    maxextents unlimited
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table MRKT_PERD_SKU_BIAS
  add constraint PK_MRKT_PERD_SKU_BIAS primary key (MRKT_ID, SLS_PERD_ID, SKU_ID)
  using index 
  tablespace &index_tablespace_name
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 40K
    next 40K
    minextents 1
    maxextents unlimited
  );
alter table MRKT_PERD_SKU_BIAS
  add constraint FK_MRKTPERD_MRKTPERDSKUBIAS foreign key (MRKT_ID, SLS_PERD_ID)
  references MRKT_PERD (MRKT_ID, PERD_ID);
-- Create/Recreate check constraints 
alter table MRKT_PERD_SKU_BIAS
  add constraint MRKT_PERD_SKU_BIAS_CHK1
  check (BIAS_PCT between 0 and 1000);
