-- Create table
create table CUSTM_RUL_SKU_LIST
(
  rul_id            NUMBER not null,
  sku_id            NUMBER not null,
  creat_user_id     VARCHAR2(35) default USER,
  creat_ts          DATE default SYSDATE,
  last_updt_user_id VARCHAR2(35) default USER,
  last_updt_ts      DATE default SYSDATE
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
  );
-- Add comments to the columns 
comment on column CUSTM_RUL_SKU_LIST.rul_id
  is 'Rule Id';
comment on column CUSTM_RUL_SKU_LIST.sku_id
  is 'SKU Id contained in the rule';
comment on column CUSTM_RUL_SKU_LIST.creat_user_id
  is 'The user who inserted the record';
comment on column CUSTM_RUL_SKU_LIST.creat_ts
  is 'Date/timestamp of record creation';
comment on column CUSTM_RUL_SKU_LIST.last_updt_user_id
  is 'The user who last changed the record';
comment on column CUSTM_RUL_SKU_LIST.last_updt_ts
  is 'Date/timestamp of latest change';
-- Create/Recreate primary, unique and foreign key constraints 
alter table CUSTM_RUL_SKU_LIST
  add constraint UK_CUSTM_RUL_SKU_LIST unique (RUL_ID, SKU_ID)
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
-- Create/Recreate primary, unique and foreign key constraints 
alter table CUSTM_RUL_SKU_LIST
  add constraint CUSTM_RUL_SKU_LIST_FK1 foreign key (RUL_ID)
  references CUSTM_RUL_MSTR (RUL_ID);
