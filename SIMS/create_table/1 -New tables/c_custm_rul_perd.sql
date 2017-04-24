-- Create table
create table CUSTM_RUL_PERD
(
  campgn_perd_id    NUMBER not null,
  rul_id            NUMBER not null,
  period_list       VARCHAR2(2048),
  r_factor          NUMBER,
  r_factor_manual   NUMBER,
  creat_user_id     VARCHAR2(35) default USER,
  creat_ts          DATE default sysdate,
  last_updt_user_id VARCHAR2(35) default USER,
  last_updt_ts      DATE default sysdate
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
comment on column CUSTM_RUL_PERD.creat_user_id
  is 'The user who inserted the record';
comment on column CUSTM_RUL_PERD.creat_ts
  is 'Date/timestamp of record creation';
comment on column CUSTM_RUL_PERD.last_updt_user_id
  is 'The user who last changed the record';
comment on column CUSTM_RUL_PERD.last_updt_ts
  is 'Date/timestamp of latest change';
-- Create/Recreate primary, unique and foreign key constraints 
alter table CUSTM_RUL_PERD
  add constraint UK_PR_CUSTMRULPERD unique (CAMPGN_PERD_ID, RUL_ID)
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
alter table CUSTM_RUL_PERD
  add constraint FK_RULID_CUSTMRULPERD foreign key (RUL_ID)
  references CUSTM_RUL_MSTR (RUL_ID);
