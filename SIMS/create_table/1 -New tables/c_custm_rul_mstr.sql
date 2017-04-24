-- Create table
create table CUSTM_RUL_MSTR
(
  mrkt_id           NUMBER not null
  rul_id            NUMBER not null,
  rul_nm            VARCHAR2(50) not null,
  rul_desc          VARCHAR2(200),
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35) default USER not null,
  last_updt_ts      DATE default sysdate not null,
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
-- Add comments to the table 
comment on column CUSTM_RUL_MSTR.mrkt_id
  is 'Market';
comment on table CUSTM_RUL_MSTR
  is 'Describes master data for custom group rules';
-- Add comments to the columns 
comment on column CUSTM_RUL_MSTR.rul_id
  is 'Rule Id';
comment on column CUSTM_RUL_MSTR.rul_nm
  is 'Rule Name';
comment on column CUSTM_RUL_MSTR.rul_desc
  is 'Rule Description';
comment on column CUSTM_RUL_MSTR.creat_user_id
  is 'The user who inserted the record';
comment on column CUSTM_RUL_MSTR.creat_ts
  is 'Date/timestamp of record creation';
comment on column CUSTM_RUL_MSTR.last_updt_user_id
  is 'The user who last changed the record';
comment on column CUSTM_RUL_MSTR.last_updt_ts
  is 'Date/timestamp of latest change';
-- Create/Recreate primary, unique and foreign key constraints 
alter table CUSTM_RUL_MSTR
  add constraint PK_CUSTM_RUL_MSTR primary key (RUL_ID)
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
alter table CUSTM_RUL_MSTR
  add constraint UK_MRKTRULNM_CUSTMRULMSTR unique (MRKT_ID, RUL_NM)
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
alter table CUSTM_RUL_MSTR
  add constraint FK_MRKT_CUSTMRULMSTR_FK1 foreign key (MRKT_ID)
  references MRKT (MRKT_ID);
