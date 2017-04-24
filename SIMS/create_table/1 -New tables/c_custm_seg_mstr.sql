-- Create table
create table CUSTM_SEG_MSTR
(
  mrkt_id           NUMBER not null,
  rul_id            NUMBER not null,
  rul_nm            VARCHAR2(50) not null,
  rul_desc          VARCHAR2(200),
  offst_lbl_id      NUMBER not null,
  catgry_id         NUMBER,
  sls_cls_cd        NUMBER,
  veh_id            NUMBER,
  perd_part         NUMBER,
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35) default USER not null,
  last_updt_ts      DATE default sysdate not null
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
comment on table CUSTM_SEG_MSTR
  is 'Describes master data for custom segment rules';
-- Add comments to the columns 
comment on column CUSTM_SEG_MSTR.mrkt_id
  is 'Market Id';
comment on column CUSTM_SEG_MSTR.rul_id
  is 'Rule Id';
comment on column CUSTM_SEG_MSTR.rul_nm
  is 'Rule Name';
comment on column CUSTM_SEG_MSTR.rul_desc
  is 'Rule Description';
comment on column CUSTM_SEG_MSTR.offst_lbl_id
  is 'Condition for offset';
comment on column CUSTM_SEG_MSTR.catgry_id
  is 'Condition for category';
comment on column CUSTM_SEG_MSTR.sls_cls_cd
  is 'Condition for sales class';
comment on column CUSTM_SEG_MSTR.veh_id
  is 'Condition for vehicle';
comment on column CUSTM_SEG_MSTR.perd_part
  is 'Condition for period part selection (1- beginning, 2 - middle, 3 - end)';
comment on column CUSTM_SEG_MSTR.creat_user_id
  is 'The user who inserted the record';
comment on column CUSTM_SEG_MSTR.creat_ts
  is 'Date/timestamp of record creation';
comment on column CUSTM_SEG_MSTR.last_updt_user_id
  is 'The user who last changed the record';
comment on column CUSTM_SEG_MSTR.last_updt_ts
  is 'Date/timestamp of latest change';
-- Create/Recreate primary, unique and foreign key constraints 
alter table CUSTM_SEG_MSTR
  add constraint PK_CUSTM_SEG_MSTR primary key (RUL_ID)
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
alter table CUSTM_SEG_MSTR
  add constraint UK_MOCSVP_CUSTMSEGMSTR unique (MRKT_ID, OFFST_LBL_ID, CATGRY_ID, SLS_CLS_CD, VEH_ID, PERD_PART)
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
alter table CUSTM_SEG_MSTR
  add constraint UK_MRKTRULNM_CUSTMSEGMSTR unique (MRKT_ID, RUL_NM)
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
alter table CUSTM_SEG_MSTR
  add constraint FK_MRKT_CUSTMSEGMSTR foreign key (MRKT_ID)
  references MRKT (MRKT_ID);
-- Create/Recreate check constraints 
alter table CUSTM_SEG_MSTR
  add constraint CHK_CUSTM_SEG_MSTR
  check (PERD_PART in (1, 2, 3));
