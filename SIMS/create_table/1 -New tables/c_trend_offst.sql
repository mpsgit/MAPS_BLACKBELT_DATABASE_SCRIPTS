-- Create table
create table TREND_OFFST
(
  mrkt_id           NUMBER not null,
  eff_sls_perd_id   NUMBER not null,
  sls_typ_id        NUMBER not null,
  offst             NUMBER not null,
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35) default USER,
  last_updt_ts      DATE default SYSDATE
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
comment on table TREND_OFFST
  is 'Trend offset - stores trend offset values counted by Market_ID and Trend Type.';
-- Add comments to the columns 
comment on column TREND_OFFST.mrkt_id
  is 'Market Id';
comment on column TREND_OFFST.eff_sls_perd_id
  is 'Effective Sales Campaign';
comment on column TREND_OFFST.sls_typ_id
  is 'Trend Type';
comment on column TREND_OFFST.offst
  is 'Offset Value';
comment on column TREND_OFFST.creat_user_id
  is 'User who created the record';
comment on column TREND_OFFST.creat_ts
  is 'Timestamp the record was created';
comment on column TREND_OFFST.last_updt_user_id
  is 'User who last updated the record';
comment on column TREND_OFFST.last_updt_ts
  is 'Timestamp the record was last updated';
-- Create/Recreate indexes 
create index FK_MRKTPERD_TRENDOFFST on TREND_OFFST (MRKT_ID, EFF_SLS_PERD_ID)
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
create index FK_MRKT_TRENDOFFST on TREND_OFFST (MRKT_ID)
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
create index FK_SLSTYP_TRENDOFFST on TREND_OFFST (SLS_TYP_ID)
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
alter table TREND_OFFST
  add constraint PK_TREND_OFFST primary key (MRKT_ID, EFF_SLS_PERD_ID, SLS_TYP_ID)
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
alter table TREND_OFFST
  add constraint FK_MRKTPERD_TRENDOFFST foreign key (MRKT_ID, EFF_SLS_PERD_ID)
  references MRKT_PERD (MRKT_ID, PERD_ID);
alter table TREND_OFFST
  add constraint FK_MRKT_TRENDOFFST foreign key (MRKT_ID)
  references MRKT (MRKT_ID);
alter table TREND_OFFST
  add constraint FK_SLSTYP_TRENDOFFST foreign key (SLS_TYP_ID)
  references SLS_TYP (SLS_TYP_ID);
