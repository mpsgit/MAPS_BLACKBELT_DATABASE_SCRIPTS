-- Create table
create table SCT_FSC_OVRRD
(
  mrkt_id           NUMBER not null,
  sls_perd_id       NUMBER not null,
  sls_typ_id        NUMBER not null,
  fsc_cd            VARCHAR2(8) not null,
  sct_unit_qty      NUMBER(9) not null,
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35) default USER not null,
  last_updt_ts      DATE default SYSDATE not null,
  offst_lbl_id      NUMBER not null
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
comment on table SCT_FSC_OVRRD
  is 'Details for FSC records excluded from r-factor and trend unit calculations.';
-- Add comments to the columns 
comment on column SCT_FSC_OVRRD.mrkt_id
  is 'Market Id';
comment on column SCT_FSC_OVRRD.sls_perd_id
  is 'Target Sales Campaign';
comment on column SCT_FSC_OVRRD.sls_typ_id
  is 'Trend Type';
comment on column SCT_FSC_OVRRD.fsc_cd
  is 'FSC Code';
comment on column SCT_FSC_OVRRD.sct_unit_qty
  is 'Total Trend Units to be recorded for this FSC in the specified Market/Campaign';
comment on column SCT_FSC_OVRRD.creat_user_id
  is 'User who created the record';
comment on column SCT_FSC_OVRRD.creat_ts
  is 'Timestamp the record was created';
comment on column SCT_FSC_OVRRD.last_updt_user_id
  is 'User who last updated the record';
comment on column SCT_FSC_OVRRD.last_updt_ts
  is 'Timestamp the record was last updated';
-- Create/Recreate indexes 
create index FK_MRKTPERD_SCTFSCOVRRD on SCT_FSC_OVRRD (MRKT_ID, SLS_PERD_ID)
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
create index FK_MRKT_SCTFSCOVRRD on SCT_FSC_OVRRD (MRKT_ID)
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
create index FK_SLSTYP_SCTFSCOVRRD on SCT_FSC_OVRRD (SLS_TYP_ID)
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
alter table SCT_FSC_OVRRD
  add constraint PK_SCT_FSC_OVRRD primary key (MRKT_ID, SLS_PERD_ID, SLS_TYP_ID, FSC_CD, OFFST_LBL_ID)
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
alter table SCT_FSC_OVRRD
  add constraint FK_MRKTPERD_SCTFSCOVRRD foreign key (MRKT_ID, SLS_PERD_ID)
  references MRKT_PERD (MRKT_ID, PERD_ID);
alter table SCT_FSC_OVRRD
  add constraint FK_MRKT_SCTFSCOVRRD foreign key (MRKT_ID)
  references MRKT (MRKT_ID);
alter table SCT_FSC_OVRRD
  add constraint FK_SLSTYP_SCTFSCOVRRD foreign key (SLS_TYP_ID)
  references SLS_TYP (SLS_TYP_ID);
