-- Create table
create table TA_CONFIG
(
  mrkt_id             NUMBER not null,
  eff_sls_perd_id     NUMBER not null,
  src_table           VARCHAR2(10) not null,
  src_sls_typ_id      NUMBER not null,
  est_src_table       VARCHAR2(10),
  est_src_sls_typ_id  NUMBER,
  offst_lbl_id        NUMBER not null,
  sls_typ_grp_nm      VARCHAR2(20) not null,
  sls_typ_lbl_id      NUMBER not null,
  x_sls_typ_lbl_id    NUMBER,
  offst_val_src_sls   NUMBER not null,
  offst_val_trgt_sls  NUMBER not null,
  offst_val_src_offr  NUMBER not null,
  offst_val_trgt_offr NUMBER not null,
  grid_typ            VARCHAR2(1) not null,
  r_factor            NUMBER,
  trgt_sls_typ_id     NUMBER not null,
  commnt              VARCHAR2(2000)
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
  );
-- Add comments to the table 
comment on table TA_CONFIG
  is 'Configuration rules for Trend Allocation';
-- Add comments to the columns 
comment on column TA_CONFIG.mrkt_id
  is 'Market_ID';
comment on column TA_CONFIG.eff_sls_perd_id
  is 'Effective Sales Period';
comment on column TA_CONFIG.src_table
  is 'INFORMATIVE only - Source table for getting quantity units';
comment on column TA_CONFIG.src_sls_typ_id
  is 'Sales Type for filtering the source table';
comment on column TA_CONFIG.est_src_table
  is 'INFORMATIVE only - Source for estimates at trend allocation (if data not found in SRC_TABLE)';
comment on column TA_CONFIG.est_src_sls_typ_id
  is 'Sales type for filtering the EST_SRC table';
comment on column TA_CONFIG.offst_lbl_id
  is 'ID of record from TA_DICT table, defining screen settings for the result LINES';
comment on column TA_CONFIG.sls_typ_grp_nm
  is 'Group of the sales type, defining the used calculation - must be in (''BI24'', ''ESTIMATE'', ''TREND'', ''ACTUAL'', ''FORECASTED_DBT'', ''FORECASTED_DMS'')';
comment on column TA_CONFIG.sls_typ_lbl_id
  is 'ID of record from TA_DICT table, defining screen settings for the result COLUMNS';
comment on column TA_CONFIG.x_sls_typ_lbl_id
  is 'ID of record from TA_DICT table, defining screen settings for the secondary result COLUMNS';
comment on column TA_CONFIG.offst_val_src_sls
  is 'Offset for sales period to get source sales period corresponding to OFFST_LBL_ID';
comment on column TA_CONFIG.offst_val_trgt_sls
  is 'Offset for sales period to get target sales period corresponding to OFFST_LBL_ID';
comment on column TA_CONFIG.offst_val_src_offr
  is 'Offset for sales period to get source offer period corresponding to OFFST_LBL_ID';
comment on column TA_CONFIG.offst_val_trgt_offr
  is 'Offset for sales period to get target offer period corresponding to OFFST_LBL_ID';
comment on column TA_CONFIG.grid_typ
  is 'INFORMATIVE only - (H)istoric or Trend (A)llocation grid will show the data corresponding to this config line';
comment on column TA_CONFIG.r_factor
  is 'R-factor used for creating input data when estimate is used instead of facts.';
comment on column TA_CONFIG.trgt_sls_typ_id
  is 'Sales type used for selecting the CONFIG to apply';
comment on column TA_CONFIG.commnt
  is 'Comment on the CONFIG line';
-- Create/Recreate primary, unique and foreign key constraints 
alter table TA_CONFIG
  add constraint PK_TA_CONFIG primary key (MRKT_ID, EFF_SLS_PERD_ID, TRGT_SLS_TYP_ID, SLS_TYP_GRP_NM, OFFST_LBL_ID)
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
alter table TA_CONFIG
  add constraint FK1_TADICT_TACONFIG foreign key (OFFST_LBL_ID)
  references TA_DICT (LBL_ID);
alter table TA_CONFIG
  add constraint FK2_TADICT_TACONFIG foreign key (SLS_TYP_LBL_ID)
  references TA_DICT (LBL_ID);
alter table TA_CONFIG
  add constraint FK3_TADICT_TACONFIG foreign key (X_SLS_TYP_LBL_ID)
  references TA_DICT (LBL_ID);
-- Create/Recreate check constraints 
alter table TA_CONFIG
  add constraint C_TA_CONFIG_GRIDTYP
  check (GRID_TYP in('H','A'));
alter table TA_CONFIG
  add constraint C_TA_CONFIG_SLSTYPGRPNM
  check (SLS_TYP_GRP_NM in('BI24', 'ESTIMATE', 'TREND', 'ACTUAL', 'FORECASTED_DBT', 'FORECASTED_DMS'));
