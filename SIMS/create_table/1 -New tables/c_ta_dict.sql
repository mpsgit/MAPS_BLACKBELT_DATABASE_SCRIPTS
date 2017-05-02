-- Create table
create table TA_DICT
(
  lbl_id       NUMBER not null,
  mrkt_id      NUMBER,
  lbl_desc     VARCHAR2(32) not null,
  lbl_wght     NUMBER not null,
  lbl_wght_dir VARCHAR2(1) not null,
  int_lbl_desc VARCHAR2(32) not null
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
comment on table TA_DICT
  is 'Trend Allocation Dictionary';
-- Add comments to the columns 
comment on column TA_DICT.lbl_id
  is 'Label ID (OFFST_LBL_ID or SLS_TYP_LBL_ID)';
comment on column TA_DICT.mrkt_id
  is 'Market ID';
comment on column TA_DICT.lbl_desc
  is 'Label Description';
comment on column TA_DICT.lbl_wght
  is 'Weight used for positioning on screen';
comment on column TA_DICT.lbl_wght_dir
  is 'Type of Entry (O - Offset, S- Sales Type)';
comment on column TA_DICT.int_lbl_desc
  is 'Internal Label Description (Label Type) - must be in(''ON-SCHEDULE'', ''OFF-SCHEDULE'', ''TRENDSETTER'', ''TRENDSETTER-2'', ''BI24'', ''ESTIMATE'', ''TREND'', ''ACTUAL'')
';
-- Create/Recreate primary, unique and foreign key constraints 
alter table TA_DICT
  add constraint PK_TA_DICT primary key (LBL_ID)
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
-- Create/Recreate check constraints 
alter table TA_DICT
  add constraint C_TA_DICT_INTLBLDESC
  check (INT_LBL_DESC in ('ON-SCHEDULE', 'OFF-SCHEDULE', 'TRENDSETTER', 'TRENDSETTER-2', 'BI24', 'ESTIMATE', 'TREND', 'ACTUAL'));
alter table TA_DICT
  add constraint C_TA_DICT_LBLWGHTDIR
  check (LBL_WGHT_DIR in ('O','S'));
