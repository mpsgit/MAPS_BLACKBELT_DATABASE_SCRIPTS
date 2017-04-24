-- Create table
create table CASH_VAL_RF_HIST
(
  mrkt_id             NUMBER not null,
  sls_perd_id         NUMBER not null,
  cash_val            NUMBER(15,2) not null,
  r_factor            NUMBER(19,4),
  prcsng_dt           DATE,
  last_updt_user_id   VARCHAR2(35) not null,
  last_updt_ts        DATE not null,
  onsch_est_bi24_ind  CHAR(1),
  offsch_est_bi24_ind CHAR(1),
  sls_typ_id          NUMBER,
  autclc_est_ind      CHAR(1),
  autclc_bst_ind      CHAR(1)
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
comment on table CASH_VAL_RF_HIST
  is 'Cash Value and R Factor History';
-- Add comments to the columns 
comment on column CASH_VAL_RF_HIST.mrkt_id
  is 'Market ID';
comment on column CASH_VAL_RF_HIST.sls_perd_id
  is 'Target Sales Campaign';
comment on column CASH_VAL_RF_HIST.cash_val
  is 'Previous Cash Value';
comment on column CASH_VAL_RF_HIST.r_factor
  is 'Previous R factor';
comment on column CASH_VAL_RF_HIST.prcsng_dt
  is 'Previous ''billing day''';
comment on column CASH_VAL_RF_HIST.last_updt_user_id
  is 'Last updating user''s ID';
comment on column CASH_VAL_RF_HIST.last_updt_ts
  is 'Timestamp of last update';
comment on column CASH_VAL_RF_HIST.onsch_est_bi24_ind
  is 'Use estimate in trend allocation if BI24 data is missing for on schedule';
comment on column CASH_VAL_RF_HIST.offsch_est_bi24_ind
  is 'Use estimate in trend allocation if BI24 data is missing for off schedule';
comment on column CASH_VAL_RF_HIST.sls_typ_id
  is 'Sales type for which trend allocation was last calculated';
comment on column CASH_VAL_RF_HIST.autclc_est_ind
  is 'Automatic EST calculation set.';
comment on column CASH_VAL_RF_HIST.autclc_bst_ind
  is 'Automatic BST calculation set.';
-- Create/Recreate indexes 
create index IDX_CASH_VAL_RF_HIST on CASH_VAL_RF_HIST (MRKT_ID, SLS_PERD_ID)
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
alter table CASH_VAL_RF_HIST
  add constraint PK_CASH_VAL_RF_HIST primary key (MRKT_ID, SLS_PERD_ID, LAST_UPDT_USER_ID, LAST_UPDT_TS)
  using index 
  tablespace &index_tablespace_name
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 40K
    next 64K
    minextents 1
    maxextents unlimited
  );
