-- Create table
create table MRKT_TRND_SLS_PERD_SLS_TYP
(
  mrkt_id                 NUMBER not null,
  trnd_sls_perd_id        NUMBER not null,
  sls_typ_id              NUMBER not null,
  sct_cash_value          NUMBER,
  sct_r_factor            NUMBER,
  sct_prcsng_dt           DATE,
  sct_aloctn_user_id      VARCHAR2(35) default 'oracle',
  sct_aloctn_strt_ts      DATE,
  sct_aloctn_end_ts       DATE,
  sct_est_bilng_dt        DATE,
  sct_bst_bilng_dt        DATE,
  sct_onsch_est_bi24_ind  CHAR(1),
  sct_offsch_est_bi24_ind CHAR(1),
  sct_autclc_est_ind      CHAR(1),
  sct_autclc_bst_ind      CHAR(1),
  creat_user_id           VARCHAR2(35) default USER,
  creat_ts                DATE default SYSDATE,
  last_updt_user_id       VARCHAR2(35) default USER,
  last_updt_ts            DATE default SYSDATE
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
  )
compress for all operations;
-- Add comments to the columns
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.trnd_sls_perd_id
  is 'the content is TARGET sls_perd_id, and references (as part of FK) to mrkt_trnd_sls_perd table';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sls_typ_id
  is 'Sales type for which trend allocation was last calculated.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_cash_value
  is 'Cash Value entered in advance by the user, or updated via the MAPS Supply Chain Trends screen.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_r_factor
  is 'Derived number used to multiply units/sales at the current stage of a sales campaign to project final units/sales at the end of the target campaign.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_prcsng_dt
  is 'The "billing day" or latest processing date of the billing data so far at the point the Cash Value and/or R-Factor was changed.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_aloctn_user_id
  is 'User who last ran Supply Chain Trend Allocation
– usually this will be "oracle" indicating the automated daily job,
          but this may have an actual MAPS user id if the process is re-run from the Supply Chain Trend screen.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_aloctn_strt_ts
  is 'Start time of Supply Chain Trend Allocation.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_aloctn_end_ts
  is 'End Time of Supply Chain Trend Allocation.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_est_bilng_dt
  is 'Billing day for EST for given campaign.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_bst_bilng_dt
  is 'Billing day for BST for given campaign.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_onsch_est_bi24_ind
  is 'Whether use estimate or not in trend allocation if BI24 data is missing for on schedule.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_offsch_est_bi24_ind
  is 'Whether use estimate or not in trend allocation if BI24 data is missing for off schedule.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_autclc_est_ind
  is 'Whether automatic EST allocation set or not.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.sct_autclc_bst_ind
  is 'Whether automatic BST allocation set or not.';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.creat_user_id
  is 'user''s ID create record';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.creat_ts
  is 'Timestamp of create record';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.last_updt_user_id
  is 'Last updating user''s ID';
comment on column MRKT_TRND_SLS_PERD_SLS_TYP.last_updt_ts
  is 'Timestamp of last update';
-- Create/Recreate primary, unique and foreign key constraints 
alter table MRKT_TRND_SLS_PERD_SLS_TYP
  add constraint PK_MRKT_TRND_SLS_PERD_SLS_TYP primary key (MRKT_ID, TRND_SLS_PERD_ID, SLS_TYP_ID)
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
alter table MRKT_TRND_SLS_PERD_SLS_TYP
  add constraint FK_MRKTTRNDSLSPRD_MTSP_SLSTYP foreign key (MRKT_ID, TRND_SLS_PERD_ID)
  references MRKT_TRND_SLS_PERD (MRKT_ID, TRND_SLS_PERD_ID);

