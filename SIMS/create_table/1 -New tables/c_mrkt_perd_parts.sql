-- Create table
create table MRKT_PERD_PARTS
(
  mrkt_id            NUMBER not null,
  eff_campgn_perd_id NUMBER not null,
  lte_day_num        NUMBER,
  gte_day_num        NUMBER
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
comment on table MRKT_PERD_PARTS
  is 'Day numbers dividing a period to three parts (1-LTE, >LTE-<GTE and GTE to last day of the period) ';
-- Add comments to the columns 
comment on column MRKT_PERD_PARTS.mrkt_id
  is 'Market to which the setting applies';
comment on column MRKT_PERD_PARTS.eff_campgn_perd_id
  is 'Period from which the setting will be applied';
comment on column MRKT_PERD_PARTS.lte_day_num
  is 'Last day of the first part defined.';
comment on column MRKT_PERD_PARTS.gte_day_num
  is 'First day of the third part defined';
-- Create/Recreate primary, unique and foreign key constraints 
alter table MRKT_PERD_PARTS
  add constraint PK_MRKT_PERD_PARTS primary key (MRKT_ID, EFF_CAMPGN_PERD_ID)
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
alter table MRKT_PERD_PARTS
  add constraint FK_MRKTPERD_MRKTPERDPARTS foreign key (MRKT_ID, EFF_CAMPGN_PERD_ID)
  references MRKT_PERD (MRKT_ID, PERD_ID);
