-- Create table
create table DLY_BILNG_ADJSTMNT
(
  dly_bilng_id      NUMBER not null,
  unit_qty          NUMBER(9) not null,
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35) default USER not null,
  last_updt_ts      DATE default SYSDATE not null
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
comment on table DLY_BILNG_ADJSTMNT
  is 'Daily Billing Adjustment';
-- Add comments to the columns 
comment on column DLY_BILNG_ADJSTMNT.dly_bilng_id
  is 'Daily Billing Id';
comment on column DLY_BILNG_ADJSTMNT.unit_qty
  is 'Adjusted Unit Qty';
comment on column DLY_BILNG_ADJSTMNT.creat_user_id
  is 'User who created the record';
comment on column DLY_BILNG_ADJSTMNT.creat_ts
  is 'Timestamp the record was created';
comment on column DLY_BILNG_ADJSTMNT.last_updt_user_id
  is 'User who last updated the record';
comment on column DLY_BILNG_ADJSTMNT.last_updt_ts
  is 'Timestamp the record was last updated';
-- Create/Recreate primary, unique and foreign key constraints 
alter table DLY_BILNG_ADJSTMNT
  add constraint PK_DLY_BILNG_ADJSTMNT primary key (DLY_BILNG_ID)
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
