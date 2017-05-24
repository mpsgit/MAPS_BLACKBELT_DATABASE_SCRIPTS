-- Create table
create table FRCST_BOOST_XCLUSN_MRKT_PERD
(
  mrkt_id           NUMBER not null,
  trgt_offr_perd_id NUMBER not null,
  catgry_id         NUMBER,
  sls_cls_cd        VARCHAR2(5),
  sgmt_id           NUMBER,
  creat_user_id     VARCHAR2(35) not null,
  creat_ts          DATE not null,
  last_updt_user_id VARCHAR2(35) not null,
  last_updt_ts      DATE not null
)
tablespace &data_tablespace_name
  pctfree 0
  pctused 40
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
    pctincrease 0
  )
compress;
-- Add comments to the table 
comment on table FRCST_BOOST_XCLUSN_MRKT_PERD
  is 'Forecast boost exclusion. This is for exclusions in the target market (not source market).
A NULL Category assumes all Categories for the specified Sales Class and Segment are excluded.
A NULL Sales Class assumes all Sales Classes for the specified Category and Segment are excluded.
A NULL Segment assumes all Segments for the specified Category and Sales Class are excluded.
All Category, Sales Class and Segment must not be null.';
-- Add comments to the columns 
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.catgry_Id
  is 'The Category to be excluded for forecast boost in the target market.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.sls_cls_cd
  is 'The Sales Class to be excluded for forecast boost in the target market.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.sgmt_Id
  is 'The Segment to be excluded for forecast boost in the target market.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.creat_user_id
  is 'The MAPS Application USER ID of the user (person or process) that created the row.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.creat_ts
  is 'The date and time the row was created.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.last_updt_user_id
  is 'Contains the MAPS USER ID of the user (person or process) that last updated the row.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.last_updt_ts
  is 'The date and time the row was last updated.';
-- Create/Recreate indexes 
create unique index UI_FRCSTBSTXCLSNMRKTPRD_MPVCSS on FRCST_BOOST_XCLUSN_MRKT_PERD (MRKT_ID, TRGT_OFFR_PERD_ID, CATGRY_ID, SLS_CLS_CD, SGMT_ID)
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
    pctincrease 50
  )
  compress 3;
-- Create/Recreate primary, unique and foreign key constraints 
alter table FRCST_BOOST_XCLUSN_MRKT_PERD
  add constraint FK_SLSCLS_FRCSTBSTXCLSN_MP foreign key (SLS_CLS_CD)
  references SLS_CLS (SLS_CLS_CD);
alter table FRCST_BOOST_XCLUSN_MRKT_PERD
  add constraint FK_CTGR_FRCSTBSTXCLSN_MP foreign key (CATGRY_ID)
  references CATGRY (CATGRY_ID);
alter table FRCST_BOOST_XCLUSN_MRKT_PERD
  add constraint FK_SGMT_FRCSTBSTXCLSN_MP foreign key (SGMT_ID)
  references SGMT (SGMT_ID);
-- Grant/Revoke object privileges 
grant select, insert, update, delete on FRCST_BOOST_XCLUSN_MRKT_PERD to CEMAPSP_ETL;
grant select on FRCST_BOOST_XCLUSN_MRKT_PERD to MAPS_SUPPORT;
grant select on FRCST_BOOST_XCLUSN_MRKT_PERD to MPSRO;
