-- Create table
create table FRCST_BOOST_XCLUSN_MRKT_PERD
(
  mrkt_id           NUMBER not null,
  trgt_offr_perd_id NUMBER not null,
  catgry_id_list    VARCHAR2(1500),
  sls_cls_cd_list   VARCHAR2(1500),
  sgmt_id_list      VARCHAR2(3000),
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
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.catgry_id_list
  is 'The Categories to be excluded for forecast boost in the target market.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.sls_cls_cd_list
  is 'The Sales Classes to be excluded for forecast boost in the target market.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.sgmt_id_list
  is 'The Segments to be excluded for forecast boost in the target market.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.creat_user_id
  is 'The MAPS Application USER ID of the user (person or process) that created the row.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.creat_ts
  is 'The date and time the row was created.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.last_updt_user_id
  is 'The MAPS Application USER ID of the user (person or process) that last updated the row.';
comment on column FRCST_BOOST_XCLUSN_MRKT_PERD.last_updt_ts
  is 'The date and time the row was last updated.';
-- Create/Recreate indexes 
create index UI_FRCSTBSTXCLSNMRKTPRD_MPVCSS on FRCST_BOOST_XCLUSN_MRKT_PERD (MRKT_ID, TRGT_OFFR_PERD_ID, CATGRY_ID_LIST, SLS_CLS_CD_LIST, SGMT_ID_LIST)
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
  compress;
-- Grant/Revoke object privileges 
grant select, insert, update, delete on FRCST_BOOST_XCLUSN_MRKT_PERD to CEMAPSP_ETL;
grant select on FRCST_BOOST_XCLUSN_MRKT_PERD to MAPS_SUPPORT;
grant select on FRCST_BOOST_XCLUSN_MRKT_PERD to MPSRO;
