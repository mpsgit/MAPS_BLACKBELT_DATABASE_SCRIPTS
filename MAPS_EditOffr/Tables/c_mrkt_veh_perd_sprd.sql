create table MRKT_VEH_PERD_SPRD
( mrkt_id           NUMBER,
  offr_perd_id      NUMBER,
  veh_id            NUMBER,
  ver_id            NUMBER,
  sprd_nr           NUMBER,
  page_data         CLOB NOT NULL,
  creat_user_id     VARCHAR2(35) default USER not null,
  creat_ts          DATE default SYSDATE not null,
  last_updt_user_id VARCHAR2(35),
  last_updt_ts      DATE
)
tablespace &data_tablespace_name;
                             
alter table MRKT_VEH_PERD_SPRD add constraint PK_MRKT_VEH_PERD_SPRD
  primary key (mrkt_id,offr_perd_id,veh_id,ver_id,sprd_nr)
  using index tablespace &index_tablespace_name;

alter table MRKT_VEH_PERD_SPRD add constraint FK_MVPV_MRKTVEHPERDSPRD
  foreign key (mrkt_id, veh_id, offr_perd_id, ver_id)
  references MRKT_VEH_PERD_VER;
