DROP TABLE mrkt_veh_perd_sprd;

CREATE TABLE mrkt_veh_perd_sprd
( mrkt_id           NUMBER,
  offr_perd_id      NUMBER,
  veh_id            NUMBER,
  ver_id            NUMBER,
  sprd_nr           NUMBER,
  page_data         CLOB NOT NULL,
  creat_user_id     VARCHAR2(35) DEFAULT USER NOT NULL,
  creat_ts          DATE DEFAULT SYSDATE NOT NULL,
  last_updt_user_id VARCHAR2(35),
  last_updt_ts      DATE
)
TABLESPACE &data_tablespace_name;
                             
ALTER TABLE mrkt_veh_perd_sprd ADD CONSTRAINT pk_mrkt_veh_perd_sprd
  PRIMARY KEY (mrkt_id,offr_perd_id,veh_id,ver_id,sprd_nr)
  USING INDEX TABLESPACE &index_tablespace_name;

ALTER TABLE mrkt_veh_perd_sprd ADD CONSTRAINT fk_mvpv_mrktvehperdsprd
  FOREIGN KEY (mrkt_id, veh_id, offr_perd_id, ver_id)
  REFERENCES mrkt_veh_perd_ver;
