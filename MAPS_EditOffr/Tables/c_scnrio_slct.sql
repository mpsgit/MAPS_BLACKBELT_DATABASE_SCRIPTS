--DROP TABLE scnrio_slct;
CREATE TABLE scnrio_slct( mrkt_id NUMBER,
                          offr_perd_id NUMBER,
                          scnrio_id  NUMBER,
                          creat_user_id     VARCHAR2(35) default USER not null,
                          creat_ts          DATE default SYSDATE not null,
                          last_updt_user_id VARCHAR2(35),
                          last_updt_ts      DATE
                          );
alter table scnrio_slct add constraint PK_scnrio_slct
  primary key (mrkt_id,offr_perd_id,scnrio_id);                          
                          
--SELECT * FROM scnrio_slct;
