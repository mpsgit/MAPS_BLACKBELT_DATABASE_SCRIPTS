--------------------------------------------------------
--  File created - Wednesday-November-29-2017   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table MRKT_PERD_PARTS
--------------------------------------------------------

  CREATE TABLE MRKT_PERD_PARTS 
   (	MRKT_ID NUMBER, 
	EFF_CAMPGN_PERD_ID NUMBER, 
	LTE_DAY_NUM NUMBER, 
	GTE_DAY_NUM NUMBER,
	CREAT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER, 
	CREAT_TS DATE DEFAULT SYSDATE, 
	LAST_UPDT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER, 
	LAST_UPDT_TS DATE DEFAULT sysdate
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE &data_tablespace_name ;
 
   COMMENT ON COLUMN MRKT_PERD_PARTS.MRKT_ID IS 'Market to which the setting applies';
 
   COMMENT ON COLUMN MRKT_PERD_PARTS.EFF_CAMPGN_PERD_ID IS 'Period from which the setting will be applied';
 
   COMMENT ON COLUMN MRKT_PERD_PARTS.LTE_DAY_NUM IS 'Last day of the first part defined.';
 
   COMMENT ON COLUMN MRKT_PERD_PARTS.GTE_DAY_NUM IS 'First day of the third part defined';
  
   COMMENT ON COLUMN CUSTM_RUL_MSTR.CREAT_USER_ID IS 'The user who inserted the record';
 
   COMMENT ON COLUMN CUSTM_RUL_MSTR.CREAT_TS IS 'Date/timestamp of record creation';
 
   COMMENT ON COLUMN CUSTM_RUL_MSTR.LAST_UPDT_USER_ID IS 'The user who last changed the record';
 
   COMMENT ON COLUMN CUSTM_RUL_MSTR.LAST_UPDT_TS IS 'Date/timestamp of latest change';

   COMMENT ON TABLE MRKT_PERD_PARTS  IS 'Day numbers dividing a period to three parts (1-LTE, >LTE-<GTE and GTE to last day of the period) ';
--------------------------------------------------------
--  DDL for Index PK_MRKT_PERD_PARTS
--------------------------------------------------------

  CREATE UNIQUE INDEX PK_MRKT_PERD_PARTS ON MRKT_PERD_PARTS (MRKT_ID, EFF_CAMPGN_PERD_ID) 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE &index_tablespace_name ;
--------------------------------------------------------
--  Constraints for Table MRKT_PERD_PARTS
--------------------------------------------------------

  ALTER TABLE MRKT_PERD_PARTS ADD CONSTRAINT PK_MRKT_PERD_PARTS PRIMARY KEY (MRKT_ID, EFF_CAMPGN_PERD_ID)
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE &index_tablespace_name  ENABLE;
 
  ALTER TABLE MRKT_PERD_PARTS MODIFY (MRKT_ID NOT NULL ENABLE);
 
  ALTER TABLE MRKT_PERD_PARTS MODIFY (EFF_CAMPGN_PERD_ID NOT NULL ENABLE);

  ALTER TABLE MRKT_PERD_PARTS MODIFY (CREAT_USER_ID NOT NULL ENABLE);
 
  ALTER TABLE MRKT_PERD_PARTS MODIFY (CREAT_TS NOT NULL ENABLE);
 
  ALTER TABLE MRKT_PERD_PARTS MODIFY (LAST_UPDT_USER_ID NOT NULL ENABLE);
 
  ALTER TABLE MRKT_PERD_PARTS MODIFY (LAST_UPDT_TS NOT NULL ENABLE);


