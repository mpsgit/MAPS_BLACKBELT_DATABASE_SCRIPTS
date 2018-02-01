--------------------------------------------------------
--  File created - Wednesday-November-29-2017   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table MRKT_TRND_SLS_PERD_SLS_TYP
--------------------------------------------------------

  CREATE TABLE MRKT_TRND_SLS_PERD_SLS_TYP 
   (MRKT_ID NUMBER, 
    TRND_SLS_PERD_ID NUMBER, 
    SLS_TYP_ID NUMBER, 
    SCT_CASH_VALUE_ON NUMBER, 
    SCT_CASH_VALUE_OFF NUMBER, 
    SCT_PRCSNG_DT DATE, 
    SCT_ALOCTN_USER_ID VARCHAR2(35 BYTE) DEFAULT 'oracle', 
    SCT_ALOCTN_STRT_TS DATE, 
    SCT_ALOCTN_END_TS DATE, 
    SCT_BILNG_DT DATE, 
    SCT_AUTCLC_IND CHAR(1 BYTE), 
    CREAT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER, 
    CREAT_TS DATE DEFAULT SYSDATE, 
    LAST_UPDT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER, 
    LAST_UPDT_TS DATE DEFAULT SYSDATE
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE &data_tablespace_name ;

   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.MRKT_ID IS 'MRKT_ID';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.TRND_SLS_PERD_ID IS 'the content is TARGET sls_perd_id, and references (as part of FK) to mrkt_trnd_sls_perd table';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SLS_TYP_ID IS 'Sales type for which trend allocation was last calculated.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_CASH_VALUE_ON IS 'Cash Value entered (On-schedule) in advance by the user, or updated via the MAPS Supply Chain Trends screen.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_CASH_VALUE_OFF IS 'Cash Value entered (Off-schedule) in advance by the user, or updated via the MAPS Supply Chain Trends screen.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_PRCSNG_DT IS 'The billing day or latest processing date of the billing data so far at the point the Cash Value was changed.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_ALOCTN_USER_ID IS 'User who last ran Supply Chain Trend Allocation
– usually this will be oracle indicating the automated daily job,
          but this may have an actual MAPS user id if the process is re-run from the Supply Chain Trend screen.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_ALOCTN_STRT_TS IS 'Start time of Supply Chain Trend Allocation.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_ALOCTN_END_TS IS 'End Time of Supply Chain Trend Allocation.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_BILNG_DT IS 'Billing day for given campaign.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.SCT_AUTCLC_IND IS 'Whether automatic allocation set or not.';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.CREAT_USER_ID IS 'user''s ID create record';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.CREAT_TS IS 'Timestamp of create record';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.LAST_UPDT_USER_ID IS 'Last updating user''s ID';
 
   COMMENT ON COLUMN MRKT_TRND_SLS_PERD_SLS_TYP.LAST_UPDT_TS IS 'Timestamp of last update';
--------------------------------------------------------
--  DDL for Index PK_MRKT_TRND_SLS_PERD_SLS_TYP
--------------------------------------------------------

  CREATE UNIQUE INDEX PK_MRKT_TRND_SLS_PERD_SLS_TYP ON MRKT_TRND_SLS_PERD_SLS_TYP (MRKT_ID, TRND_SLS_PERD_ID, SLS_TYP_ID) 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE &index_tablespace_name ;
--------------------------------------------------------
--  Constraints for Table MRKT_TRND_SLS_PERD_SLS_TYP
--------------------------------------------------------

  ALTER TABLE MRKT_TRND_SLS_PERD_SLS_TYP ADD CONSTRAINT PK_MRKT_TRND_SLS_PERD_SLS_TYP PRIMARY KEY (MRKT_ID, TRND_SLS_PERD_ID, SLS_TYP_ID)
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE &index_tablespace_name  ENABLE;
 
  ALTER TABLE MRKT_TRND_SLS_PERD_SLS_TYP MODIFY (MRKT_ID NOT NULL ENABLE);
 
  ALTER TABLE MRKT_TRND_SLS_PERD_SLS_TYP MODIFY (TRND_SLS_PERD_ID NOT NULL ENABLE);
 
  ALTER TABLE MRKT_TRND_SLS_PERD_SLS_TYP MODIFY (SLS_TYP_ID NOT NULL ENABLE);
