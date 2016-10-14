CREATE TABLE MRKT_SLS_PERD 
(
  MRKT_ID NUMBER NOT NULL 
, SLS_PERD_ID NUMBER NOT NULL 
, SLS_ALOCTN_AUTO_USER_ID VARCHAR2(35 BYTE) 
, SLS_ALOCTN_AUTO_STRT_TS DATE 
, SLS_ALOCTN_AUTO_END_TS DATE
, SCT_CASH_VAL NUMBER(15,2)
, SCT_R_FACTOR NUMBER(6,4)
, SCT_PRCSNG_DT DATE
, SCT_ALOCTN_USER_ID VARCHAR2(35 BYTE) NOT NULL
, SCT_ALOCTN_STRT_TS DATE
, SCT_ALOCTN_END_TS DATE
, CREAT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER NOT NULL 
, CREAT_TS DATE DEFAULT SYSDATE NOT NULL 
, LAST_UPDT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER NOT NULL 
, LAST_UPDT_TS DATE DEFAULT SYSDATE NOT NULL 
, CREAT_UNPLND_OFFR_USER_ID VARCHAR2(35 BYTE) 
, CREAT_UNPLND_OFFR_STRT_TS DATE 
, CREAT_UNPLND_OFFR_END_TS DATE 
, CONSTRAINT PK_MRKT_SLS_PERD PRIMARY KEY 
  (
    MRKT_ID 
  , SLS_PERD_ID 
  )
  USING INDEX 
  (
      CREATE UNIQUE INDEX PK_MRKT_SLS_PERD ON MRKT_SLS_PERD (MRKT_ID ASC, SLS_PERD_ID ASC) 
      TABLESPACE &index_tablespace_name 
  )
  ENABLE ) 
TABLESPACE &data_tablespace_name 
;

COMMENT ON TABLE MRKT_SLS_PERD IS 'Market Sales Period.  Data that is required for every sales period.  This table contains only sales periods (not years and quarters as in the MRKT_PERD table).';

COMMENT ON COLUMN MRKT_SLS_PERD.SLS_ALOCTN_AUTO_USER_ID IS 'Sales allocation auto user id.  The user id that last ran the sales allocation auto processing for this market and sales period.';

COMMENT ON COLUMN MRKT_SLS_PERD.SLS_ALOCTN_AUTO_STRT_TS IS 'Sales allocation auto start time.  The last time that the sales allocation auto process was started.';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_CASH_VAL IS 'Cash Value entered in advance by the user, or updated via the MAPS Supply Chain Trends screen';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_R_FACTOR IS 'Derived number used to multiply units/sales at the current stage of a sales campaign to project final units/sales at the end of the target campaign';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_PRCSNG_DT IS 'The ‘billing day’ or latest processing date of the billing data so far at the point the Cash Value and/or R-Factor was changed';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_ALOCTN_USER_ID_DT IS 'User who last ran Supply Chain Trend Allocation – usually this will be ‘oracle’ indicating the automated daily job, but this may have an actual MAPS user id if the process is re-run from the Supply Chain Trend screen';

