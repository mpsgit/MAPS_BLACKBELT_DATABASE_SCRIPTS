CREATE TABLE mrkt_perd_typ_rf_ovrrd 
(
  MRKT_ID NUMBER NOT NULL 
, SLS_PERD_ID NUMBER NOT NULL 
, SLS_TYP_ID NUMBER NOT NULL 
, CATGRY_ID NUMBER NOT NULL 
, BRND_ID NUMBER NOT NULL 
, SGMT_ID NUMBER NOT NULL 
, FORM_ID NUMBER NOT NULL 
, R_FACTOR NUMBER NOT NULL 
, CREAT_USER_ID VARCHAR2(35) DEFAULT USER NOT NULL 
, CREAT_TS DATE DEFAULT SYSDATE NOT NULL 
, LAST_UPDT_USER_ID VARCHAR2(35) DEFAULT USER NOT NULL 
, LAST_UPDT_TS DATE DEFAULT SYSDATE NOT NULL 
, CONSTRAINT PK_mrkt_perd_typ_rf_ovrrd PRIMARY KEY (MRKT_ID, SLS_PERD_ID, SLS_TYP_ID)
  USING INDEX 
  (
      CREATE UNIQUE INDEX PK_mrkt_perd_typ_rf_ovrrd ON mrkt_perd_typ_rf_ovrrd (MRKT_ID ASC, SLS_PERD_ID ASC, SLS_TYP_ID ASC) 
      TABLESPACE &index_tablespace_name 
  ) ENABLE,
  CONSTRAINT FK_MRKTPERD_mrktperdtyp FOREIGN KEY (MRKT_ID, SLS_PERD_ID)
   REFERENCES MRKT_PERD (MRKT_ID, PERD_ID) ENABLE,
  CONSTRAINT FK_MRKT_mrktperdtyp FOREIGN KEY ( MRKT_ID )
   REFERENCES MRKT ( MRKT_ID ) ENABLE,
  CONSTRAINT FK_SLSTYP_mrktperdtyp FOREIGN KEY ( SLS_TYP_ID )
   REFERENCES SLS_TYP ( SLS_TYP_ID ) ENABLE 
) 
TABLESPACE &data_tablespace_name;

COMMENT ON TABLE mrkt_perd_typ_rf_ovrrd IS 'Trend offset - stores trend offset values counted by Market_ID and Trend Type.';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.MRKT_ID IS 'Market Id';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.SLS_PERD_ID IS 'Target Sales Campaign';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.SLS_TYP_ID IS 'Trend Type';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.CATGRY_ID IS 'Category for which special R-Factor is set';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.BRND_ID IS 'Brand for which special R-Factor is set';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.SGMT_ID IS 'Segment for which special R-Factor is set';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.FORM_ID_ID IS 'Form for which special R-Factor is set';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.CREAT_USER_ID IS 'User who created the record';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.CREAT_TS IS 'Timestamp the record was created';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.LAST_UPDT_USER_ID IS 'User who last updated the record';

COMMENT ON COLUMN mrkt_perd_typ_rf_ovrrd.LAST_UPDT_TS IS 'Timestamp the record was last updated';
