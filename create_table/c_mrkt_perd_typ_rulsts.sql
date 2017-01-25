CREATE TABLE MRKT_PERD_TYP_RULSTS 
(
  MRKT_ID NUMBER NOT NULL 
, TRG_PERD_ID NUMBER NOT NULL 
, SLS_TYP_ID NUMBER NOT NULL 
, CATGRY_ID NUMBER NOT NULL 
, BRND_ID NUMBER NOT NULL 
, SGMT_ID NUMBER NOT NULL 
, FORM_ID NUMBER NOT NULL 
, R_FACTOR NUMBER NOT NULL 
, HIST_PERD_ID NUMBER NOT NULL 
, HIST_PERD_TYP VARCHAR2(1 BYTE) NOT NULL 
, CREAT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER NOT NULL 
, CREAT_TS DATE DEFAULT SYSDATE NOT NULL 
, LAST_UPDT_USER_ID VARCHAR2(35 BYTE) DEFAULT USER NOT NULL 
, LAST_UPDT_TS DATE DEFAULT SYSDATE NOT NULL 
, 
	 CONSTRAINT MRKT_PERD_TYP_RULSTS_CHK1 CHECK (HIST_PERD_TYP IN('O','F','T')) ENABLE, 
	 CONSTRAINT FK_MRKTPERD_MRKTPERDTYPRULSTS FOREIGN KEY (MRKT_ID, TRG_PERD_ID)
	  REFERENCES MRKT_PERD (MRKT_ID, PERD_ID) ENABLE, 
	 CONSTRAINT FK2_MRKTPERD_MRKTPERDTYPRULSTS FOREIGN KEY (MRKT_ID, HIST_PERD_ID)
	  REFERENCES MRKT_PERD (MRKT_ID, PERD_ID) ENABLE, 
	 CONSTRAINT FK_SLSTYP_MRKTPERDTYPRULSTS FOREIGN KEY (SLS_TYP_ID)
	  REFERENCES SLS_TYP (SLS_TYP_ID) ENABLE
)
TABLESPACE APP_DATA_WEDEV
;

   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."MRKT_ID" IS 'Market Id';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."TRG_PERD_ID" IS 'Target Sales Campaign';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."SLS_TYP_ID" IS 'Trend Type';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."CATGRY_ID" IS 'Category for which special R-Factor is set';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."BRND_ID" IS 'Brand for which special R-Factor is set';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."SGMT_ID" IS 'Segment for which special R-Factor is set';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."FORM_ID" IS 'Form for which special R-Factor is set';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."R_FACTOR" IS 'R-factor for ID-set';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."HIST_PERD_ID" IS 'One of historic periods used to calculate default r-factor for the specified rule';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."HIST_PERD_ID" IS 'Type of period included: on-, off schedule or trendsetter (O, F or T)';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."CREAT_USER_ID" IS 'User who created the record';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."CREAT_TS" IS 'Timestamp the record was created';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."LAST_UPDT_USER_ID" IS 'User who last updated the record';
   COMMENT ON COLUMN "WEDEV"."MRKT_PERD_TYP_RULSTS"."LAST_UPDT_TS" IS 'Timestamp the record was last updated';
   COMMENT ON TABLE "WEDEV"."MRKT_PERD_TYP_RULSTS"  IS 'Rulesets - stores rules used to calculate for Market_ID, Targer period, Trend Type and ID-vector.';