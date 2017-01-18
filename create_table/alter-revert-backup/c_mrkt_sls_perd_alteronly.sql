ALTER TABLE MRKT_SLS_PERD 
NOCOMPRESS

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_CASH_VAL NUMBER(15,2) );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_R_FACTOR NUMBER(19,4) );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_PRCSNG_DT DATE );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_ALOCTN_USER_ID VARCHAR2(35) DEFAULT oracle );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_ALOCTN_STRT_TS DATE );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_ALOCTN_END_TS DATE );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_ONSCH_EST_BI24_IND CHAR(1) );

ALTER TABLE MRKT_SLS_PERD 
ADD (SCT_OFFSCH_EST_BI24_IND CHAR(1) );

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_ONSCH_EST_BI24_IND IS 'Use estimate in trend allocation if BI24 data is missing for on schedule';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_OFFSCH_EST_BI24_IND IS 'Use estimate in trend allocation if BI24 data is missing for off schedule';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_CASH_VAL IS 'Cash Value entered in advance by the user, or updated via the MAPS Supply Chain Trends screen';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_R_FACTOR IS 'Derived number used to multiply units/sales at the current stage of a sales campaign to project final units/sales at the end of the target campaign';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_PRCSNG_DT IS 'The ‘billing day’ or latest processing date of the billing data so far at the point the Cash Value and/or R-Factor was changed';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_ALOCTN_USER_ID IS 'User who last ran Supply Chain Trend Allocation – usually this will be ‘oracle’ indicating the automated daily job, but this may have an actual MAPS user id if the process is re-run from the Supply Chain Trend screen';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_ALOCTN_STRT_TS IS 'Start time of Supply Chain Trend Allocation';

COMMENT ON COLUMN MRKT_SLS_PERD.SCT_ALOCTN_END_TS IS 'End Time of Supply Chain Trend Allocation';

ALTER TABLE MRKT_SLS_PERD 
COMPRESS