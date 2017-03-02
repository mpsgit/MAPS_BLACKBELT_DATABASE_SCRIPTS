CREATE TABLE TA_CONFIG 
(
  MRKT_ID NUMBER NOT NULL 
, EFF_SLS_PERD_ID NUMBER NOT NULL 
, SRC_TABLE VARCHAR2(10) NOT NULL 
, SRC_SLS_TYP_ID NUMBER NOT NULL 
, EST_SRC_TABLE VARCHAR2(10) 
, EST_SRC_SLS_TYP_ID NUMBER 
, OFFST_LBL_ID NUMBER NOT NULL 
, SLS_TYP_GRP_NM VARCHAR2(20) NOT NULL 
, SLS_TYP_LBL_ID NUMBER NOT NULL 
, X_SLS_TYP_LBL_ID NUMBER 
, OFFST_VAL_SRC_SLS NUMBER NOT NULL 
, OFFST_VAL_TRGT_SLS NUMBER NOT NULL 
, OFFST_VAL_SRC_OFFR NUMBER NOT NULL 
, OFFST_VAL_TRGT_OFFR NUMBER NOT NULL 
, GRID_TYP VARCHAR2(1) NOT NULL 
, R_FACTOR NUMBER 
, TRGT_SLS_TYP_ID NUMBER NOT NULL 
, COMMNT VARCHAR2(2000) 
, CONSTRAINT TA_CONFIG_PK PRIMARY KEY 
  (
    MRKT_ID 
  , EFF_SLS_PERD_ID 
  , TRGT_SLS_TYP_ID 
  , SLS_TYP_GRP_NM 
  , OFFST_LBL_ID 
  )
  USING INDEX 
  (
      CREATE UNIQUE INDEX PK_TA_CONFIG ON TA_CONFIG (MRKT_ID ASC, EFF_SLS_PERD_ID ASC, TRGT_SLS_TYP_ID ASC, SLS_TYP_GRP_NM ASC, OFFST_LBL_ID ASC) 
      TABLESPACE &index_tablespace_name 
  )
  ENABLE 
) 
TABLESPACE &data_tablespace_name;

ALTER TABLE TA_CONFIG
ADD CONSTRAINT C_TA_CONFIG_GRIDTYP CHECK 
(GRID_TYP in('H','A'))
ENABLE;

ALTER TABLE TA_CONFIG
ADD CONSTRAINT C_TA_CONFIG_SLSTYPGRPNM CHECK 
(SLS_TYP_GRP_NM in('BI24', 'ESTIMATE', 'OPERATIONAL ESTIMATE', 'TREND', 'ACTUAL', 'FORECASTED FROM DBT', 'FORECASTED FROM DMS'))
ENABLE;

COMMENT ON TABLE TA_CONFIG IS 'Configuration rules for Trend Allocation';

COMMENT ON COLUMN TA_CONFIG.MRKT_ID IS 'Market_ID';

COMMENT ON COLUMN TA_CONFIG.EFF_SLS_PERD_ID IS 'Effective Sales Period';

COMMENT ON COLUMN TA_CONFIG.SRC_TABLE IS 'Source table for getting quantity units';

COMMENT ON COLUMN TA_CONFIG.SRC_SLS_TYP_ID IS 'Sales Type for filtering the source table';

COMMENT ON COLUMN TA_CONFIG.EST_SRC_TABLE IS 'Source for estimates at trend allocation (if data not found in SRC_TABLE)';

COMMENT ON COLUMN TA_CONFIG.EST_SRC_SLS_TYP_ID IS 'Sales type for filtering the EST_SRC table';

COMMENT ON COLUMN TA_CONFIG.OFFST_LBL_ID IS 'ID of record from TA_DICT table, defining screen settings for the result lines';

COMMENT ON COLUMN TA_CONFIG.SLS_TYP_GRP_NM IS 'Group of the sales type, defining the used calculation version';

COMMENT ON COLUMN TA_CONFIG.SLS_TYP_LBL_ID IS 'ID of record from TA_DICT table, defining screen settings for the result';

COMMENT ON COLUMN TA_CONFIG.X_SLS_TYP_LBL_ID IS 'ID of record from TA_DICT table, defining screen setting for the secondary resultresult';

COMMENT ON COLUMN TA_CONFIG.OFFST_VAL_SRC_SLS IS 'Offset for sales period to to get source sales period corresponding to OFFST_LBL_ID';

COMMENT ON COLUMN TA_CONFIG.OFFST_VAL_TRGT_SLS IS 'Offset for sales period to to get target sales period corresponding to OFFST_LBL_ID';

COMMENT ON COLUMN TA_CONFIG.OFFST_VAL_SRC_OFFR IS 'Offset for sales period to to get source offer period corresponding to OFFST_LBL_ID';

COMMENT ON COLUMN TA_CONFIG.OFFST_VAL_TRGT_OFFR IS 'Offset for sales period to to get target offer period corresponding to OFFST_LBL_ID';

COMMENT ON COLUMN TA_CONFIG.GRID_TYP IS '(H)istoric or Trend (A)llocation grid will show the data corresponding to this config line';

COMMENT ON COLUMN TA_CONFIG.R_FACTOR IS 'R-factor used for creating input data when estimate is used instead of facts.';

COMMENT ON COLUMN TA_CONFIG.TRGT_SLS_TYP_ID IS 'Sales type used for selecting the rules to apply';

COMMENT ON COLUMN TA_CONFIG.COMMNT IS 'Comment on the rule line';