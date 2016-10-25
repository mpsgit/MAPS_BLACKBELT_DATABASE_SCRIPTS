CREATE INDEX FK_MRKTPERD_SCTFSCOVRRD ON SCT_FSC_OVRRD (MRKT_ID, SLS_PERD_ID) 
TABLESPACE &index_tablespace_name;

CREATE INDEX FK_MRKT_SCTFSCOVRRD ON SCT_FSC_OVRRD (MRKT_ID) 
TABLESPACE &index_tablespace_name;

CREATE INDEX FK_SLSTYP_SCTFSCOVRRD ON SCT_FSC_OVRRD (SLS_TYP_ID) 
TABLESPACE &index_tablespace_name;

