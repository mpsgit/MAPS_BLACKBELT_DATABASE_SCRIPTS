set serveroutput on;

DECLARE
  a FS_MRKT_PRFL_BNCHMRK_LINE := NEW 
    FS_MRKT_PRFL_BNCHMRK_LINE(68,7777777,20170305,
	                          NEW FS_MRKT_PRFL_BNCHMRK_DTA_TABLE(NEW FS_MRKT_PRFL_BNCHMRK_DATA(7777771,'Y'),
							                                     NEW FS_MRKT_PRFL_BNCHMRK_DATA(7777772,'N')),
							  NULL,NULL,NULL,NULL);
--  b FS_MRKT_PRFL_BNCHMRK_LINE;
--  c FS_MRKT_PRFL_BNCHMRK_LINE;
BEGIN
  FS_MRKT_PRFL_BNCHMRK_SYNC.SYNC_BNCHMRK(a);
END;