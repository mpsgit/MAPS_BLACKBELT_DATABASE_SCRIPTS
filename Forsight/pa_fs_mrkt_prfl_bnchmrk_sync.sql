create or replace PACKAGE FS_MRKT_PRFL_BNCHMRK_SYNC AS 

  TYPE FS_MRKT_PRFL_BNCHMRK_DATA IS RECORD
    ( BNCHMRK_PRFL_CD number(7),
      DFALT_IND char(1)
    );
  
  TYPE FS_MRKT_PRFL_BNCHMRK_DTA_TABLE IS TABLE OF FS_MRKT_PRFL_BNCHMRK_DATA  index by binary_integer;
  
  TYPE FS_MRKT_PRFL_BNCHMRK_LINE IS RECORD
   ( MRKT_ID number,
     PRFL_CD number(7,0),
     EFF_PERD_ID number,
     BENCHMARK_DATA FS_MRKT_PRFL_BNCHMRK_DTA_TABLE,
     CREAT_USR_ID varchar2(35),
     CREAT_TS date,
     LAST_UPDT_USR_ID varchar2(35),
     LAST_UPDT_TS date
     );
  
  PROCEDURE SYNCH_BNCHMRK(p_parm IN FS_MRKT_PRFL_BNCHMRK_LINE);

END FS_MRKT_PRFL_BNCHMRK_SYNC;

create or replace PACKAGE BODY FS_MRKT_PRFL_BNCHMRK_SYNC AS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE SYNC_BNCHMRK(p_parm IN FS_MRKT_PRFL_BNCHMRK_LINE) AS
    TYPE data_table IS TABLE OF number index by pls_integer;
    p_data_count number :=p_parm.BENCHMARK_DATA.count;
    used_bnchmrk_prfl_codes data_table;
  BEGIN
    DBMS_OUTPUT.PUT_LINE(p_parm.MRKT_ID);
-- If no benchmark_profile_code referre in input,
--   delete all records identified by market_id, profile_code and effective_period_id
--   then add a new record, where benchmark_profile_code is the same as the profile_code and the default_indicator is set to 'yes' ('D')
    IF p_data_count=0 THEN
      DBMS_OUTPUT.PUT_LINE('DELETE FROM FS_MRKT_PRFL_BNCHMRK
        WHERE MRKT_ID=p_parm.MRKT_ID AND PRFL_CD=p_parm.PRFL_CD AND EFF_PERD_ID= p_parm.EFF_PERD_ID;
      INSERT INTO FS_MRKT_PRFL_BNCHMRK(MRKT_ID,PRFL_CD,EFF_PERD_ID,BNCHMRK_PRFL_CD,DFALT_IND)
        VALUES (p_parm.MRKT_ID,p_parm.PRFL_CD,p_parm.EFF_PERD_ID,p_parm.PRFL_CD,''D'');');
    ELSE
-- else insert new and modify the existing records if needed
      DBMS_OUTPUT.PUT_LINE(p_data_count);
      MERGE INTO FS_MRKT_PRFL_BNCHMRK target
        USING (SELECT p_parm.MRKT_ID as MRKT_ID, p_parm.PRFL_CD PRFL_CD, p_parm.EFF_PERD_ID EFF_PERD_ID, BNCHMRK_PRFL_CD, DFALT_IND FROM p_parm.BENCHMARK_DATA) source
          ON (source.MRKT_ID=target.MRKT_ID AND source.PRFL_CD=target.PRFL_CD AND source.EFF_PERD_ID=target.EFF_PERD_ID AND source.BNCHMRK_PRFL_CD=target.BNCHMRK_PRFL_CD)
        WHEN MATCHED THEN
          UPDATE SET target.DFALT_IND = p_parm.BENCHMARK_DATA.DFALT_IND WHERE target.DFALT_IND!=source.DFALT_IND
        WHEN NOT MATCHED THEN
          INSERT (MRKT_ID,PRFL_CD,EFF_PERD_ID,BNCHMRK_PRFL_CD,DFALT_IND)
            VALUES (p_parm.MRKT_ID,p_parm.PRFL_CD,p_parm.EFF_PERD_ID,source.BNCHMRK_PRFL_CD,source.DFALT_IND);
--   then delete the records identified by market_id, profile_code, effective_period_id and benchmark_profile_code not mentioned in the input (p_parm).      
      FOR i IN 1..p_data_count LOOP
        used_bnchmrk_prfl_codes(i):=p_parm.BENCHMARK_DATA(i).BNCHMRK_PRFL_CD;
      END LOOP;
      DBMS_OUTPUT.PUT_LINE(used_bnchmrk_prfl_codes.count);
      DBMS_OUTPUT.PUT_LINE('DELETE FROM FS_MRKT_PRFL_BNCHMRK
        WHERE MRKT_ID=p_parm.MRKT_ID AND PRFL_CD=p_parm.PRFL_CD AND EFF_PERD_ID= p_parm.EFF_PERD_ID
          AND BNCHMRK_PRFL_CD NOT IN(used_bnchmrk_prfl_codes)');
    END IF;
    NULL;
  END SYNC_BNCHMRK;

END FS_MRKT_PRFL_BNCHMRK_SYNC;