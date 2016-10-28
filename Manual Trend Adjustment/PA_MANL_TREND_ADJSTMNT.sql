create or replace PACKAGE PA_MANL_TREND_ADJSTMNT AS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 26/10/2016
  * Description  : First created 
  ******************************************************/
  PROCEDURE INITIATE_TREND_OFFSET_TABLE (p_eff_sls_perd_id IN NUMBER);
  
  FUNCTION GET_TREND_TYPE_LIST RETURN OBJ_TREND_TYPE_TABLE PIPELINED;
  
  FUNCTION GET_TARGET_CAMPAIGN(p_mrkt_id IN NUMBER, p_sls_perd_id IN NUMBER, p_sls_typ_id IN NUMBER) RETURN NUMBER;
  
  PROCEDURE SET_MANL_TREND_ADJSTMNT(p_mrkt_id IN NUMBER,
                                  p_sls_perd_id IN NUMBER,
                                  p_sls_typ_id IN NUMBER,
                                  p_fsc_cd IN NUMBER,
                                  p_sct_unit_qty IN NUMBER,
                                  p_user_id IN VARCHAR2,
                                  p_stus OUT NUMBER);
                                  
  FUNCTION GET_MANL_TREND_ADJSTMNT(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_sls_typ_id IN NUMBER
                        ) RETURN OBJ_MANL_TREND_ADJSTMNT_TABLE PIPELINED;  

  FUNCTION GET_MANL_TREND_ADJSTMNT2(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_sls_typ_id IN NUMBER,
                        p_fsc_cd_array IN NUMBER_ARRAY
                        ) RETURN OBJ_MANL_TREND_ADJSTMNT_TABLE PIPELINED;  

END PA_MANL_TREND_ADJSTMNT;

create or replace PACKAGE BODY PA_MANL_TREND_ADJSTMNT AS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 26/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE INITIATE_TREND_OFFSET_TABLE(p_eff_sls_perd_id IN NUMBER) AS
-- Not for regular use, just for initiating the empty table
--  with rules defined at definition of OFFST column
  BEGIN
    INSERT INTO TREND_OFFST(MRKT_ID,EFF_SLS_PERD_ID,SLS_TYP_ID,OFFST)
    SELECT MRKT_ID,EFF_SLS_PERD_ID,SLS_TYP_ID,
           CASE WHEN M_CNTRY_CD='UK' and SLS_TYP_ID = 3 THEN 2 ELSE 1 END OFFST
      FROM 
        (SELECT SLS_TYP_ID,SLS_TYP_NM 
           FROM SLS_TYP_GRP
             JOIN SLS_TYP USING(SLS_TYP_GRP_ID)
           WHERE SLS_TYP_GRP_DESC_TXT='Trend') TRT, 
        (SELECT MRKT_ID,MIN(PERD_ID) EFF_SLS_PERD_ID, MAX(CNTRY_CD) M_CNTRY_CD
           FROM MRKT_PERD
             JOIN MRKT USING(MRKT_ID)
           WHERE PERD_ID>=p_eff_sls_perd_id GROUP BY MRKT_ID ) MPL
    ;
  END INITIATE_TREND_OFFSET_TABLE;
  
  PROCEDURE CREATE_SLS_TYPE_CONFG(p_config_item_id IN NUMBER, p_seq_nr NUMBER, p_config_item_val NUMBER) AS
-- Not for regular use, just for initiating the if no config item defined at all
--  with rules defined at definition of OFFST column
    BEGIN
    INSERT INTO CONFIG_ITEM(CONFIG_ITEM_ID,CONFIG_ITEM_DESC_TXT,CONFIG_ITEM_LABL_TXT,SEQ_NR)
         VALUES(p_config_item_id,'Sales Type Id for Trend Allocation',
       'Sales Type Id for Trend Allocation',p_seq_nr);
    INSERT INTO MRKT_CONFIG_ITEM(MRKT_ID,CONFIG_ITEM_ID,MRKT_CONFIG_ITEM_DESC_TXT,MRKT_CONFIG_ITEM_LABL_TXT,MRKT_CONFIG_ITEM_VAL_TXT)
    SELECT MM.MRKT_ID,p_config_item_id,'Sales Type Id for Trend Allocation',
         'Sales Type Id for Trend Allocation',p_config_item_val
    FROM MRKT MM
    WHERE NOT EXISTS(SELECT MRKT_ID FROM MRKT_CONFIG_ITEM MCI WHERE MCI.MRKT_ID=MM.MRKT_ID AND MCI.CONFIG_ITEM_ID=10000 );
    END CREATE_SLS_TYPE_CONFG;


  FUNCTION GET_TREND_TYPE_LIST RETURN OBJ_TREND_TYPE_TABLE PIPELINED AS
    CURSOR cc IS
      SELECT OBJ_TREND_TYPE_LINE(SLS_TYP_ID,SLS_TYP_NM) cline
        FROM SLS_TYP WHERE SLS_TYP_GRP_ID=2;
  BEGIN
    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_TREND_TYPE_LIST;

  FUNCTION GET_TARGET_CAMPAIGN(p_mrkt_id IN NUMBER, p_sls_perd_id IN NUMBER, p_sls_typ_id IN NUMBER) RETURN NUMBER AS
    res NUMBER;
  BEGIN
    SELECT PA_MAPS_PUBLIC.perd_plus(p_mrkt_id,p_sls_perd_id,OFFST) TARGET_PERD_ID
      INTO res 
      FROM (SELECT MAX(MRKT_ID) mrkt_id,
             MAX(EFF_SLS_PERD_ID) eff_sls_perd_id,
             MAX(SLS_TYP_ID) sls_typ_id
        FROM  TREND_OFFST 
          WHERE MRKT_ID=p_mrkt_id 
            AND EFF_SLS_PERD_ID<=p_sls_perd_id 
            AND SLS_TYP_ID=p_sls_typ_id)
      JOIN TREND_OFFST USING(MRKT_ID,EFF_SLS_PERD_ID,SLS_TYP_ID);
    RETURN res;
  END GET_TARGET_CAMPAIGN;

  FUNCTION GET_MANL_TREND_ADJSTMNT(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_sls_typ_id IN NUMBER
                        ) RETURN OBJ_MANL_TREND_ADJSTMNT_TABLE PIPELINED AS
    CURSOR cc IS
      WITH  ACT_FSC AS
      (SELECT FSC_CD, MRKT_ID, MAX(STRT_PERD_ID) MAX_PERD_ID 
                 FROM MRKT_FSC 
                 WHERE MRKT_ID=p_mrkt_id AND STRT_PERD_ID<=p_sls_perd_id
                 GROUP BY FSC_CD, MRKT_ID),
      MESP AS
        (SELECT DLY_BILNG_MTCH_ID
           FROM MRKT_EFF_SLS_PERD
           WHERE MRKT_ID       = p_mrkt_id
             AND EFF_SLS_PERD_ID =
               (SELECT MAX (MESP.EFF_SLS_PERD_ID)
                  FROM MRKT_EFF_SLS_PERD MESP
                  WHERE MESP.MRKT_ID = p_mrkt_id
                    AND MESP.EFF_SLS_PERD_ID <= p_sls_perd_id
                )
        )
        ,
        SSFO AS
        (SELECT 
           SFO.FSC_CD, SFO.SCT_UNIT_QTY, SFO.CREAT_USER_ID,SFO.CREAT_TS,
           SFO.LAST_UPDT_TS, (U.USER_FRST_NM || ' ' || U.USER_LAST_NM) USER_NM  
           FROM SCT_FSC_OVRRD SFO
             LEFT JOIN MPS_USER U ON U.USER_NM=SFO.LAST_UPDT_USER_ID
           WHERE SFO.MRKT_ID=p_mrkt_id AND SFO.SLS_PERD_ID=p_sls_perd_id AND SFO.SLS_TYP_ID=p_sls_typ_id
        )
      SELECT OBJ_MANL_TREND_ADJSTMNT_LINE(
        DB.FSC_CD,
        MAX(FSC.PROD_DESC_TXT),
        MAX(FSC.SKU_ID),
        MAX(MS.LCL_SKU_NM),
        SUM(DB.UNIT_QTY),
        MAX(SFO.SCT_UNIT_QTY),
        MAX(SFO.LAST_UPDT_TS),
        MAX(SFO.USER_NM)
        ) cline
      FROM   DLY_BILNG DB
        JOIN DLY_BILNG_CNTRL DBC
          ON NVL ( DBC.LCL_BILNG_ACTN_CD, DB.LCL_BILNG_ACTN_CD )   = DB.LCL_BILNG_ACTN_CD
            AND NVL ( DBC.LCL_BILNG_TRAN_TYP, DB.LCL_BILNG_TRAN_TYP ) = DB.LCL_BILNG_TRAN_TYP
            AND NVL ( DBC.LCL_BILNG_OFFR_TYP, DB.LCL_BILNG_OFFR_TYP ) = DB.LCL_BILNG_OFFR_TYP
            AND NVL ( DBC.LCL_BILNG_DEFRD_CD, DB.LCL_BILNG_DEFRD_CD ) = DB.LCL_BILNG_DEFRD_CD
            AND NVL ( DBC.LCL_BILNG_SHPNG_CD, DB.LCL_BILNG_SHPNG_CD ) = DB.LCL_BILNG_SHPNG_CD
        JOIN MESP USING(DLY_BILNG_MTCH_ID)
        JOIN MRKT_CONFIG_ITEM MCI
          ON MCI.MRKT_ID=DB.MRKT_ID 
            AND MCI.MRKT_CONFIG_ITEM_VAL_TXT=DBC.SLS_TYP_ID 
            AND MCI.CONFIG_ITEM_ID=10000
        LEFT JOIN ACT_FSC ACT
          ON ACT.FSC_CD=DB.FSC_CD AND ACT.MRKT_ID=DB.MRKT_ID
        JOIN MRKT_FSC FSC
          ON FSC.MRKT_ID=DB.MRKT_ID AND FSC.FSC_CD=DB.FSC_CD AND FSC.STRT_PERD_ID=ACT.MAX_PERD_ID   
        LEFT JOIN MRKT_SKU MS
          ON MS.MRKT_ID=DB.MRKT_ID AND MS.SKU_ID=FSC.SKU_ID
        LEFT JOIN SSFO SFO
          ON SFO.FSC_CD=DB.FSC_CD
      WHERE DB.MRKT_ID = p_mrkt_id
        AND DB.SLS_PERD_ID = p_sls_perd_id
      GROUP BY DB.FSC_CD;
  BEGIN
    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_MANL_TREND_ADJSTMNT;
  
  FUNCTION GET_MANL_TREND_ADJSTMNT2(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER,
                        p_sls_typ_id IN NUMBER,
                        p_fsc_cd_array IN NUMBER_ARRAY
                        ) RETURN OBJ_MANL_TREND_ADJSTMNT_TABLE PIPELINED AS
    CURSOR cc IS
      WITH  ACT_FSC AS
      (SELECT FSC_CD, MRKT_ID, MAX(STRT_PERD_ID) MAX_PERD_ID 
                 FROM MRKT_FSC 
                 WHERE MRKT_ID=p_mrkt_id AND STRT_PERD_ID<=p_sls_perd_id
                 GROUP BY FSC_CD, MRKT_ID),
      MESP AS
        (SELECT DLY_BILNG_MTCH_ID
           FROM MRKT_EFF_SLS_PERD
           WHERE MRKT_ID       = p_mrkt_id
             AND EFF_SLS_PERD_ID =
               (SELECT MAX (MESP.EFF_SLS_PERD_ID)
                  FROM MRKT_EFF_SLS_PERD MESP
                  WHERE MESP.MRKT_ID = p_mrkt_id
                    AND MESP.EFF_SLS_PERD_ID <= p_sls_perd_id
                )
        ),
      SSFO AS
        (SELECT 
           SFO.FSC_CD, SFO.SCT_UNIT_QTY, SFO.CREAT_USER_ID,SFO.CREAT_TS,
           SFO.LAST_UPDT_TS, (U.USER_FRST_NM || ' ' || U.USER_LAST_NM) USER_NM  
           FROM SCT_FSC_OVRRD SFO
             LEFT JOIN MPS_USER U ON U.USER_NM=SFO.LAST_UPDT_USER_ID
           WHERE SFO.MRKT_ID=p_mrkt_id AND SFO.SLS_PERD_ID=p_sls_perd_id AND SFO.SLS_TYP_ID=p_sls_typ_id
        )
      SELECT OBJ_MANL_TREND_ADJSTMNT_LINE(
        DB.FSC_CD,
        MAX(FSC.PROD_DESC_TXT),
        MAX(FSC.SKU_ID),
        MAX(MS.LCL_SKU_NM),
        SUM(DB.UNIT_QTY),
        MAX(SFO.SCT_UNIT_QTY),
        MAX(SFO.LAST_UPDT_TS),
        MAX(SFO.USER_NM)
        ) cline
      FROM   DLY_BILNG DB
        JOIN DLY_BILNG_CNTRL DBC
          ON NVL ( DBC.LCL_BILNG_ACTN_CD, DB.LCL_BILNG_ACTN_CD )   = DB.LCL_BILNG_ACTN_CD
            AND NVL ( DBC.LCL_BILNG_TRAN_TYP, DB.LCL_BILNG_TRAN_TYP ) = DB.LCL_BILNG_TRAN_TYP
            AND NVL ( DBC.LCL_BILNG_OFFR_TYP, DB.LCL_BILNG_OFFR_TYP ) = DB.LCL_BILNG_OFFR_TYP
            AND NVL ( DBC.LCL_BILNG_DEFRD_CD, DB.LCL_BILNG_DEFRD_CD ) = DB.LCL_BILNG_DEFRD_CD
            AND NVL ( DBC.LCL_BILNG_SHPNG_CD, DB.LCL_BILNG_SHPNG_CD ) = DB.LCL_BILNG_SHPNG_CD
        JOIN MESP USING(DLY_BILNG_MTCH_ID)
        JOIN MRKT_CONFIG_ITEM MCI
          ON MCI.MRKT_ID=p_mrkt_id 
            AND MCI.MRKT_CONFIG_ITEM_VAL_TXT=DBC.SLS_TYP_ID 
            AND MCI.CONFIG_ITEM_ID=10000
        LEFT JOIN ACT_FSC ACT
          ON ACT.FSC_CD=DB.FSC_CD AND ACT.MRKT_ID=DB.MRKT_ID
        JOIN MRKT_FSC FSC
          ON FSC.MRKT_ID=DB.MRKT_ID AND FSC.FSC_CD=DB.FSC_CD AND FSC.STRT_PERD_ID=ACT.MAX_PERD_ID   
        LEFT JOIN MRKT_SKU MS
          ON MS.MRKT_ID=DB.MRKT_ID AND MS.SKU_ID=FSC.SKU_ID
        LEFT JOIN SSFO SFO
          ON SFO.FSC_CD=DB.FSC_CD
      WHERE DB.MRKT_ID = p_mrkt_id
        AND DB.SLS_PERD_ID = p_sls_perd_id
        AND DB.FSC_CD IN(select column_value from table( p_fsc_cd_array))
      GROUP BY DB.FSC_CD;
  BEGIN
    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_MANL_TREND_ADJSTMNT2;
  
  PROCEDURE SET_MANL_TREND_ADJSTMNT(p_mrkt_id IN NUMBER,
                                  p_sls_perd_id IN NUMBER,
                                  p_sls_typ_id IN NUMBER,
                                  p_fsc_cd IN NUMBER,
                                  p_sct_unit_qty IN NUMBER,
                                  p_user_id IN VARCHAR2,
                                  p_stus OUT NUMBER) AS
  /*********************************************************
  * INPUT p_sct_unit_qty IS NULL handled as record has to be deleted
  *
  * Possible OUT Values
  * 0 - success
  * 2 - database error in DELETE, UPDATE or INSERT statements
  * 3 - Obligatory foreign keys (SLS_TYP,MRKT_PERD) not found
  ******************************************************/
    counter1 NUMBER;
    counter2 NUMBER;
  BEGIN
    p_STUS:=0;
    -- precheck constraints
    SELECT count(*) INTO counter1 FROM SLS_TYP WHERE SLS_TYP_ID=p_sls_typ_id;
    SELECT count(*) INTO counter2 FROM MRKT_PERD WHERE MRKT_ID=p_mrkt_id AND PERD_ID=p_sls_perd_id;
    IF counter1+counter2=2 THEN
    -- p_new_bi24_units IS NULL, the record if exists
      IF p_sct_unit_qty IS NULL THEN
        BEGIN
        SAVEPOINT before_delete;
          DELETE FROM SCT_FSC_OVRRD 
            WHERE MRKT_ID=p_mrkt_id
              AND SLS_PERD_ID=p_sls_perd_id
              AND SLS_TYP_ID=p_sls_typ_id
              AND FSC_CD=p_fsc_cd;
        EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_delete; p_stus:=2;
        END;
    -- otherwise upsert using p_new_bi24_units
      ELSE
        BEGIN
        SAVEPOINT before_upsert; 		 
          MERGE INTO SCT_FSC_OVRRD trgt
            USING (SELECT p_mrkt_id t1, p_sls_perd_id t2,
                          p_sls_typ_id t3, p_fsc_cd t4 FROM dual) src
              ON (trgt.MRKT_ID=src.t1 AND trgt.SLS_PERD_ID=src.t2 
                  AND trgt.SLS_TYP_ID=src.t3 AND trgt.FSC_CD=src.t4)
            WHEN MATCHED THEN
              UPDATE SET trgt.SCT_UNIT_QTY = p_sct_unit_qty, trgt.LAST_UPDT_USER_ID=p_user_id
            WHEN NOT MATCHED THEN
              INSERT (MRKT_ID, SLS_PERD_ID, SLS_TYP_ID,
                        FSC_CD, SCT_UNIT_QTY, LAST_UPDT_USER_ID)
                VALUES (p_mrkt_id, p_sls_perd_id, p_sls_typ_id,
                        p_fsc_cd,p_sct_unit_qty,p_user_id);
	       EXCEPTION WHEN OTHERS THEN ROLLBACK TO before_changes; p_stus:=2;
		     END;	
      END IF;
    ELSE p_STUS:=3;  
    END IF;
  END SET_MANL_TREND_ADJSTMNT;

END PA_MANL_TREND_ADJSTMNT;