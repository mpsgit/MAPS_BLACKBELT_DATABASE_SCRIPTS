CREATE OR REPLACE 
PACKAGE PA_MANL_TREND_ADJSTMNT AS 
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
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_MANL_TREND_ADJSTMNT_TABLE PIPELINED; -- todo create the object first
  

END PA_MANL_TREND_ADJSTMNT;

CREATE OR REPLACE
PACKAGE BODY PA_MANL_TREND_ADJSTMNT AS
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
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_MANL_TREND_ADJSTMNT_TABLE PIPELINED AS
    CURSOR cc IS
      WITH MESP AS
        (SELECT DLY_BILNG_MTCH_ID
           FROM MRKT_EFF_SLS_PERD
           WHERE MRKT_ID       = p_mrkt_id
             AND EFF_SLS_PERD_ID =
               (SELECT MAX (mesp.eff_sls_perd_id)
                  FROM MRKT_EFF_SLS_PERD mesp
                  WHERE mesp.mrkt_id        = p_mrkt_id
                    AND mesp.eff_sls_perd_id <= p_sls_perd_id
                )
        )
      SELECT OBJ_MANL_TREND_ADJSTMNT_LINE(
        MAX(DB.FSC_CD),
        PA_MAPS_PUBLIC.get_fsc_desc(p_mrkt_id,p_sls_perd_id,MAX(db.FSC_CD)),
        DB.SKU_ID,
        MAX(MS.LCL_SKU_NM),
        SUM(DB.UNIT_QTY),
        MAX(SFO.SCT_UNIT_QTY),
        MAX(SFO.LAST_UPDT_USER_ID),
        MAX(SFO.LAST_UPDT_TS)) cline
      FROM   dly_bilng DB
        JOIN dly_bilng_cntrl DBC
          ON NVL ( DBC.LCL_BILNG_ACTN_CD, DB.LCL_BILNG_ACTN_CD )   = DB.LCL_BILNG_ACTN_CD
            AND NVL ( DBC.LCL_BILNG_TRAN_TYP, DB.LCL_BILNG_TRAN_TYP ) = DB.LCL_BILNG_TRAN_TYP
            AND NVL ( DBC.LCL_BILNG_OFFR_TYP, DB.LCL_BILNG_OFFR_TYP ) = DB.LCL_BILNG_OFFR_TYP
            AND NVL ( DBC.LCL_BILNG_DEFRD_CD, DB.LCL_BILNG_DEFRD_CD ) = DB.LCL_BILNG_DEFRD_CD
            AND NVL ( DBC.LCL_BILNG_SHPNG_CD, DB.LCL_BILNG_SHPNG_CD ) = DB.LCL_BILNG_SHPNG_CD
        JOIN MESP USING(DLY_BILNG_MTCH_ID)
        LEFT JOIN SCT_FSC_OVRRD SFO
          ON SFO.MRKT_ID=DB.MRKT_ID AND SFO.SLS_PERD_ID=DB.SLS_PERD_ID
            AND SFO.SLS_TYP_ID=DBC.SLS_TYP_ID AND SFO.FSC_CD=DB.FSC_CD
        LEFT JOIN MRKT_SKU MS
          ON MS.MRKT_ID=DB.MRKT_ID AND MS.SKU_ID=DB.SKU_ID
      WHERE DB.mrkt_id = p_mrkt_id
        AND DB.Sls_Perd_Id = p_sls_perd_id
        AND DBC.SLS_TYP_ID = (select MRKT_CONFIG_ITEM_VAL_TXT 
                                from mrkt_config_item 
                                where mrkt_id=p_mrkt_id
                                  and CONFIG_ITEM_ID=10000)
      GROUP BY DB.SKU_ID;
  BEGIN
    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_MANL_TREND_ADJSTMNT;
  
    PROCEDURE SET_MANL_TREND_ADJSTMNT(p_mrkt_id IN NUMBER,
                                  p_sls_perd_id IN NUMBER,
                                  p_sls_typ_id IN NUMBER,
                                  p_fsc_cd IN NUMBER,
                                  p_sct_unit_qty IN NUMBER,
                                  p_user_id IN VARCHAR2,
                                  p_stus OUT NUMBER) AS
  BEGIN
    -- TODO: Implementation required for PROCEDURE PA_MANL_TREND_ADJSTMNT.SET_MANL_TREND_ADJSTMNT
    NULL;
  END SET_MANL_TREND_ADJSTMNT;

END PA_MANL_TREND_ADJSTMNT;