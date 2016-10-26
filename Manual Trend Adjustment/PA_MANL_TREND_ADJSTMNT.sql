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
                        ) RETURN OBJ_CASH_VAL_MANTNC_TABLE PIPELINED; -- todo create the object first
  

END PA_MANL_TREND_ADJSTMNT;

CREATE OR REPLACE
PACKAGE BODY PA_MANL_TREND_ADJSTMNT AS
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 26/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE INITIATE_TREND_OFFSET_TABLE (p_eff_sls_perd_id IN NUMBER) AS
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