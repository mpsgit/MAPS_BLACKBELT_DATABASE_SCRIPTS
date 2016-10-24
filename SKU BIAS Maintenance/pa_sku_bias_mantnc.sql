create or replace PACKAGE SKU_BIAS_MANTNC AS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 12/10/2016
  * Description  : First created 
  ******************************************************/

  PROCEDURE SET_SKU_BIAS(p_mrkt_id IN NUMBER,
                         p_sls_perd_id IN NUMBER,
                         p_new_sku_bias IN NUMBER,
                         p_stus OUT NUMBER);
                                  
  FUNCTION GET_SKU_BIAS(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED;                                

END SKU_BIAS_MANTNC;

CREATE OR REPLACE
PACKAGE BODY SKU_BIAS_MANTNC AS

  FUNCTION GET_SKU_BIAS(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED AS
  CURSOR cc IS
    SELECT OBJ_SKU_BIAS_MANTNC_LINE(
      FSC.FSC_CD,
      FSC.PROD_DESC_TXT,
      S.SKU_ID,
      S.LCL_SKU_NM,
      DECODE((SELECT 1 AA from dual 
              WHERE EXISTS( SELECT SKU_PRC_AMT FROM MRKT_PERD_SKU_PRC MPSP
                            WHERE MPSP.MRKT_ID=p_mrkt_id AND MPSP.OFFR_PERD_ID=p_sls_perd_id
                              AND MPSP.SKU_ID=S.SKU_ID AND MPSP.PRC_LVL_TYP_CD='RP' )
                AND EXISTS( SELECT * FROM SKU_COST SC 
                            WHERE SC.MRKT_ID=p_mrkt_id AND SC.OFFR_PERD_ID=p_sls_perd_id
                              AND SC.SKU_ID=S.SKU_ID AND COST_TYP='P')
                AND (PA_MAPS_PUBLIC.get_sls_cls_cd(p_sls_perd_id, p_mrkt_id, s.avlbl_perd_id,
                         s.intrdctn_perd_id, s.demo_ofs_nr, s.demo_durtn_nr, s.new_durtn_nr,
                         s.stus_perd_id, s.dspostn_perd_id, s.on_stus_perd_id)!='-1')
              ),1,'A','N'),
	    DECODE((SELECT count(*) FROM OFFR_SKU_LINE OSL 
              WHERE OSL.OFFR_ID in( SELECT OFFR_ID FROM OFFR O
                                    WHERE O.MRKT_ID=p_mrkt_id
                                     AND O.VER_ID = 0
                                     AND O.OFFR_TYP='CMP'
                                     AND O.OFFR_PERD_ID =p_sls_perd_id)
               AND OSL.DLTD_IND NOT IN ('Y','y')
               AND OSL.SKU_ID=S.SKU_ID),0,'N','P'),
      PSB.BIAS_PCT,
      PSB.CREAT_TS,
      PSB.CREAT_USER_ID,
      PSB.LAST_UPDT_TS,
      PSB.LAST_UPDT_USER_ID) cline
    FROM MRKT_SKU S
      LEFT JOIN ( SELECT * FROM MRKT_PERD_SKU_BIAS
                    WHERE MRKT_ID=p_mrkt_id AND SLS_PERD_ID=p_sls_perd_id         
        ) PSB
        ON PSB.MRKT_ID=S.MRKT_ID AND PSB.SKU_ID=S.SKU_ID
      LEFT JOIN MRKT_FSC FSC
        ON FSC.MRKT_ID=p_mrkt_id AND FSC.SKU_ID=S.SKU_ID
    WHERE S.MRKT_ID=p_mrkt_id AND S.DLTD_IND NOT IN ('Y','y')    
    ORDER BY S.MRKT_ID,S.SKU_ID;
  BEGIN

    FOR rec in cc LOOP
      pipe row(rec.cline);
    END LOOP;
  END GET_SKU_BIAS;
  
  PROCEDURE SET_SKU_BIAS(p_mrkt_id IN NUMBER,
                         p_sls_perd_id IN NUMBER,
                         p_new_sku_bias IN NUMBER,
                         p_stus OUT NUMBER) AS
  BEGIN
    -- TODO: Implementation required for PROCEDURE SKU_BIAS_MANTNC.SET_SKU_BIAS
    NULL;
  END SET_SKU_BIAS;
                                  
END SKU_BIAS_MANTNC;