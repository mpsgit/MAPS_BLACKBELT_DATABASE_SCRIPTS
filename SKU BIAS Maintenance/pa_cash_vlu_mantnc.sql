create or replace PACKAGE SKU_BIAS_MANTNC AS 
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 20/10/2016
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
  /*********************************************************
  * History
  * Created by   : Schiff Gy
  * Date         : 20/10/2016
  * Description  : First created 
  ******************************************************/

  FUNCTION GET_SKU_BIAS(p_mrkt_id IN NUMBER,
                        p_sls_perd_id IN NUMBER
                        ) RETURN OBJ_SKU_BIAS_MANTNC_TABLE PIPELINED AS
  CURSOR cc IS
    SELECT OBJ_SKU_BIAS_MANTNC_LINE(
      FSC.FSC_CD,
      FSC.PROD_DESC_TXT,
      S.SKU_ID,
      S.LCL_SKU_NM,
	    'A',
	    'P',
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
    ORDER BY PSB.SKU_ID;
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